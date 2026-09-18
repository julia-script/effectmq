/** Atomic Redis storage for durable application events. @module */
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as EventRecord from "./EventRecord.js";
import eventEngineScript from "./lua/eventEngine.js";
import * as NodeRedisPool from "./NodeRedisPool.js";
import {
  RedisConnectionRoles,
  type RedisConnectionRolesService,
  RedisPool,
  type RedisPoolService,
} from "./RedisPool.js";

/** Queue policy is persisted on first use. All clients must agree on it. */
export interface Queue {
  readonly name: string;
  readonly onCompletion?: "delete" | "archive";
  /** Default event lifetime in milliseconds; null or omitted means indefinite. */
  readonly ttlMs?: number | null;
  /** Lifetime after settlement; null or omitted archives indefinitely. */
  readonly archiveRetentionMs?: number | null;
}

/** Engine namespace and maximum records per maintenance category per call. */
export interface Config {
  readonly prefix?: string;
  readonly maintenanceBatchSize?: number;
}

const ErrorCodeSchema = Schema.Literals([
  "InvalidInput",
  "ConfigurationConflict",
  "CapacityExceeded",
  "SubscriptionMissing",
  "LeaseLost",
  "EventNotActive",
  "CorruptStorage",
  "IndeterminateWrite",
  "IdentityError",
]);

/** Storage/ownership failure. IndeterminateWrite means the operation may have committed. */
export class EventEngineError extends Data.TaggedError("EventEngineError")<{
  readonly code: typeof ErrorCodeSchema.Type;
  readonly message: string;
  readonly cause?: unknown;
  readonly operation?: string;
  readonly eventId?: string;
}> {}

/** Result of acknowledging a retained or already deleted event. */
export type Acknowledgement = "acknowledged" | "already-acknowledged" | "gone";
/** A bounded maintenance pass, with a signal for immediately due remaining work. */
export interface MaintenanceResult {
  readonly processed: number;
  readonly pending: boolean;
}
/** Encoded delivery for adapter authors. Prefer EventQueue for typed payloads. */
export interface EncodedDelivery {
  readonly event: EventRecord.EncodedEvent;
  readonly leaseToken: string;
}
/** Fenced identity used for mutation. */
export interface Attempt extends EventRecord.Subscription {
  readonly id: string;
  readonly token: string;
}

/** Atomic operations. Crypto is required only when generating new identities. */
export interface Service {
  readonly subscribe: (
    queue: Queue,
    name: string,
  ) => Effect.Effect<EventRecord.Subscription, EventEngineError, Crypto.Crypto>;
  readonly unsubscribe: (
    queue: Queue,
    subscription: EventRecord.Subscription,
  ) => Effect.Effect<boolean, EventEngineError>;
  readonly emit: (
    queue: Queue,
    payload: string,
    ttlMs?: number | null,
  ) => Effect.Effect<EventRecord.EncodedEvent, EventEngineError, Crypto.Crypto>;
  readonly get: (
    queue: Queue,
    id: string,
  ) => Effect.Effect<EventRecord.EncodedEvent | null, EventEngineError>;
  readonly take: (
    queue: Queue,
    subscription: EventRecord.Subscription,
    leaseMs?: number,
  ) => Effect.Effect<EncodedDelivery | null, EventEngineError, Crypto.Crypto>;
  readonly acknowledge: (
    queue: Queue,
    attempt: Attempt,
  ) => Effect.Effect<Acknowledgement, EventEngineError>;
  readonly renew: (
    queue: Queue,
    attempt: Attempt,
    leaseMs: number,
  ) => Effect.Effect<void, EventEngineError>;
  readonly release: (
    queue: Queue,
    attempt: Attempt,
    delayMs?: number,
  ) => Effect.Effect<void, EventEngineError>;
  readonly maintain: (
    queue: Queue,
  ) => Effect.Effect<MaintenanceResult, EventEngineError>;
  readonly listArchived: (
    queue: Queue,
    options?: { readonly offset?: number; readonly limit?: number },
  ) => Effect.Effect<ReadonlyArray<string>, EventEngineError>;
}

/** Provide with layerNoDeps for a custom Redis adapter, or layer for Node. */
export class EventEngine extends Context.Service<EventEngine, Service>()(
  "@effectmq/core/EventEngine",
) {}

const newIdentity = Effect.fnUntraced(function* (): Effect.fn.Return<
  string,
  EventEngineError,
  Crypto.Crypto
> {
  return yield* (yield* Crypto.Crypto).randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new EventEngineError({
          code: "IdentityError",
          message: "Could not generate event identity",
          cause,
        }),
    ),
  );
});

const invalid = (message: string): EventEngineError =>
  new EventEngineError({ code: "InvalidInput", message });
const corrupt = (cause: unknown, operation?: string): EventEngineError =>
  new EventEngineError({
    code: "CorruptStorage",
    message: "Invalid event storage reply",
    cause,
    operation,
  });

/** Validate a nonempty queue/subscription name, bounded to 256 UTF-8 bytes. */
const validateName = (name: string): Effect.Effect<void, EventEngineError> =>
  typeof name === "string" &&
  name.length > 0 &&
  name.isWellFormed() &&
  Buffer.byteLength(name, "utf8") <= 256
    ? Effect.void
    : Effect.fail(invalid("Names must contain 1 to 256 UTF-8 bytes"));

// Bound sums below cjson's exact integer range, including the Redis timestamp.
const maxDurationMs = 3_153_600_000_000;
const validateDuration = (
  value: number | null,
  name: string,
  minimum = 0,
): Effect.Effect<void, EventEngineError> =>
  value === null ||
  (Number.isSafeInteger(value) && value >= minimum && value <= maxDurationMs)
    ? Effect.void
    : Effect.fail(
        invalid(
          `${name} must be an integer between ${minimum} and ${maxDurationMs}, or null`,
        ),
      );

const queuePolicy = Effect.fnUntraced(function* (
  queue: Queue,
): Effect.fn.Return<string, EventEngineError> {
  yield* validateName(queue.name);
  const onCompletion = queue.onCompletion ?? "delete";
  if (onCompletion !== "delete" && onCompletion !== "archive")
    return yield* invalid("Unknown completion policy");
  const ttlMs = queue.ttlMs ?? null;
  const archiveRetentionMs = queue.archiveRetentionMs ?? null;
  yield* validateDuration(ttlMs, "ttlMs");
  yield* validateDuration(archiveRetentionMs, "archiveRetentionMs");
  return JSON.stringify({ onCompletion, ttlMs, archiveRetentionMs });
});
const checkSubscription = Effect.fnUntraced(function* (
  queue: Queue,
  subscription: EventRecord.Subscription,
): Effect.fn.Return<void, EventEngineError> {
  if (subscription.queue !== queue.name)
    return yield* invalid("Subscription belongs to another queue");
  yield* validateName(subscription.name);
  yield* validateName(subscription.generation);
});

const ReplySchema = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), value: Schema.Unknown }),
  Schema.Struct({
    ok: Schema.Literal(false),
    code: ErrorCodeSchema,
    message: Schema.String,
  }),
]);
const SubscriptionReply = Schema.Struct({
  name: Schema.String,
  generation: Schema.String,
});
const DeliveryReply = Schema.NullOr(
  Schema.Struct({
    event: EventRecord.EncodedEventSchema,
    leaseToken: Schema.String,
  }),
);
const AckReply = Schema.Literals([
  "acknowledged",
  "already-acknowledged",
  "gone",
]);
const MaintenanceReply = Schema.Struct({
  processed: Schema.Number,
  pending: Schema.Boolean,
});

/** Construct an engine with an existing Redis command service. */
export const makeWithRedis = Effect.fnUntraced(function* (
  redis: RedisPoolService,
  config: Config = {},
  roles?: RedisConnectionRolesService,
): Effect.fn.Return<Service, EventEngineError> {
  const prefix = config.prefix ?? "~effectmq:events:v1";
  const batch = config.maintenanceBatchSize ?? 100;
  if (typeof prefix !== "string" || prefix.length === 0 || /[{}]/.test(prefix))
    return yield* invalid("prefix must be nonempty and contain no braces");
  if (!Number.isSafeInteger(batch) || batch < 1 || batch > 1000)
    return yield* invalid("maintenanceBatchSize must be between 1 and 1000");

  const call = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(
    queue: Queue,
    operation: string,
    input: object,
    schema: S,
  ): Effect.fn.Return<S["Type"], EventEngineError> {
    const policy = yield* queuePolicy(queue);
    const root = `${prefix}:{${Buffer.from(queue.name, "utf8").toString("base64url")}}`;
    const connection =
      roles === undefined
        ? redis
        : operation === "maintain"
          ? roles.maintenance
          : ["take", "acknowledge", "renew", "release"].includes(operation)
            ? roles.worker
            : roles.producer;
    const raw = yield* connection
      .evalScript<unknown>(
        eventEngineScript,
        { numberOfKeys: 1 },
        root,
        operation,
        policy,
        JSON.stringify(input),
        String(batch),
      )
      .pipe(
        Effect.mapError(
          (cause) =>
            new EventEngineError({
              code: "IndeterminateWrite",
              operation,
              eventId:
                "id" in input && typeof input.id === "string"
                  ? input.id
                  : undefined,
              message: `Event ${operation} may have committed; inspect durable state before retrying`,
              cause,
            }),
        ),
      );
    const invalidReply = (cause: unknown) => corrupt(cause, operation);
    const parsed = yield* Effect.try({
      try: () => {
        if (typeof raw !== "string")
          throw new TypeError("Expected a JSON reply");
        return JSON.parse(raw) as unknown;
      },
      catch: invalidReply,
    });
    const reply = yield* Schema.decodeUnknownEffect(ReplySchema)(parsed).pipe(
      Effect.mapError(invalidReply),
    );
    if (!reply.ok)
      return yield* new EventEngineError({
        code: reply.code,
        message: reply.message,
        operation,
      });
    return yield* Schema.decodeUnknownEffect(schema)(reply.value).pipe(
      Effect.mapError(invalidReply),
    );
  });
  const mutateAttempt = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(
    queue: Queue,
    operation: string,
    attempt: Attempt,
    schema: S,
    extra: object = {},
  ): Effect.fn.Return<S["Type"], EventEngineError> {
    yield* checkSubscription(queue, attempt);
    yield* validateName(attempt.id);
    yield* validateName(attempt.token);
    return yield* call(queue, operation, { ...attempt, ...extra }, schema);
  });

  return EventEngine.of({
    subscribe: Effect.fnUntraced(function* (queue, name) {
      yield* validateName(name);
      const generation = yield* newIdentity();
      const identity = yield* call(
        queue,
        "subscribe",
        { name, generation },
        SubscriptionReply,
      );
      return { queue: queue.name, ...identity };
    }),
    unsubscribe: Effect.fnUntraced(function* (queue, subscription) {
      yield* checkSubscription(queue, subscription);
      return yield* call(queue, "unsubscribe", subscription, Schema.Boolean);
    }),
    emit: Effect.fnUntraced(function* (queue, payload, ttlMs) {
      const ttl = ttlMs === undefined ? (queue.ttlMs ?? null) : ttlMs;
      yield* validateDuration(ttl, "ttlMs");
      if (typeof payload !== "string" || Buffer.byteLength(payload) > 1_500_000)
        return yield* invalid(
          "Encoded payload must be a string no larger than 1.5 MB",
        );
      const id = yield* newIdentity();
      return yield* call(
        queue,
        "emit",
        { id, queue: queue.name, payload, ttlMs: ttl },
        EventRecord.EncodedEventSchema,
      );
    }),
    get: Effect.fnUntraced(function* (queue, id) {
      yield* validateName(id);
      return yield* call(
        queue,
        "get",
        { id },
        Schema.NullOr(EventRecord.EncodedEventSchema),
      );
    }),
    take: Effect.fnUntraced(function* (queue, subscription, leaseMs = 30_000) {
      yield* checkSubscription(queue, subscription);
      if (typeof leaseMs !== "number")
        return yield* invalid("leaseMs must be numeric");
      yield* validateDuration(leaseMs, "leaseMs", 1);
      const token = yield* newIdentity();
      return yield* call(
        queue,
        "take",
        { ...subscription, leaseMs, token },
        DeliveryReply,
      );
    }),
    acknowledge: (queue, attempt) =>
      mutateAttempt(queue, "acknowledge", attempt, AckReply),
    renew: Effect.fnUntraced(function* (queue, attempt, leaseMs) {
      if (typeof leaseMs !== "number")
        return yield* invalid("leaseMs must be numeric");
      yield* validateDuration(leaseMs, "leaseMs", 1);
      yield* mutateAttempt(queue, "renew", attempt, Schema.Boolean, {
        leaseMs,
      });
    }),
    release: Effect.fnUntraced(function* (queue, attempt, delayMs = 0) {
      if (typeof delayMs !== "number")
        return yield* invalid("delayMs must be numeric");
      yield* validateDuration(delayMs, "delayMs");
      yield* mutateAttempt(queue, "release", attempt, Schema.Boolean, {
        delayMs,
      });
    }),
    maintain: (queue) => call(queue, "maintain", {}, MaintenanceReply),
    listArchived: Effect.fnUntraced(function* (
      queue,
      { offset = 0, limit = 100 } = {},
    ) {
      if (
        !Number.isSafeInteger(offset) ||
        offset < 0 ||
        !Number.isSafeInteger(limit) ||
        limit < 1 ||
        limit > 1000
      )
        return yield* invalid(
          "Archive offset must be nonnegative and limit between 1 and 1000",
        );
      return yield* call(
        queue,
        "listArchived",
        { offset, limit },
        Schema.Array(Schema.String),
      );
    }),
  });
});

/** Construct an engine using a caller-provided RedisPool. */
export const make = Effect.fnUntraced(function* (
  config: Config = {},
): Effect.fn.Return<Service, EventEngineError, RedisPool> {
  return yield* makeWithRedis(
    yield* RedisPool,
    config,
    Option.getOrUndefined(yield* Effect.serviceOption(RedisConnectionRoles)),
  );
});
/** Dependency-free engine layer; supply RedisPool separately. */
export const layerNoDeps = (
  config: Config = {},
): Layer.Layer<EventEngine, EventEngineError, RedisPool> =>
  Layer.effect(EventEngine, make(config));
/** Standard Node composition, including Redis and Crypto services. */
export const layer = (
  config: {
    readonly engine?: Config;
    readonly redis?: NodeRedisPool.RedisConfig;
  } = {},
): Layer.Layer<
  | EventEngine
  | Crypto.Crypto
  | Layer.Success<ReturnType<typeof NodeRedisPool.layer>>,
  EventEngineError | Layer.Error<ReturnType<typeof NodeRedisPool.layer>>
> =>
  layerNoDeps(config.engine).pipe(
    Layer.provideMerge(
      Layer.merge(NodeRedisPool.layer(config.redis), NodeCrypto.layer),
    ),
  );
