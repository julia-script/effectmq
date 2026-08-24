/**
 * The low-level task engine: a Redis-backed service implementing the queue
 * primitives (create/take/complete/fail, locking, delayed and cron schedules,
 * and a per-queue event stream) as an atomic Lua script (see
 * `src/lua/taskEngine.lua`, loaded by content and invoked via `EVALSHA`).
 * Most consumers should use the higher-level `TaskQueue`/`Scheduler` APIs
 * rather than calling the engine directly.
 *
 * @module
 */

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as Redis from "effect/unstable/persistence/Redis";
import {
  type EngineTask,
  type EngineTaskInsert,
  EngineTaskSchema,
  type EngineTerminalResult,
  EngineTerminalResultSchema,
} from "./EngineRecord.js";
import taskEngineScript from "./lua/taskEngine.js";
import { UnknownFromMsgpack } from "./MessagePack.js";
import * as Observability from "./Observability.js";
import * as NodeRedisPool from "./NodeRedisPool.js";
import {
  type RedisConnectionRoles,
  RedisPool,
  type RedisPoolService,
} from "./RedisPool.js";
import { type Event, EventSchema } from "./TaskEvent.js";

const TypeId = "~effectmq/TaskEngine" as const;

/**
 * Configures Redis key namespacing, deterministic test time, and sweep bounds.
 *
 * `maintenanceBatchSize` defaults to 100 and cannot exceed
 * {@link maxMaintenanceBatchSize}. The default key prefix is `~effectmq:v1`.
 *
 * @category Configuration
 * @since 0.3.0
 */
export type TaskEngineConfig = {
  debugMode?: boolean;
  prefix?: string;
  maintenanceBatchSize?: number;
};
/**
 * Largest supported number of records processed by one atomic maintenance call.
 *
 * @category Configuration
 * @since 0.3.0
 */
export const maxMaintenanceBatchSize = 1_000;

/** Predictable validation failure for task-engine configuration. */
export class TaskEngineConfigurationError extends Data.TaggedError(
  "TaskEngineConfigurationError",
)<{
  readonly field: "maintenanceBatchSize";
  readonly constraint: string;
  readonly actual: unknown;
}> {}

const validateConfig = (
  config: TaskEngineConfig = {},
): Effect.Effect<void, TaskEngineConfigurationError> => {
  const maintenanceBatchSize = config.maintenanceBatchSize ?? 100;
  return Number.isSafeInteger(maintenanceBatchSize) &&
    maintenanceBatchSize >= 1 &&
    maintenanceBatchSize <= maxMaintenanceBatchSize
    ? Effect.void
    : Effect.fail(
        new TaskEngineConfigurationError({
          field: "maintenanceBatchSize",
          constraint: `an integer between 1 and ${maxMaintenanceBatchSize}`,
          actual: maintenanceBatchSize,
        }),
      );
};
/**
 * Wraps a Redis, script, encoding, or decoding failure at the engine boundary.
 *
 * @category Errors
 * @since 0.1.0
 */
export type TaskEngineErrorReason =
  | {
      readonly _tag: "InvalidInput";
      readonly operation: string;
      readonly field: string;
      readonly constraint: string;
    }
  | { readonly _tag: "TransportFailure"; readonly operation: string }
  | { readonly _tag: "ScriptFailure"; readonly operation: string }
  | {
      readonly _tag: "InvalidReply";
      readonly operation: string;
      readonly expected: string;
    }
  | {
      readonly _tag: "RelationshipLimit";
      readonly scope: "holder" | "retained";
      readonly maxCount: number;
    }
  | { readonly _tag: "IndeterminateCommit"; readonly operation: string }
  | { readonly _tag: "LeaseLost"; readonly operation: string };

export class TaskEngineError extends Data.TaggedError("TaskEngineError")<{
  readonly reason: TaskEngineErrorReason;
  readonly cause: unknown;
}> {
  static invalidReply(operation: string, expected: string) {
    return (cause: unknown) =>
      new TaskEngineError({
        reason: { _tag: "InvalidReply", operation, expected },
        cause,
      });
  }

  static redis(operation: string, mutating = false) {
    return (cause: unknown) =>
      new TaskEngineError({
        reason: classifyRedisFailure(operation, mutating, cause),
        cause,
      });
  }
}

/**
 * Reports whether an offer created a generation or returned an existing one.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskCreateResult {
  readonly status: "created" | "existing";
  readonly cursor: string;
  readonly task: EngineTask;
}

/**
 * One acquired execution attempt and its opaque ownership credential.
 *
 * The token belongs to this specific acquisition, not to the worker or task.
 * Every ownership-sensitive transition must present it unchanged.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskAttempt {
  readonly task: EngineTask;
  readonly leaseToken: string;
}

/**
 * Indicates that an attempt no longer owns the task generation it tried to mutate.
 *
 * @category Errors
 * @since 0.3.0
 */
export class LeaseLost extends Data.TaggedError("LeaseLost")<{
  readonly prefix: string;
  readonly taskId: string;
  readonly cause: TaskEngineError;
}> {}

/**
 * Redis Stream cursor bounds for one queue's retained lifecycle events.
 *
 * @category Models
 * @since 0.3.0
 */
export interface EventCursors {
  readonly first: string;
  readonly earliest: string;
  readonly latest: string;
}

/**
 * A task index that can be inspected through `TaskEngine.listTasks`.
 *
 * @category Models
 * @since 0.3.0
 */
export type TaskList = "wait" | "scheduled" | "active" | "failed" | "success";

/**
 * One bounded page of task identifiers from an engine index.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskListPage {
  /** Task ids in FIFO order for `wait`, otherwise ascending score then id. */
  readonly items: readonly string[];
  /** Opaque cursor for the next page; absent when this page is final. */
  readonly nextCursor?: string;
}

/**
 * Indicates that event retention trimmed the stream position a reader requested.
 *
 * Resume from `earliest` when skipping the missing interval is acceptable.
 *
 * @category Errors
 * @since 0.3.0
 */
export class CursorExpired extends Data.TaggedError("CursorExpired")<{
  readonly requested: string;
  readonly earliest: string;
}> {}

/** Indicates that a stream cursor is neither `$`, `0`, nor a Redis stream id. */
export class InvalidCursor extends Data.TaggedError("InvalidCursor")<{
  readonly cursor: string;
}> {}

const diagnosticText = (cause: unknown, depth = 0): string => {
  if (depth >= 6) return String(cause);
  if (typeof cause !== "object" || cause === null) return String(cause);

  const parts = [String(cause)];
  if ("message" in cause) parts.push(String(cause.message));
  if ("cause" in cause) parts.push(diagnosticText(cause.cause, depth + 1));
  return parts.join(" ");
};

const transportPattern =
  /ECONNRESET|ECONNREFUSED|ETIMEDOUT|EPIPE|socket closed|connection (?:is )?closed|connection lost|read only/i;

const classifyRedisFailure = (
  operation: string,
  mutating: boolean,
  cause: unknown,
): TaskEngineErrorReason => {
  const diagnostic = diagnosticText(cause);
  const relationship = diagnostic.match(
    /STORAGE_RELATIONSHIP_LIMIT (holder|retained) (\d+)/,
  );
  if (relationship) {
    return {
      _tag: "RelationshipLimit",
      scope: relationship[1] as "holder" | "retained",
      maxCount: Number(relationship[2]),
    };
  }
  if (diagnostic.includes("LEASE_LOST")) {
    return { _tag: "LeaseLost", operation };
  }
  if (transportPattern.test(diagnostic)) {
    return {
      _tag: mutating ? "IndeterminateCommit" : "TransportFailure",
      operation,
    };
  }
  return { _tag: "ScriptFailure", operation };
};

const isLeaseLost = (error: TaskEngineError) =>
  error.reason._tag === "LeaseLost";

const validatePositiveSafeInteger = (
  operation: string,
  field: string,
  value: number,
) =>
  Number.isSafeInteger(value) && value > 0
    ? Effect.void
    : Effect.fail(
        new TaskEngineError({
          reason: {
            _tag: "InvalidInput",
            operation,
            field,
            constraint: "a positive safe integer",
          },
          cause: value,
        }),
      );

type ParsedStreamId = readonly [time: bigint, sequence: bigint];

const parseStreamId = (value: string): ParsedStreamId | undefined => {
  const match = /^(\d+)(?:-(\d+))?$/.exec(value);
  if (
    match === null ||
    match[1].length > 20 ||
    (match[2] !== undefined && match[2].length > 20)
  ) {
    return undefined;
  }
  const time = BigInt(match[1]);
  const sequence = BigInt(match[2] ?? "0");
  const maxComponent = (1n << 64n) - 1n;
  return time <= maxComponent && sequence <= maxComponent
    ? [time, sequence]
    : undefined;
};

const compareStreamIds = (
  [leftTime, leftSequence]: ParsedStreamId,
  [rightTime, rightSequence]: ParsedStreamId,
): number => {
  const timeDifference = leftTime - rightTime;
  if (timeDifference !== 0n) return timeDifference < 0n ? -1 : 1;
  const sequenceDifference = leftSequence - rightSequence;
  return sequenceDifference === 0n ? 0 : sequenceDifference < 0n ? -1 : 1;
};

/**
 * Low-level atomic queue, lease, retention, schedule, and event operations.
 *
 * Most applications should use `TaskQueue`, `Worker`, and `Scheduler`. Use the
 * engine directly for administration, inspection, or custom runtimes that can
 * uphold its generation and lease-token invariants.
 *
 * **Gotchas**
 *
 * A successful Redis write followed by a lost connection can be indeterminate.
 * Ownership-sensitive operations fail with {@link LeaseLost} when their exact
 * per-attempt token no longer owns the generation.
 *
 * @category Services
 * @since 0.1.0
 */
export class TaskEngine extends Context.Service<
  TaskEngine,
  {
    readonly [TypeId]: typeof TypeId;
    /** Compatibility offer that returns only the created or existing task. */
    readonly createTask: (
      task: EngineTaskInsert,
    ) => Effect.Effect<EngineTask, TaskEngineError>;
    /** Idempotently offers a task and reports whether its generation was new. */
    readonly offerTask: (
      task: EngineTaskInsert,
    ) => Effect.Effect<TaskCreateResult, TaskEngineError>;
    /** Reads a task generation, or `null` when no task record remains. */
    readonly getTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<EngineTask | null, TaskEngineError>;
    /** Reads the latest generation number for a task identity. */
    readonly getGeneration: (
      prefix: string,
      id: string,
    ) => Effect.Effect<number, TaskEngineError>;
    /** Reads a retained terminal result for an exact generation. */
    readonly getResult: (
      prefix: string,
      id: string,
      generation: number,
    ) => Effect.Effect<EngineTerminalResult | null, TaskEngineError>;
    /** Lists at most 1,000 task ids using an opaque pagination cursor. */
    readonly listTasks: (
      prefix: string,
      list: TaskList,
      options?: { readonly cursor?: string; readonly limit?: number },
    ) => Effect.Effect<TaskListPage, TaskEngineError>;
    /** Runs one bounded promotion, lease-recovery, and retention sweep. */
    readonly maintain: (
      prefix: string,
    ) => Effect.Effect<Observability.QueueHealth, TaskEngineError>;
    /** Reads the first, earliest-retained, and latest event stream cursors. */
    readonly eventCursors: (
      prefix: string,
    ) => Effect.Effect<EventCursors, TaskEngineError>;

    /** Settles an owned attempt successfully using its exact lease token. */
    readonly writeSuccess: (
      prefix: string,
      id: string,
      leaseToken: string,
      result: unknown,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Records an owned attempt failure and optionally schedules its retry. */
    readonly writeError: (
      prefix: string,
      id: string,
      leaseToken: string,
      error: unknown,
      retryAt?: Duration.Input,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Renews an owned attempt's lease for `lockTimeout` milliseconds. */
    readonly extendLock: (
      prefix: string,
      id: string,
      leaseToken: string,
      lockTimeout: number,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Voluntarily releases an owned attempt and returns it to runnable work. */
    readonly removeLock: (
      prefix: string,
      id: string,
      leaseToken: string,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Acquires the next runnable task with a fresh lease token. */
    readonly takeTask: (
      prefix: string,
      lockTimeout: number,
    ) => Effect.Effect<TaskAttempt | null, TaskEngineError, Crypto.Crypto>;
    /** Removes an unretained task generation and all queue memberships. */
    readonly removeTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;
    /** Administratively remove a generation even while results are retained. */
    readonly forceRemoveTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;

    /** Initializes a durable schedule cursor without moving an existing cursor. */
    readonly setSchedule: (
      id: string,
      next: Date,
    ) => Effect.Effect<Date, TaskEngineError>;
    /** Compare-and-advances a durable schedule cursor. */
    readonly consumeSchedule: (
      name: string,
      toConsume: Date,
      next: Date,
    ) => Effect.Effect<{ consumed: boolean; next?: Date }, TaskEngineError>;
    /**
     * Stream lifecycle events for a queue, read from its Redis Stream
     * (`<prefix>:<name>:events`) via `XREAD`. Starts from `cursor` (defaulting
     * to now) and advances the cursor past each yielded event. Events are raw
     * {@link Event}s; `TaskQueue.stream` decodes their payloads against the
     * queue's schemas.
     */
    readonly stream: (
      name: string,
      options?: {
        cursor?: string;
        pollInterval?: Duration.Duration;
      },
    ) => Stream.Stream<
      Event,
      TaskEngineError | CursorExpired | InvalidCursor | Schema.SchemaError
    >;
  }
>()("@effectmq/core/TaskEngine") {}

/**
 * The service interface represented by the {@link TaskEngine} tag.
 *
 * @category Services
 * @since 0.1.0
 */
export type TaskEngineService = TaskEngine["Service"];
// must match MOCKTIME_KEY in src/lua/taskEngine.lua
const MOCKTIME_KEY = "$$$effectmq/debug/mocktime";

/**
 * Override the engine's notion of "now" (only honored when the engine is built
 * with `debugMode`). Intended for deterministic tests of delays and schedules.
 *
 * **Gotchas**
 *
 * The mock clock is a Redis-global debug key, not a queue- or prefix-local
 * clock. Never enable `debugMode` in production.
 *
 * @category Testing
 * @since 0.1.0
 */
export const setMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* RedisPool;
    yield* redis.send("SET", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.redis("setMockTime", true)));

/**
 * Advances the Redis-global mock clock by a duration.
 *
 * Only engines built with `debugMode` read this clock. See {@link setMockTime}.
 *
 * @category Testing
 * @since 0.1.0
 */
export const stepMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* RedisPool;
    yield* redis.send("INCRBY", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.redis("stepMockTime", true)));

const invalidReply = (operation: string, expected: string, received: unknown) =>
  new TaskEngineError({
    reason: { _tag: "InvalidReply", operation, expected },
    cause: { received },
  });

const decodeText = (
  operation: string,
  value: unknown,
): Effect.Effect<string, TaskEngineError> => {
  if (typeof value === "string") return Effect.succeed(value);
  if (value instanceof Uint8Array) {
    return Effect.try({
      try: () => Buffer.from(value).toString("utf8"),
      catch: (cause) =>
        new TaskEngineError({
          reason: {
            _tag: "InvalidReply",
            operation,
            expected: "UTF-8 text bytes",
          },
          cause,
        }),
    });
  }
  return Effect.fail(invalidReply(operation, "text or Uint8Array", value));
};

const decodeNumber = (
  operation: string,
  value: unknown,
): Effect.Effect<number, TaskEngineError> =>
  Effect.gen(function* () {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    const text = yield* decodeText(operation, value);
    const decoded = Number(text);
    return Number.isFinite(decoded)
      ? decoded
      : yield* invalidReply(operation, "a finite number", value);
  });

const decodeArray = (
  operation: string,
  value: unknown,
): Effect.Effect<ReadonlyArray<unknown>, TaskEngineError> =>
  Array.isArray(value)
    ? Effect.succeed(value)
    : Effect.fail(invalidReply(operation, "an array", value));

const decodeTuple = (
  operation: string,
  value: unknown,
  length: number,
): Effect.Effect<ReadonlyArray<unknown>, TaskEngineError> =>
  Effect.flatMap(decodeArray(operation, value), (items) =>
    items.length === length
      ? Effect.succeed(items)
      : Effect.fail(
          invalidReply(operation, `an array of length ${length}`, value),
        ),
  );

/** Fold a flat `[k1, v1, k2, v2, ...]` reply into a prototype-safe record. */
const entriesToRecord = Effect.fnUntraced(function* (
  operation: string,
  value: unknown,
) {
  const entries = yield* decodeArray(operation, value);
  if (entries.length % 2 !== 0) {
    return yield* invalidReply(
      operation,
      "an even-length field/value array",
      value,
    );
  }
  const record: Record<string, unknown> = Object.create(null);
  for (let index = 0; index < entries.length; index += 2) {
    record[yield* decodeText(operation, entries[index])] = entries[index + 1];
  }
  return record;
});

/** Decode a flat `["id", id, "name", name, ...]` raw-entry reply from Lua. */
const parseTask = Effect.fnUntraced(function* (task: unknown) {
  const record = yield* entriesToRecord("decodeTask", task);
  return yield* Schema.decodeUnknownEffect(EngineTaskSchema)(record).pipe(
    Effect.mapError(TaskEngineError.invalidReply("decodeTask", "task record")),
  );
});

const parseTerminalResult = Effect.fnUntraced(function* (result: unknown) {
  const record = yield* entriesToRecord("decodeTerminalResult", result);
  return yield* Schema.decodeUnknownEffect(EngineTerminalResultSchema)(
    record,
  ).pipe(
    Effect.mapError(
      TaskEngineError.invalidReply("decodeTerminalResult", "terminal result"),
    ),
  );
});

const packUnknown = Schema.encodeEffect(UnknownFromMsgpack);
/** Encode a structured value as msgpack bytes for a script argument. */
const pack = (value: unknown) =>
  packUnknown(value).pipe(
    Effect.mapError(
      (cause) =>
        new TaskEngineError({
          reason: { _tag: "ScriptFailure", operation: "encodeScriptArgument" },
          cause,
        }),
    ),
  );

const decodeEvents = Schema.decodeUnknownEffect(Schema.Array(EventSchema));

/**
 * Joins Redis key namespace segments with a colon.
 *
 * Segments are not escaped; callers should avoid embedded colons when they
 * need unambiguous composition.
 *
 * @category Utilities
 * @since 0.1.0
 */
export const makePrefix = (...prefixes: string[]) => prefixes.join(":");

/**
 * Builds a {@link TaskEngine} implementation against a concrete Redis service.
 *
 * The script is loaded lazily by exact content and
 * invoked through cached `EVALSHA`, with one transparent `NOSCRIPT` recovery.
 * `debugMode` enables the mockable clock (see {@link setMockTime});
 * `prefix` namespaces all keys. Every acquisition creates a fresh opaque lease
 * token; callers must present it for every ownership-sensitive transition.
 *
 * @category Constructors
 * @since 0.3.0
 */
export const makeWithRedis = (
  redis: RedisPoolService,
  {
    debugMode = false,
    prefix = "~effectmq:v1",
    maintenanceBatchSize = 100,
  }: TaskEngineConfig = {},
) =>
  Effect.gen(function* () {
    yield* validateConfig({ maintenanceBatchSize });
    const debugFlag = debugMode ? "1" : "0";
    const withPrefix = (key: string) => `${prefix}:${key}`;

    // Every operation receives its name followed by the debug flag and its
    // own arguments. The Lua dispatcher preserves the operation-local layout.
    const call =
      <A = unknown>(name: string, mutating = false) =>
      (...args: ReadonlyArray<string | Uint8Array>) =>
        redis
          .evalScript<A>(
            taskEngineScript,
            {},
            name,
            debugFlag,
            String(maintenanceBatchSize),
            ...args,
          )
          .pipe(Effect.mapError(TaskEngineError.redis(name, mutating)));

    // binary replies: these functions return msgpack-encoded tasks
    const callBinary =
      <A = unknown>(name: string, mutating = false) =>
      (...args: ReadonlyArray<string | Uint8Array>) =>
        redis
          .evalScript<A>(
            taskEngineScript,
            { binaryReply: true },
            name,
            debugFlag,
            String(maintenanceBatchSize),
            ...args,
          )
          .pipe(Effect.mapError(TaskEngineError.redis(name, mutating)));

    const createTaskFn = callBinary("effectmq_createTask", true);
    const getTaskFn = callBinary("effectmq_getTask");
    const getGenerationFn = call("effectmq_getGeneration");
    const getResultFn = callBinary("effectmq_getResult");
    const takeTaskFn = callBinary("effectmq_takeTask", true);
    const writeSuccessFn = call("effectmq_writeSuccess", true);
    const writeErrorFn = call("effectmq_writeError", true);
    const removeTaskFn = call("effectmq_removeTask", true);
    const forceRemoveTaskFn = call("effectmq_forceRemoveTask", true);
    const extendLockFn = call("effectmq_extendLock", true);
    const removeLockFn = call("effectmq_removeLock", true);
    const setScheduleFn = call("effectmq_setSchedule", true);
    const consumeScheduleFn = call("effectmq_consumeSchedule", true);
    const listTasksFn = call("effectmq_listTasks");
    const maintainFn = call("effectmq_maintain", true);
    const eventCursorsFn = call("effectmq_eventCursors");

    const withLeaseFence = <A>(
      operation: Effect.Effect<A, TaskEngineError>,
      queue: string,
      taskId: string,
    ) =>
      operation.pipe(
        Effect.tapError((error) =>
          isLeaseLost(error)
            ? Effect.all([
                Effect.logWarning("effectmq task ownership lost", {
                  queue,
                  taskId,
                }),
                Metric.update(
                  Metric.withAttributes(Observability.ownershipLosses, {
                    queue,
                  }),
                  1,
                ),
              ]).pipe(Effect.asVoid)
            : Effect.void,
        ),
        Effect.mapError((cause) =>
          isLeaseLost(cause)
            ? new LeaseLost({ cause, prefix: queue, taskId })
            : cause,
        ),
      );

    const offerTask = Effect.fnUntraced(function* (task: EngineTaskInsert) {
      const numericFields: ReadonlyArray<
        readonly [keyof EngineTaskInsert, number, boolean]
      > = [
        ["delay", task.delay, false],
        ["maxRetries", task.maxRetries, true],
        ["maxStalledCount", task.maxStalledCount ?? 1, true],
        ["maxErrorEntries", task.maxErrorEntries ?? 100, true],
        ["maxRelationships", task.maxRelationships ?? 1_000, true],
        ["maxEventEntries", task.maxEventEntries ?? 10_000, true],
        [
          "taskRecordRetentionMs",
          task.taskRecordRetentionMs ?? 604_800_000,
          true,
        ],
        ["resultRetentionMs", task.resultRetentionMs ?? 86_400_000, true],
        [
          "terminalIndexRetentionMs",
          task.terminalIndexRetentionMs ?? 604_800_000,
          true,
        ],
        [
          "deadLetterRetentionMs",
          task.deadLetterRetentionMs ?? 2_592_000_000,
          true,
        ],
        ["eventRetentionMs", task.eventRetentionMs ?? 604_800_000, true],
      ];
      for (const [field, value, integer] of numericFields) {
        const minimum =
          field === "maxRetries" ? -1 : field === "maxEventEntries" ? 1 : 0;
        if (
          !Number.isFinite(value) ||
          Math.abs(value) > Number.MAX_SAFE_INTEGER ||
          value < minimum ||
          (integer && !Number.isSafeInteger(value))
        ) {
          return yield* new TaskEngineError({
            reason: {
              _tag: "InvalidInput",
              operation: "effectmq_createTask",
              field,
              constraint:
                field === "delay"
                  ? "a finite safe number greater than or equal to 0"
                  : `a safe integer greater than or equal to ${minimum}`,
            },
            cause: value,
          });
        }
      }
      const retentionHolder = task.retentionHolder
        ? yield* pack({
            ...task.retentionHolder,
            queue: withPrefix(task.retentionHolder.queue),
          })
        : "";
      const creator = task.creator
        ? yield* pack({
            ...task.creator,
            queue: withPrefix(task.creator.queue),
          })
        : "";
      const reply = yield* createTaskFn(
        withPrefix(task.prefix),
        "0", // throwOnExists
        task.id,
        task.name,
        yield* pack(task.payload),
        String(task.delay),
        String(task.maxRetries ?? -1),
        task.onSuccessPolicy,
        task.onFailurePolicy,
        retentionHolder,
        creator,
        task.onDuplicate ?? "return-existing",
        String(task.maxStalledCount ?? 1),
        task.schemaId ?? task.name,
        String(task.maxErrorEntries ?? 100),
        String(task.maxRelationships ?? 1000),
        String(task.maxEventEntries ?? 10_000),
        String(task.taskRecordRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
        String(task.resultRetentionMs ?? 24 * 60 * 60 * 1000),
        String(task.terminalIndexRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
        String(task.deadLetterRetentionMs ?? 30 * 24 * 60 * 60 * 1000),
        String(task.eventRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
      );
      const [rawStatus, rawCursor, rawTask] = yield* decodeTuple(
        "effectmq_createTask",
        reply,
        3,
      );
      const status = yield* decodeText("effectmq_createTask.status", rawStatus);
      if (status !== "created" && status !== "existing") {
        return yield* invalidReply(
          "effectmq_createTask.status",
          '"created" or "existing"',
          rawStatus,
        );
      }
      return {
        status,
        cursor: yield* decodeText("effectmq_createTask.cursor", rawCursor),
        task: yield* parseTask(rawTask),
      } satisfies TaskCreateResult;
    });

    return TaskEngine.of({
      [TypeId]: TypeId,
      offerTask,
      createTask: (task) =>
        offerTask(task).pipe(Effect.map((result) => result.task)),

      getTask: Effect.fnUntraced(function* (prefix: string, id: string) {
        const reply = yield* getTaskFn(withPrefix(prefix), id);
        return reply === null ? null : yield* parseTask(reply);
      }),
      getGeneration: (prefix, id) =>
        getGenerationFn(withPrefix(prefix), id).pipe(
          Effect.flatMap((reply) =>
            decodeNumber("effectmq_getGeneration", reply),
          ),
        ),
      getResult: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        generation: number,
      ) {
        const reply = yield* getResultFn(
          withPrefix(prefix),
          id,
          String(generation),
        );
        return reply === null ? null : yield* parseTerminalResult(reply);
      }),
      writeSuccess: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        leaseToken: string,
        result: unknown,
      ) {
        yield* withLeaseFence(
          writeSuccessFn(
            withPrefix(prefix),
            leaseToken,
            id,
            yield* pack(result),
          ),
          prefix,
          id,
        );
      }),
      writeError: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        leaseToken: string,
        error: unknown,
        retryAt?: Duration.Input,
      ) {
        const retryAtMillis = yield* Effect.try({
          try: () => (retryAt === undefined ? -1 : Duration.toMillis(retryAt)),
          catch: (cause) =>
            new TaskEngineError({
              reason: {
                _tag: "InvalidInput",
                operation: "effectmq_writeError",
                field: "retryAt",
                constraint: "a valid finite safe timestamp",
              },
              cause,
            }),
        });
        if (
          !Number.isFinite(retryAtMillis) ||
          Math.abs(retryAtMillis) > Number.MAX_SAFE_INTEGER ||
          retryAtMillis < -1
        ) {
          return yield* new TaskEngineError({
            reason: {
              _tag: "InvalidInput",
              operation: "effectmq_writeError",
              field: "retryAt",
              constraint: "a finite safe timestamp or the terminal sentinel",
            },
            cause: retryAtMillis,
          });
        }
        yield* withLeaseFence(
          writeErrorFn(
            withPrefix(prefix),
            leaseToken,
            id,
            yield* pack(error),
            String(retryAtMillis),
          ),
          prefix,
          id,
        );
      }),
      listTasks: Effect.fnUntraced(function* (
        prefix: string,
        list: TaskList,
        options: { readonly cursor?: string; readonly limit?: number } = {},
      ) {
        const cursor = options.cursor ?? "0";
        const offset = Number(cursor);
        const limit = options.limit ?? 100;
        if (
          !Number.isSafeInteger(offset) ||
          offset < 0 ||
          !Number.isSafeInteger(limit) ||
          limit < 1 ||
          limit > 1_000
        ) {
          return yield* new TaskEngineError({
            reason: {
              _tag: "InvalidReply",
              operation: "listTasks",
              expected: "a non-negative cursor and limit between 1 and 1000",
            },
            cause: new RangeError(
              "cursor must be a non-negative integer and limit must be between 1 and 1000",
            ),
          });
        }
        const rawItems = yield* listTasksFn(
          withPrefix(prefix),
          list,
          cursor,
          String(limit),
        );
        const [rawNextCursor, ...rawTaskIds] = yield* decodeArray(
          "effectmq_listTasks",
          rawItems,
        );
        const nextCursor = yield* decodeText(
          "effectmq_listTasks.cursor",
          rawNextCursor,
        );
        const items = yield* Effect.forEach(rawTaskIds, (item) =>
          decodeText("effectmq_listTasks.taskId", item),
        );
        return {
          items,
          nextCursor: nextCursor === "" ? undefined : nextCursor,
        };
      }),
      maintain: Effect.fnUntraced(function* (prefix: string) {
        const reply = yield* maintainFn(withPrefix(prefix)).pipe(
          Effect.tapError((error) =>
            Effect.all([
              Metric.update(
                Metric.withAttributes(Observability.retentionFailures, {
                  queue: prefix,
                }),
                1,
              ),
              Effect.logError("effectmq maintenance sweep failed", {
                queue: prefix,
                error,
              }),
            ]).pipe(Effect.asVoid),
          ),
        );
        const values = yield* decodeTuple("effectmq_maintain", reply, 7);
        const numbers = yield* Effect.forEach(values, (value) =>
          decodeNumber("effectmq_maintain", value),
        );
        const health: Observability.QueueHealth = {
          depth: numbers[0],
          oldestTaskAgeMs: numbers[1],
          sweepLagMs: numbers[2],
          dueBacklog: numbers[3],
          expiredLeaseBacklog: numbers[4],
          retentionBacklog: numbers[5],
          processed: numbers[6],
        };
        yield* Observability.recordQueueHealth(prefix, health);
        return health;
      }),
      eventCursors: Effect.fnUntraced(function* (prefix: string) {
        const [first, earliest, latest] = yield* eventCursorsFn(
          withPrefix(prefix),
        ).pipe(
          Effect.flatMap((reply) =>
            decodeTuple("effectmq_eventCursors", reply, 3),
          ),
        );
        return {
          first: yield* decodeText("effectmq_eventCursors.first", first),
          earliest: yield* decodeText(
            "effectmq_eventCursors.earliest",
            earliest,
          ),
          latest: yield* decodeText("effectmq_eventCursors.latest", latest),
        };
      }),
      takeTask: Effect.fnUntraced(function* (
        prefix: string,
        lockTimeout: number,
      ) {
        yield* validatePositiveSafeInteger(
          "effectmq_takeTask",
          "lockTimeout",
          lockTimeout,
        );
        const crypto = yield* Crypto.Crypto;
        const leaseToken = yield* crypto.randomUUIDv4.pipe(
          Effect.map((uuid) => `lease/${uuid}`),
          Effect.mapError(
            (cause) =>
              new TaskEngineError({
                reason: {
                  _tag: "ScriptFailure",
                  operation: "generateLeaseToken",
                },
                cause,
              }),
          ),
        );
        const reply = yield* takeTaskFn(
          withPrefix(prefix),
          leaseToken,
          String(lockTimeout),
        );

        if (reply === null) return null;
        const [rawLeaseToken, rawTask] = yield* decodeTuple(
          "effectmq_takeTask",
          reply,
          2,
        );
        return {
          leaseToken: yield* decodeText(
            "effectmq_takeTask.leaseToken",
            rawLeaseToken,
          ),
          task: yield* parseTask(rawTask),
        } satisfies TaskAttempt;
      }),
      removeTask: (prefix, id) =>
        removeTaskFn(withPrefix(prefix), id).pipe(Effect.asVoid),
      forceRemoveTask: (prefix, id) =>
        forceRemoveTaskFn(withPrefix(prefix), id).pipe(Effect.asVoid),

      extendLock: Effect.fnUntraced(
        function* (prefix, id, leaseToken, lockTimeout) {
          yield* validatePositiveSafeInteger(
            "effectmq_extendLock",
            "lockTimeout",
            lockTimeout,
          );
          return yield* withLeaseFence(
            extendLockFn(
              withPrefix(prefix),
              leaseToken,
              id,
              String(lockTimeout),
            ),
            prefix,
            id,
          ).pipe(Effect.asVoid);
        },
      ),
      removeLock: (prefix, id, leaseToken) =>
        withLeaseFence(
          removeLockFn(withPrefix(prefix), leaseToken, id),
          prefix,
          id,
        ).pipe(Effect.asVoid),
      setSchedule: (name, next) => {
        return setScheduleFn(prefix, name, String(next.getTime())).pipe(
          Effect.flatMap((reply) =>
            decodeNumber("effectmq_setSchedule", reply),
          ),
          Effect.map((next) => new Date(next)),
        );
      },
      consumeSchedule: (name, toConsume, next) => {
        return consumeScheduleFn(
          prefix,
          name,
          String(toConsume.getTime()),
          String(next.getTime()),
        ).pipe(
          Effect.flatMap((reply) =>
            Effect.gen(function* () {
              const values = yield* decodeArray(
                "effectmq_consumeSchedule",
                reply,
              );
              if (values.length < 1 || values.length > 2) {
                return yield* invalidReply(
                  "effectmq_consumeSchedule",
                  "a one- or two-item tuple",
                  reply,
                );
              }
              const consumed = yield* decodeNumber(
                "effectmq_consumeSchedule.consumed",
                values[0],
              );
              const rawNext = values[1];
              return {
                consumed: consumed === 1,
                next:
                  rawNext === null || rawNext === undefined
                    ? undefined
                    : new Date(
                        yield* decodeNumber(
                          "effectmq_consumeSchedule.next",
                          rawNext,
                        ),
                      ),
              };
            }),
          ),
        );
      },

      stream: (
        name,
        options: {
          cursor?: string;
          pollInterval?: Duration.Duration;
        } = {},
      ) => {
        const streamKey = `${withPrefix(name)}:events`;
        const blockMilliseconds = Duration.toMillis(
          options.pollInterval ?? Duration.seconds(2),
        );

        // XREAD reply shapes vary by client and RESP version: ioredis
        // replies with [stream, entries] tuples, node-redis with a Map
        // (binary type mapping) or an object keyed by stream name.
        // Normalize to the entry list of the single stream we read.
        const entriesOf = Effect.fnUntraced(function* (reply: unknown) {
          let rawEntries: unknown;
          if (reply instanceof Map) {
            rawEntries = [...reply.values()][0];
          } else {
            const streams = yield* decodeArray("xread", reply);
            const stream = yield* decodeTuple("xread.stream", streams[0], 2);
            rawEntries = stream[1];
          }
          const entries = yield* decodeArray("xread.entries", rawEntries);
          if (entries.length === 0) {
            return yield* invalidReply(
              "xread.entries",
              "at least one stream entry",
              reply,
            );
          }
          return yield* Effect.forEach(entries, (entry) =>
            decodeTuple("xread.entry", entry, 2),
          );
        });

        const readFrom = (cursor: string) =>
          Stream.paginate(cursor, (cursor) =>
            Effect.gen(function* () {
              // BLOCK makes the server hold the read until an event arrives
              // (or 2s pass), so the repeat below re-issues immediately on an
              // empty reply without a client-side polling schedule.
              const reply = yield* redis
                .sendBinary(
                  "XREAD",
                  "BLOCK",
                  String(blockMilliseconds),
                  "STREAMS",
                  streamKey,
                  cursor,
                )
                .pipe(
                  Effect.mapError(TaskEngineError.redis("xread")),
                  Effect.repeat({
                    until: (value) => !!value,
                  }),
                );

              // reassemble each entry's flat fields into an event record:
              // "new:"/"existing:" prefixed fields are task snapshots, the
              // rest is the tag-specific payload (values stay raw bytes — the
              // event schema decodes scalars and msgpack blobs per field)
              const entries = yield* entriesOf(reply);
              const records = yield* Effect.forEach(entries, (entry) =>
                Effect.gen(function* () {
                  const [entryId, rawFields] = entry;
                  const fields = yield* decodeArray("xread.fields", rawFields);
                  if (fields.length % 2 !== 0) {
                    return yield* invalidReply(
                      "xread.fields",
                      "an even-length field/value array",
                      rawFields,
                    );
                  }
                  const flat: Record<string, unknown> = Object.create(null);
                  const newTask: Record<string, unknown> = Object.create(null);
                  const existingTask: Record<string, unknown> =
                    Object.create(null);
                  for (let i = 0; i < fields.length; i += 2) {
                    const key = yield* decodeText("xread.fieldName", fields[i]);
                    const value = fields[i + 1];
                    if (key.startsWith("new:")) {
                      newTask[key.slice("new:".length)] = value;
                    } else if (key.startsWith("existing:")) {
                      existingTask[key.slice("existing:".length)] = value;
                    } else {
                      flat[key] = value;
                    }
                  }
                  const {
                    taskId,
                    generation,
                    protocolVersion,
                    schemaId,
                    _tag,
                    ...payloadFields
                  } = flat;
                  const tag = yield* decodeText("xread.tag", _tag);
                  const payload =
                    tag === "task.created"
                      ? { ...payloadFields, newTask }
                      : tag === "task.updated"
                        ? { ...payloadFields, existingTask, newTask }
                        : payloadFields;
                  return {
                    id: yield* decodeText("xread.entryId", entryId),
                    taskId: yield* decodeText("xread.taskId", taskId),
                    generation,
                    protocolVersion,
                    schemaId,
                    _tag: tag,
                    payload,
                  };
                }),
              );

              const events = yield* decodeEvents(records).pipe(
                Effect.tapError((error) => Effect.log(error.toString())),
              );

              const nextCursor = events[events.length - 1].id;

              return [events, Option.some(nextCursor)] as const;
            }),
          );

        return Stream.unwrap(
          Effect.gen(function* () {
            const [first, earliest, latest] = yield* eventCursorsFn(
              withPrefix(name),
            ).pipe(
              Effect.flatMap((reply) =>
                decodeTuple("effectmq_eventCursors", reply, 3),
              ),
            );
            const cursors = {
              first: yield* decodeText("effectmq_eventCursors.first", first),
              earliest: yield* decodeText(
                "effectmq_eventCursors.earliest",
                earliest,
              ),
              latest: yield* decodeText("effectmq_eventCursors.latest", latest),
            };
            const firstId = parseStreamId(cursors.first);
            const earliestId = parseStreamId(cursors.earliest);
            const latestId = parseStreamId(cursors.latest);
            if (
              firstId === undefined ||
              earliestId === undefined ||
              latestId === undefined
            ) {
              return yield* invalidReply(
                "effectmq_eventCursors",
                "Redis stream ids",
                cursors,
              );
            }
            const cursor = options.cursor ?? cursors.latest;
            const requestedId =
              options.cursor === undefined
                ? latestId
                : cursor === "$"
                  ? undefined
                  : parseStreamId(cursor);
            if (cursor !== "$" && requestedId === undefined) {
              return yield* new InvalidCursor({ cursor });
            }
            const streamWasTrimmed =
              cursors.first !== "0-0" &&
              compareStreamIds(earliestId, firstId) > 0;
            const requestedFromStart =
              requestedId !== undefined &&
              requestedId[0] === 0n &&
              requestedId[1] === 0n;
            const requestedTrimmedEvent =
              requestedFromStart ||
              (requestedId !== undefined &&
                compareStreamIds(requestedId, firstId) >= 0 &&
                compareStreamIds(requestedId, earliestId) < 0);
            if (streamWasTrimmed && requestedTrimmedEvent) {
              return yield* new CursorExpired({
                requested: cursor,
                earliest: cursors.earliest,
              });
            }
            return readFrom(cursor);
          }),
        );
      },
    });
  });

/**
 * Builds a task engine from the ambient producer {@link RedisPool} service.
 *
 * @category Constructors
 * @since 0.1.0
 */
export const make = (config?: TaskEngineConfig) =>
  Effect.gen(function* () {
    yield* validateConfig(config);
    const redis = yield* RedisPool;
    return yield* makeWithRedis(redis, config);
  });

/**
 * Provides {@link TaskEngine} from an ambient {@link RedisPool}.
 * Use this for custom Redis implementations, tests, and Bun plus node-redis.
 *
 * @category Layers
 * @since 0.1.0
 */
export const layerNoDeps = (config?: TaskEngineConfig) =>
  Layer.effect(TaskEngine, make(config));

/**
 * Configuration for the standard live service graph.
 *
 * @category Configuration
 * @since 0.1.0
 */
export interface LiveConfig {
  readonly engine?: TaskEngineConfig;
  readonly redis?: NodeRedisPool.RedisConfig;
}

/**
 * Provides the live graph: Redis connections, connection roles and health,
 * Crypto, and the task engine.
 *
 * @category Layers
 * @since 0.1.0
 */
export const layer = (
  config: LiveConfig = {},
): Layer.Layer<
  | TaskEngine
  | RedisPool
  | RedisConnectionRoles
  | NodeRedisPool.RedisConnectionHealth
  | Redis.Redis
  | Crypto.Crypto,
  | TaskEngineConfigurationError
  | Redis.RedisError
  | NodeRedisPool.UnsupportedRedisTopology
  | NodeRedisPool.InvalidRedisConfiguration
> =>
  layerNoDeps(config.engine).pipe(
    Layer.provideMerge(
      Layer.merge(NodeRedisPool.layer(config.redis), NodeCrypto.layer),
    ),
  );
