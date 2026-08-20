/**
 * Typed task definitions: the schema-bearing description of a unit of work
 * (payload, success, and error types) that a `TaskQueue` processes.
 *
 * @module
 */
import { type Effect, Schedule, Schema } from "effect";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import { defaultStorageLimits, type StorageLimits } from "./StorageProtocol.js";
import { buildFromOptions } from "./utils.js";

const TypeId = "~effectmq/Task" as const;

/** Default retry cap applied when `maxRetries` is not set, so an unbounded schedule can't loop forever. */
const DEFAULT_MAX_RETRIES = 5;

/**
 * Finite retention windows, in milliseconds, for one task generation.
 *
 * Each resource expires independently during bounded maintenance sweeps. A
 * result can therefore expire before its task record or terminal index.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface RetentionPolicy {
  readonly taskRecordMs: number;
  readonly resultMs: number;
  readonly terminalIndexMs: number;
  readonly deadLetterMs: number;
  readonly eventMs: number;
}

/**
 * Default retention windows for task records, results, indexes, and events.
 *
 * Task records, terminal indexes, and events default to seven days; results to
 * one day; and dead-letter entries to 30 days.
 *
 * @category Configuration
 * @since 0.3.0
 */
export const defaultRetentionPolicy: RetentionPolicy = {
  taskRecordMs: 7 * 24 * 60 * 60 * 1000,
  resultMs: 24 * 60 * 60 * 1000,
  terminalIndexMs: 7 * 24 * 60 * 60 * 1000,
  deadLetterMs: 30 * 24 * 60 * 60 * 1000,
  eventMs: 7 * 24 * 60 * 60 * 1000,
};

const resolveRetention = (
  retention: Partial<RetentionPolicy> | undefined,
): RetentionPolicy => {
  const resolved = { ...defaultRetentionPolicy, ...retention };
  for (const [name, value] of Object.entries(resolved)) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(`${name} must be a non-negative safe integer`);
    }
  }
  return resolved;
};

const resolveStorageLimits = (
  limits: Partial<StorageLimits> | undefined,
): StorageLimits => {
  const resolved = { ...defaultStorageLimits, ...limits };
  for (const [name, value] of Object.entries(resolved)) {
    const minimum = name === "maxEventEntries" ? 1 : 0;
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw new RangeError(`${name} must be a non-negative safe integer`);
    }
  }
  return resolved;
};

/**
 * A decoded task generation as seen by a handler.
 *
 * It includes the typed payload, optional terminal value, attempt and stall
 * counters, retry policy, retention policy, and engine-assigned identity.
 *
 * @category Models
 * @since 0.1.0
 */
export type { Task } from "./Schemas.js";

/**
 * The schema-bearing definition of one task family.
 *
 * A definition owns payload, success, and failure schemas; retry behavior;
 * storage and retention limits; and the idempotency-key function used by
 * `TaskQueue.offer`.
 *
 * @category Models
 * @since 0.1.0
 */
export interface TaskDefinition<
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
> {
  readonly [TypeId]: typeof TypeId;

  readonly name: string;
  /** Stable identity of this payload/success/failure schema family. */
  readonly schemaId: string;
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly errorSchema: Error;
  readonly retrySchedule?: Schedule.Schedule<
    unknown,
    NoInfer<Error["Type"]>,
    unknown,
    R
  >;
  readonly maxRetries: number;
  readonly storageLimits: StorageLimits;
  readonly retention: RetentionPolicy;

  readonly idempotencyKey: (payload: Payload["Type"]) => string;
}
/**
 * Resolves either a struct schema or bare struct fields to a struct schema.
 *
 * @category Schemas
 * @since 0.2.0
 */
export type ResolvePayload<T extends AnyStructSchema | Schema.Struct.Fields> =
  T extends AnyStructSchema
    ? T
    : Schema.Struct<T extends Schema.Struct.Fields ? T : never>;

/**
 * Normalizes a task payload declaration to a struct schema.
 *
 * Existing schemas are returned unchanged; bare fields are wrapped with
 * `Schema.Struct`.
 *
 * @category Schemas
 * @since 0.2.0
 */
export const resolvePayloadSchema = <
  T extends AnyStructSchema | Schema.Struct.Fields,
>(
  payload: T,
): ResolvePayload<T> => {
  return Schema.isSchema(payload)
    ? (payload as ResolvePayload<T>)
    : (Schema.Struct(payload) as ResolvePayload<T>);
};

const makeInternal = <
  Payload extends AnyStructSchema | Schema.Struct.Fields,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
>(config: {
  name: string;
  schemaId?: string;
  success: Success;
  error: Error;
  payload: Payload;
  maxRetries?: number | null;
  storageLimits?: Partial<StorageLimits>;
  retention?: Partial<RetentionPolicy>;
  idempotencyKey?: (payload: ResolvePayload<Payload>["Type"]) => string;
  retrySchedule?: Schedule.Schedule<any, NoInfer<Error["Type"]>, any, R>;
}): TaskDefinition<ResolvePayload<Payload>, Success, Error, R> => {
  const payloadSchema = resolvePayloadSchema(config.payload);
  const successSchema = (config.success ?? Schema.Void) as Success;
  const errorSchema = (config.error ?? Schema.Never) as Error;

  const self: TaskDefinition<ResolvePayload<Payload>, Success, Error, R> = {
    [TypeId]: TypeId,
    name: config.name,
    schemaId: config.schemaId ?? config.name,
    payloadSchema: payloadSchema,
    successSchema: successSchema,
    errorSchema: errorSchema,
    retrySchedule: config.retrySchedule,
    // unset → default cap of 5; null → unbounded; a number → that number
    maxRetries:
      config.maxRetries === undefined
        ? DEFAULT_MAX_RETRIES
        : (config.maxRetries ?? Infinity),
    storageLimits: resolveStorageLimits(config.storageLimits),
    retention: resolveRetention(config.retention),
    idempotencyKey:
      config.idempotencyKey ?? (() => `${config.name}/${crypto.randomUUID()}`),
  };

  return self;
};

/**
 * Defines a typed task family.
 *
 * `payload` accepts either a `Schema.Struct` or bare fields. `success` and
 * `error` explicitly define the two terminal channels. `retry` accepts an
 * Effect `Schedule` or repeat-style options, while `maxRetries` independently
 * caps retries at five by default; pass `null` only for an intentionally
 * unbounded cap.
 *
 * **Gotchas**
 *
 * Without `idempotencyKey`, every call derives a random key. Identical payloads
 * are therefore distinct offers unless the caller supplies a stable key or an
 * explicit task identifier.
 *
 * **Example: Define an idempotent task with bounded retries**
 *
 * ```ts
 * import { Schema } from "effect"
 * import { Task } from "@effectmq/core"
 *
 * const sendInvoice = Task.make({
 *   name: "send-invoice",
 *   schemaId: "send-invoice/v1",
 *   payload: { invoiceId: Schema.String },
 *   success: Schema.Void,
 *   error: Schema.Struct({ reason: Schema.String }),
 *   idempotencyKey: ({ invoiceId }) => invoiceId,
 *   maxRetries: 3
 * })
 * ```
 *
 * @category Constructors
 * @since 0.1.0
 */
export const make: {
  <
    Payload extends AnyStructSchema | Schema.Struct.Fields,
    Success extends Schema.Top = Schema.Void,
    Error extends Schema.Top = Schema.Never,
    R1 = never,
    R2 = never,
    R3 = never,
  >(config: {
    name: string;
    schemaId?: string;
    success: Success;
    error: Error;
    payload: Payload;
    maxRetries?: number | null;
    storageLimits?: Partial<StorageLimits>;
    retention?: Partial<RetentionPolicy>;
    idempotencyKey?: (payload: ResolvePayload<Payload>["Type"]) => string;
    retry?: {
      while?:
        | ((
            error: NoInfer<Error["Type"]>,
          ) => boolean | Effect.Effect<boolean, NoInfer<Error["Type"]>, R1>)
        | undefined;
      until?:
        | ((
            error: NoInfer<Error["Type"]>,
          ) => boolean | Effect.Effect<boolean, NoInfer<Error["Type"]>, R2>)
        | undefined;
      times?: number | undefined;
      schedule?:
        | Schedule.Schedule<unknown, NoInfer<Error["Type"]>, unknown, R3>
        | undefined;
    };
  }): TaskDefinition<ResolvePayload<Payload>, Success, Error, R1 | R2 | R3>;

  <
    Payload extends AnyStructSchema | Schema.Struct.Fields,
    Success extends Schema.Top = Schema.Void,
    Error extends Schema.Top = Schema.Never,
    Env = never,
  >(config: {
    name: string;
    schemaId?: string;
    payload: Payload;
    success: Success;
    error: Error;
    maxRetries?: number | null;
    storageLimits?: Partial<StorageLimits>;
    retention?: Partial<RetentionPolicy>;
    idempotencyKey?: (payload: ResolvePayload<Payload>["Type"]) => string;
    retry: Schedule.Schedule<
      any,
      NoInfer<Error["Type"]>,
      NoInfer<Error["Type"]>,
      Env
    >;
  }): TaskDefinition<ResolvePayload<Payload>, Success, Error, Env>;
} = (({
  name,
  schemaId,
  payload,
  success,
  error,
  maxRetries,
  storageLimits,
  retention,
  idempotencyKey,
  retry,
}: {
  name: string;
  schemaId?: string;
  payload: AnyStructSchema | Schema.Struct.Fields;
  success: Schema.Top;
  error: Schema.Top;
  maxRetries?: number | null;
  storageLimits?: Partial<StorageLimits>;
  retention?: Partial<RetentionPolicy>;
  idempotencyKey?: (
    payload: ResolvePayload<AnyStructSchema | Schema.Struct.Fields>["Type"],
  ) => string;
  retry:
    | Effect.Repeat.Options<unknown>
    | Schedule.Schedule<unknown, unknown, unknown, unknown>;
}) => {
  const schedule = retry
    ? Schedule.isSchedule(retry)
      ? retry
      : buildFromOptions(retry)
    : undefined;
  return makeInternal({
    name,
    schemaId,
    success,
    error,
    payload,
    maxRetries,
    storageLimits,
    retention,
    idempotencyKey,
    retrySchedule: schedule,
  });
}) as never;
