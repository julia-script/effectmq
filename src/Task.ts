/**
 * Typed task definitions: the schema-bearing description of a unit of work
 * (payload, success, and error types) that a {@link TaskQueue} processes.
 *
 * @module
 */
import { type Effect, Schedule, Schema } from "effect";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import { buildFromOptions } from "./utils.js";

const TypeId = "~effectmq/Task" as const;

/** Default retry cap applied when `maxRetries` is not set, so an unbounded schedule can't loop forever. */
const DEFAULT_MAX_RETRIES = 5;

/**
 * A decoded task as seen by a handler: the typed payload/success/error fields
 * plus the engine-assigned `id` and `name`.
 */
export type { Task } from "./Schemas.js";

/**
 * The schema-bearing definition of a task type: its name, payload/success/error
 * schemas, and how to derive an idempotency key from a payload.
 */
export interface TaskDefinition<
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
> {
  readonly [TypeId]: typeof TypeId;

  readonly name: string;
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

  readonly idempotencyKey: (payload: Payload["Type"]) => string;
}
export type ResolvePayload<T extends AnyStructSchema | Schema.Struct.Fields> =
  T extends AnyStructSchema
    ? T
    : Schema.Struct<T extends Schema.Struct.Fields ? T : never>;

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
  success: Success;
  error: Error;
  payload: Payload;
  maxRetries?: number | null;
  idempotencyKey?: (payload: ResolvePayload<Payload>["Type"]) => string;
  retrySchedule?: Schedule.Schedule<any, NoInfer<Error["Type"]>, any, R>;
}): TaskDefinition<ResolvePayload<Payload>, Success, Error, R> => {
  const payloadSchema = resolvePayloadSchema(config.payload);
  const successSchema = (config.success ?? Schema.Void) as Success;
  const errorSchema = (config.error ?? Schema.Never) as Error;

  const self: TaskDefinition<ResolvePayload<Payload>, Success, Error, R> = {
    [TypeId]: TypeId,
    name: config.name,
    payloadSchema: payloadSchema,
    successSchema: successSchema,
    errorSchema: errorSchema,
    retrySchedule: config.retrySchedule,
    // unset → default cap of 5; null → unbounded; a number → that number
    maxRetries:
      config.maxRetries === undefined
        ? DEFAULT_MAX_RETRIES
        : (config.maxRetries ?? Infinity),
    idempotencyKey:
      config.idempotencyKey ?? (() => `${config.name}/${crypto.randomUUID()}`),
  };

  return self;
};

/**
 * Define a task type.
 *
 * `payload` may be either a `Schema.Struct` or a bare fields object (which is
 * wrapped into a struct). `success`/`error` default to
 * `Schema.Void`/`Schema.Never`. When `idempotencyKey` is omitted, a random
 * key is generated per offer, so identical payloads are treated as distinct.
 * `retry` is a `Schedule` (or `{ while, until, times, schedule }` options)
 * that drives when a failed task is retried; `maxRetries` caps the attempts
 * (default 5; `null` for unbounded).
 *
 * @returns A {@link TaskDefinition} to pass to `TaskQueue.make`.
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
    success: Success;
    error: Error;
    payload: Payload;
    maxRetries?: number | null;
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
    payload: Payload;
    success: Success;
    error: Error;
    maxRetries?: number | null;
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
  payload,
  success,
  error,
  maxRetries,
  idempotencyKey,
  retry,
}: {
  name: string;
  payload: AnyStructSchema | Schema.Struct.Fields;
  success: Schema.Top;
  error: Schema.Top;
  maxRetries?: number | null;
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
    success,
    error,
    payload,
    maxRetries,
    idempotencyKey,
    retrySchedule: schedule,
  });
}) as never;
