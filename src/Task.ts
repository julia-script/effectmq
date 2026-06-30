/**
 * Typed task definitions: the schema-bearing description of a unit of work
 * (payload, success, and error types) that a {@link TaskQueue} processes.
 *
 * @module
 */
import { Schema } from "effect";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import type { TaskSchema } from "./Schemas.js";

const TypeId = "~effectmq/Task" as const;

/**
 * A decoded task as seen by a handler: the typed payload/success/error fields
 * plus the engine-assigned `id` and `name`.
 */
export type Task<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = TaskSchema<Payload, Success, Error>["Type"] & { id: string; name: string };

/**
 * The schema-bearing definition of a task type: its name, payload/success/error
 * schemas, and how to derive an idempotency key from a payload.
 */
export interface TaskDefinition<
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
> {
  readonly [TypeId]: typeof TypeId;

  readonly name: string;
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly errorSchema: Error;

  readonly idempotencyKey: (payload: Payload["Type"]) => string;
}
type ResolvePayload<T extends AnyStructSchema | Schema.Struct.Fields> =
  T extends AnyStructSchema
    ? T
    : Schema.Struct<T extends Schema.Struct.Fields ? T : never>;

const resolvePayloadSchema = <T extends AnyStructSchema | Schema.Struct.Fields>(
  payload: T,
): ResolvePayload<T> => {
  return Schema.isSchema(payload)
    ? (payload as ResolvePayload<T>)
    : (Schema.Struct(payload) as ResolvePayload<T>);
};

/**
 * Define a task type.
 *
 * `payload` may be either a `Schema.Struct` or a bare fields object (which is
 * wrapped into a struct). `successSchema`/`errorSchema` default to
 * `Schema.Void`/`Schema.Never`. When `idempotencyKey` is omitted, a random
 * key is generated per offer, so identical payloads are treated as distinct.
 *
 * @returns A {@link TaskDefinition} to pass to `TaskQueue.make`.
 */
export const make = <
  Payload extends AnyStructSchema | Schema.Struct.Fields,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(config: {
  name: string;
  successSchema: Success;
  errorSchema: Error;
  payload: Payload;
  idempotencyKey?: (payload: Payload["Type"]) => string;
}): TaskDefinition<ResolvePayload<Payload>, Success, Error> => {
  const payloadSchema = resolvePayloadSchema(config.payload);
  const successSchema = (config.successSchema ?? Schema.Void) as Success;
  const errorSchema = (config.errorSchema ?? Schema.Never) as Error;

  const self: TaskDefinition<ResolvePayload<Payload>, Success, Error> = {
    [TypeId]: TypeId,
    name: config.name,
    payloadSchema: payloadSchema,
    successSchema: successSchema,
    errorSchema: errorSchema,

    idempotencyKey:
      config.idempotencyKey ?? (() => `${config.name}/${crypto.randomUUID()}`),
  };

  return self;
};
