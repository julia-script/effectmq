/**
 * Shared schemas for the task model: completion policies, the built-in task
 * error types, and {@link makeTaskSchema} which assembles a fully-typed task
 * schema from payload/success/error schemas.
 *
 * @module
 */
import * as Schema from "effect/Schema";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";

/**
 * Policy applied to a task once it completes (on success or failure):
 * `delete` removes it, `keep` clears it from all lists, and
 * `mark-as-success`/`mark-as-failure` move it to the corresponding list.
 */
const CompletionPolicySchema = Schema.Literals([
  "delete",
  "keep",
  "mark-as-success",
  "mark-as-failure",
]);

/** A completion policy value (`delete` | `keep` | `mark-as-success` | `mark-as-failure`). */
export type CompletionPolicy = typeof CompletionPolicySchema.Type;
export class StalledErrorSchema extends Schema.TaggedErrorClass<StalledErrorSchema>()(
  "~effectmq/Error/Stalled",
  {
    timestamp: Schema.Number,
  },
) {
  static of(timestamp: number) {
    return new StalledErrorSchema({ timestamp });
  }
}
export class CanceledErrorSchema extends Schema.TaggedErrorClass<CanceledErrorSchema>()(
  "~effectmq/Error/Canceled",
  {
    timestamp: Schema.Number,
  },
) {
  static of(timestamp: number) {
    return new CanceledErrorSchema({ timestamp });
  }
}
/** Union of the engine's built-in task errors (`Stalled`, `Canceled`). */
export const TaskErrorSchema = Schema.Union([
  StalledErrorSchema,
  CanceledErrorSchema,
]);

export type TaskErrorSchema = typeof TaskErrorSchema.Type;

/**
 * Build a fully-typed task schema from a task's `payload`, `success`, and
 * `error` schemas. The resulting struct decodes the stored task hash: the
 * payload/success fields are JSON-decoded, and `errors` accepts both the
 * built-in {@link TaskErrorSchema} and the task's own error type.
 */
export const makeTaskSchema = <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(config: {
  payload: Payload;
  success: Success;
  error: Error;
}) =>
  Schema.Struct({
    _tag: Schema.tagDefaultOmit("Task"),
    id: Schema.String,
    name: Schema.String,
    delay: Schema.Number,
    maxRetries: Schema.Number,
    onSuccessPolicy: CompletionPolicySchema,
    onFailurePolicy: CompletionPolicySchema,
    createdAt: Schema.Date,
    updatedAt: Schema.Date,

    payload: config.payload.pipe(Schema.fromJsonString),
    errors: Schema.Union([TaskErrorSchema, config.error]).pipe(Schema.Array),
    success: config.success.pipe(Schema.fromJsonString, Schema.optional),
  });

export type TaskSchema<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = ReturnType<typeof makeTaskSchema<Payload, Success, Error>>;
export const EngineTaskSchema = makeTaskSchema({
  payload: Schema.Struct({}),
  success: Schema.Void,
  error: Schema.Never,
}).pipe(Schema.toEncoded);
export type EngineTask = typeof EngineTaskSchema.Type;

export const EngineTaskInsertSchema = Schema.Struct({
  prefix: Schema.String,
  id: Schema.String,
  name: Schema.String,
  payload: Schema.String,
  delay: Schema.Number,
  maxRetries: Schema.Number,
  onSuccessPolicy: CompletionPolicySchema,

  onFailurePolicy: CompletionPolicySchema,
});
export type EngineTaskInsert = typeof EngineTaskInsertSchema.Type;
