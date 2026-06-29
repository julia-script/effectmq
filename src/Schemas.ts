import * as Schema from "effect/Schema";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";

const CompletionPolicySchema = Schema.Literals([
  "delete",
  "keep",
  "mark-as-success",
  "mark-as-failure",
]);

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
export const TaskErrorSchema = Schema.Union([
  StalledErrorSchema,
  CanceledErrorSchema,
]);

export type TaskErrorSchema = typeof TaskErrorSchema.Type;
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
