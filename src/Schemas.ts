/**
 * Shared schemas for the task model: completion policies, the built-in task
 * error types, and {@link makeTaskSchema} which assembles a fully-typed task
 * schema from payload/success/error schemas.
 *
 * @module
 */
import { type Effect, SchemaGetter } from "effect";
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

export const errorEntrySchema = <Error extends Schema.Top>(error: Error) =>
  Schema.Struct({
    error: error,
    timestamp: DateFromNumberSchema,
    retryAt: Schema.optional(DateFromNumberSchema),
  });

export type ErrorEntry<Error> = {
  error: Error;
  timestamp: Date;
  retryAt?: Date;
};
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

const DateFromNumberSchema = Schema.Number.pipe(
  Schema.decodeTo(Schema.Date, {
    decode: SchemaGetter.transform((value) => {
      return new Date(value);
    }),
    encode: SchemaGetter.transform((value) => {
      return value.getTime();
    }),
  }),
);

/**
 * A decoded task as seen by a handler: the typed payload/success/error fields
 * plus the engine-assigned `id` and `name`.
 */
export interface Task<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly _tag: "Task";
  readonly id: string;
  readonly name: string;
  readonly payload: Payload["Type"];
  readonly success?: Success["Type"] | undefined;
  readonly errors: readonly ErrorEntry<
    Error["Type"] | StalledErrorSchema | CanceledErrorSchema
  >[];
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly delay: number;
  readonly maxRetries: number;
  readonly onSuccessPolicy: CompletionPolicy;
  readonly onFailurePolicy: CompletionPolicy;
}
/**
 * Build a fully-typed task schema from a task's `payload`, `success`, and
 * `error` schemas. The resulting struct decodes the stored task hash: the
 * payload/success fields are JSON-decoded, and `errors` accepts both the
 * built-in {@link TaskErrorSchema} and the task's own error type.
 */
export const makeTaskSchema = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(config: {
  payloadSchema: Payload;
  successSchema: Success;
  errorSchema: Error;
}) => {
  const payloadDecoder = Schema.decodeTo(config.payloadSchema)(
    Schema.Unknown,
  ) as unknown as Schema.Union<[Schema.decodeTo<Payload, Schema.Unknown>]>;
  const schema = Schema.Struct({
    _tag: Schema.tagDefaultOmit("Task"),
    id: Schema.String,
    name: Schema.String,
    delay: Schema.Number,
    maxRetries: Schema.Number,
    onSuccessPolicy: CompletionPolicySchema,
    onFailurePolicy: CompletionPolicySchema,
    createdAt: Schema.Date,
    updatedAt: Schema.Date,

    payload: payloadDecoder,
    errors: errorEntrySchema(
      Schema.Unknown.pipe(
        Schema.decodeTo(Schema.Union([TaskErrorSchema, config.errorSchema])),
      ),
    ).pipe(Schema.Array),

    success: Schema.Unknown.pipe(
      Schema.decodeTo(config.successSchema),
      Schema.optional,
    ),
  });

  return schema satisfies Schema.Schema<Task<Payload, Success, Error>>;
};

export const decodeTask = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  config: {
    payloadSchema: Payload;
    successSchema: Success;
    errorSchema: Error;
  },
  task: EngineTask,
): Effect.Effect<
  Task<Payload, Success, Error>,
  Schema.SchemaError,
  Error["DecodingServices"]
> => {
  const decode = Schema.decodeEffect(
    makeTaskSchema<Payload, Success, Error>(config),
  );
  return decode(task);
};

export const encodeTask = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  config: {
    payloadSchema: Payload;
    successSchema: Success;
    errorSchema: Error;
  },
  task: Task<Payload, Success, Error>,
): Effect.Effect<EngineTask, Schema.SchemaError, Error["EncodingServices"]> =>
  Schema.encodeEffect(makeTaskSchema<Payload, Success, Error>(config))(task);

export type TaskSchema<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = ReturnType<typeof makeTaskSchema<Payload, Success, Error>>;
export const EngineTaskSchema = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  delay: Schema.NumberFromString,
  maxRetries: Schema.NumberFromString,
  onSuccessPolicy: CompletionPolicySchema,
  onFailurePolicy: CompletionPolicySchema,
  createdAt: Schema.NumberFromString.pipe(
    Schema.decodeTo(DateFromNumberSchema),
  ),
  updatedAt: Schema.NumberFromString.pipe(
    Schema.decodeTo(DateFromNumberSchema),
  ),
  payload: Schema.Unknown.pipe(Schema.fromJsonString),
  success: Schema.Unknown.pipe(Schema.optional),
  errors: Schema.Struct({
    timestamp: Schema.Number,
    error: Schema.Unknown,
    retryAt: Schema.optional(Schema.Number),
  }).pipe(Schema.Array, Schema.fromJsonString),
});

export type EngineTask = typeof EngineTaskSchema.Type;

export const EngineTaskInsertSchema = Schema.Struct({
  prefix: Schema.String,
  id: Schema.String,
  name: Schema.String,
  payload: Schema.Unknown,
  delay: Schema.Number,
  maxRetries: Schema.Number,
  onSuccessPolicy: CompletionPolicySchema,

  onFailurePolicy: CompletionPolicySchema,
});
export type EngineTaskInsert = typeof EngineTaskInsertSchema.Type;

const RedisEngineEntriesSchema = Schema.Array(Schema.String).pipe(
  Schema.decodeTo(Schema.Record(Schema.String, Schema.String), {
    encode: SchemaGetter.transform((value) => {
      return Object.entries(value).flatMap(([key, value]) => [
        key,
        String(value),
      ]);
    }),
    decode: SchemaGetter.transform((value) => {
      const result: Record<string, string> = {};
      for (let i = 0; i < value.length; i += 2) {
        result[value[i]] = value[i + 1];
      }
      return result;
    }),
  }),
);

export const TaskLists = Schema.Literals([
  "wait",
  "scheduled",
  "active",
  "failed",
  "success",
]);

export const EngineTaskFromRedisEntriesSchema = Schema.Array(
  Schema.String,
).pipe(
  Schema.decodeTo(RedisEngineEntriesSchema),
  Schema.decodeTo(EngineTaskSchema),
);

const eventBase = {
  id: Schema.String,
  taskId: Schema.String,
  payload: Schema.String,
};
export const EventSchema = Schema.Union([
  Schema.TaggedStruct("task.created", {
    ...eventBase,
    payload: Schema.Struct({
      newTask: EngineTaskFromRedisEntriesSchema,
    }).pipe(Schema.fromJsonString),
  }),
  Schema.TaggedStruct("task.updated", {
    ...eventBase,
    payload: Schema.Struct({
      existingTask: EngineTaskFromRedisEntriesSchema,
      newTask: EngineTaskFromRedisEntriesSchema,
    }).pipe(Schema.fromJsonString),
  }),
  Schema.TaggedStruct("task.failed", {
    ...eventBase,
    // the payload is JSON-decoded once here; error/retryAt are already values
    payload: Schema.Struct({
      policy: CompletionPolicySchema,
      error: Schema.Unknown,
      retryAt: Schema.Number.pipe(Schema.optional),
    }).pipe(Schema.fromJsonString),
  }),
  Schema.TaggedStruct("task.completed", {
    ...eventBase,
    payload: Schema.Struct({
      success: Schema.Unknown,
      policy: CompletionPolicySchema,
    }).pipe(Schema.fromJsonString),
  }),
  Schema.TaggedStruct("task.moved", {
    ...eventBase,
    payload: Schema.Struct({
      from: TaskLists.pipe(Schema.optional),
      to: TaskLists.pipe(Schema.optional),
    }).pipe(Schema.fromJsonString),
  }),
]);

export type Event = typeof EventSchema.Type;

export const EventFromReply = EventSchema.pipe(Schema.fromJsonString);

const EventTypeSchema = EventSchema.mapMembers((member) =>
  member.map((member) => member.fields._tag),
);
export type EventType = typeof EventTypeSchema.Type;

/**
 * Decode a single entry from an `XREAD` reply into a typed {@link Event}.
 *
 * Redis returns each stream entry as `[eventId, ["taskId", ..., "_tag", ...,
 * "payload", ...]]`. We flatten that nested tuple into a flat key/value list,
 * fold it into a record, then decode into the tagged {@link EventSchema}.
 */
export const IncomingRedisEventSchema = Schema.Tuple([
  Schema.String,
  Schema.Tuple([
    Schema.Literal("taskId"),
    Schema.String,
    Schema.Literal("_tag"),
    EventTypeSchema,
    Schema.Literal("payload"),
    Schema.String,
  ]),
]).pipe(
  Schema.decodeTo(
    Schema.Tuple([Schema.String, Schema.String.pipe(Schema.Array)]),
  ),
  Schema.decodeTo(Schema.String.pipe(Schema.Array), {
    decode: SchemaGetter.transform((value) => {
      const eventId = value[0];
      const [, taskId, , _tag, , payload] = value[1];
      return [
        "id",
        eventId,
        "taskId",
        taskId,
        "_tag",
        _tag,
        "payload",
        payload,
      ] as const;
    }),
    encode: SchemaGetter.transform((value) => {
      const [id, ...rest] = value;
      return [id, rest] as const;
    }),
  }),
  Schema.decodeTo(RedisEngineEntriesSchema),
  Schema.decodeTo(EventSchema),
);
export const decodeIncomingRedisEventList = Schema.decodeUnknownEffect(
  Schema.Array(IncomingRedisEventSchema),
);
