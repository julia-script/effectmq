/** Public schemas and codecs for typed task records. @module */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as StorageProtocol from "./StorageProtocol.js";

export const CompletionPolicySchema = Schema.Literals([
  "delete",
  "keep",
  "mark-as-success",
  "mark-as-failure",
]);
export type CompletionPolicy = typeof CompletionPolicySchema.Type;

export const TaskIdentitySchema = Schema.Struct({
  queue: Schema.String,
  id: Schema.String,
  generation: Schema.Number,
});
export type TaskIdentity = typeof TaskIdentitySchema.Type;

export const TaskOutcomeSchema = Schema.Literals(["success", "failure"]);
export type TaskOutcome = typeof TaskOutcomeSchema.Type;

export const DateFromNumberSchema = Schema.Number.pipe(
  Schema.decodeTo(Schema.Date, {
    decode: SchemaGetter.transform((value) => new Date(value)),
    encode: SchemaGetter.transform((value) => value.getTime()),
  }),
);

export const errorEntrySchema = <Error extends Schema.Top>(error: Error) =>
  Schema.Struct({
    error,
    timestamp: DateFromNumberSchema,
    retryAt: Schema.optional(DateFromNumberSchema),
  });

export interface ErrorEntry<Error> {
  readonly error: Error;
  readonly timestamp: Date;
  readonly retryAt?: Date;
}

export class StalledErrorSchema extends Schema.TaggedError<StalledErrorSchema>()(
  "~effectmq/Error/Stalled",
  { timestamp: Schema.Number },
) {
  static of(timestamp: number) {
    return new StalledErrorSchema({ timestamp });
  }
}

export class CanceledErrorSchema extends Schema.TaggedError<CanceledErrorSchema>()(
  "~effectmq/Error/Canceled",
  { timestamp: Schema.Number },
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

export interface Task<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly _tag: "Task";
  readonly id: string;
  readonly generation: number;
  readonly name: string;
  readonly payload: Payload["Type"];
  readonly success?: Success["Type"];
  readonly errors: readonly ErrorEntry<
    Error["Type"] | StalledErrorSchema | CanceledErrorSchema
  >[];
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly delay: number;
  readonly maxRetries: number;
  readonly maxStalledCount: number;
  readonly maxErrorEntries: number;
  readonly maxRelationships: number;
  readonly maxEventEntries: number;
  readonly taskRecordRetentionMs: number;
  readonly resultRetentionMs: number;
  readonly terminalIndexRetentionMs: number;
  readonly deadLetterRetentionMs: number;
  readonly eventRetentionMs: number;
  readonly attempt: number;
  readonly handlerFailureCount: number;
  readonly stalledAttemptCount: number;
  readonly onSuccessPolicy: CompletionPolicy;
  readonly onFailurePolicy: CompletionPolicy;
}

/** Decoded durable record accepted by {@link decodeTask}. */
export interface StoredTaskRecord {
  readonly id: string;
  readonly protocolVersion: number;
  readonly schemaId: string;
  readonly generation: number;
  readonly name: string;
  readonly delay: number;
  readonly maxRetries: number;
  readonly maxStalledCount: number;
  readonly maxErrorEntries: number;
  readonly maxRelationships: number;
  readonly maxEventEntries: number;
  readonly taskRecordRetentionMs: number;
  readonly resultRetentionMs: number;
  readonly terminalIndexRetentionMs: number;
  readonly deadLetterRetentionMs: number;
  readonly eventRetentionMs: number;
  readonly attempt: number;
  readonly handlerFailureCount: number;
  readonly stalledAttemptCount: number;
  readonly onSuccessPolicy: CompletionPolicy;
  readonly onFailurePolicy: CompletionPolicy;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly payload: unknown;
  readonly success?: unknown;
  readonly errors: readonly {
    readonly timestamp: number;
    readonly error: unknown;
    readonly retryAt?: number;
  }[];
  readonly creator?: TaskIdentity;
  readonly outcome?: TaskOutcome;
}

export const makeTaskSchema = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(config: {
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly errorSchema: Error;
}) => {
  const payloadDecoder = Schema.Unknown.pipe(
    Schema.decodeTo(config.payloadSchema),
  );
  return Schema.Struct({
    _tag: Schema.tagDefaultOmit("Task"),
    id: Schema.String,
    generation: Schema.Number,
    name: Schema.String,
    delay: Schema.Number,
    maxRetries: Schema.Number,
    maxStalledCount: Schema.Number,
    maxErrorEntries: Schema.Number,
    maxRelationships: Schema.Number,
    maxEventEntries: Schema.Number,
    taskRecordRetentionMs: Schema.Number,
    resultRetentionMs: Schema.Number,
    terminalIndexRetentionMs: Schema.Number,
    deadLetterRetentionMs: Schema.Number,
    eventRetentionMs: Schema.Number,
    attempt: Schema.Number,
    handlerFailureCount: Schema.Number,
    stalledAttemptCount: Schema.Number,
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
};

export const decodeTask = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  config: {
    readonly schemaId: string;
    readonly payloadSchema: Payload;
    readonly successSchema: Success;
    readonly errorSchema: Error;
  },
  task: StoredTaskRecord,
): Effect.fn.Return<
  Task<Payload, Success, Error>,
  Schema.SchemaError | StorageProtocol.StorageProtocolError,
  | Payload["DecodingServices"]
  | Success["DecodingServices"]
  | Error["DecodingServices"]
> {
  const decode = Schema.decodeEffect(makeTaskSchema(config));
  if (
    !StorageProtocol.readableProtocolVersions.includes(
      task.protocolVersion as 1,
    )
  ) {
    return yield* new StorageProtocol.UnsupportedProtocolVersion({
      encountered: task.protocolVersion,
      supported: StorageProtocol.readableProtocolVersions,
    });
  }
  if (task.schemaId !== config.schemaId) {
    return yield* new StorageProtocol.SchemaIdentityMismatch({
      expected: config.schemaId,
      encountered: task.schemaId,
    });
  }
  const errors = yield* Effect.forEach(task.errors, (entry) => {
    const value = entry.error;
    const isBuiltIn =
      typeof value === "object" &&
      value !== null &&
      "_tag" in value &&
      Object.values(StorageProtocol.builtInErrorTags).includes(
        value._tag as never,
      );
    return isBuiltIn
      ? Effect.succeed(entry)
      : StorageProtocol.decodeValue(value, config.schemaId, "failure").pipe(
          Effect.map((error) => ({ ...entry, error })),
        );
  });
  return (yield* decode({
    ...task,
    payload: yield* StorageProtocol.decodeValue(
      task.payload,
      config.schemaId,
      "payload",
    ),
    success:
      task.success === undefined
        ? undefined
        : yield* StorageProtocol.decodeValue(
            task.success,
            config.schemaId,
            "success",
          ),
    errors,
  })) as Task<Payload, Success, Error>;
});

export const encodeTask = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  config: {
    readonly payloadSchema: Payload;
    readonly successSchema: Success;
    readonly errorSchema: Error;
  },
  task: Task<Payload, Success, Error>,
) => {
  const schema = makeTaskSchema(config);
  return Schema.encodeEffect(schema)(task as typeof schema.Type);
};

export type TaskSchema<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = ReturnType<typeof makeTaskSchema<Payload, Success, Error>>;
