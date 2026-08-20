/**
 * Shared schemas for the task model: completion policies, the built-in task
 * error types, and {@link makeTaskSchema} which assembles a fully-typed task
 * schema from payload/success/error schemas.
 *
 * @module
 */
import { Effect, SchemaGetter } from "effect";
import * as Schema from "effect/Schema";
import { Packr } from "msgpackr";
import * as StorageProtocol from "./StorageProtocol.js";

// standard msgpack only (no msgpackr record extension — Redis' cmsgpack
// can't read it) and 64-bit ints as JS numbers (timestamps would otherwise
// decode as BigInt)
const packr = new Packr({ useRecords: false, int64AsType: "number" });

/** msgpack bytes ⇄ decoded unknown value. */
export const UnknownFromMsgpack = Schema.Uint8Array.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.transform((bytes: Uint8Array): unknown =>
      packr.unpack(bytes),
    ),
    encode: SchemaGetter.transform(
      (value: unknown): Uint8Array => packr.pack(value),
    ),
  }),
);

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

/** Stable identity of one task generation at the public queue boundary. */
export const TaskIdentitySchema = Schema.Struct({
  queue: Schema.String,
  id: Schema.String,
  generation: Schema.Number,
});
export type TaskIdentity = typeof TaskIdentitySchema.Type;

/** Terminal outcome recorded when a task generation settles. */
const TaskOutcomeSchema = Schema.Literals(["success", "failure"]);
/** Terminal outcome of a settled task (`success` | `failure`). */
export type TaskOutcome = typeof TaskOutcomeSchema.Type;

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
export class StalledErrorSchema extends Schema.TaggedError<StalledErrorSchema>()(
  "~effectmq/Error/Stalled",
  {
    timestamp: Schema.Number,
  },
) {
  static of(timestamp: number) {
    return new StalledErrorSchema({ timestamp });
  }
}
export class CanceledErrorSchema extends Schema.TaggedError<CanceledErrorSchema>()(
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
  readonly generation: number;
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
/**
 * Build a fully-typed task schema from a task's `payload`, `success`, and
 * `error` schemas. The resulting struct decodes the stored task hash: the
 * payload/success fields are decoded from the v1 storage envelope, and
 * `errors` accepts both the built-in {@link TaskErrorSchema} and the task's
 * own error type.
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

  return schema satisfies Schema.Schema<Task<Payload, Success, Error>>;
};

export const decodeTask = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  config: {
    schemaId: string;
    payloadSchema: Payload;
    successSchema: Success;
    errorSchema: Error;
  },
  task: EngineTask,
): Effect.Effect<
  Task<Payload, Success, Error>,
  Schema.SchemaError | StorageProtocol.StorageProtocolError,
  Error["DecodingServices"]
> => {
  const decode = Schema.decodeEffect(
    makeTaskSchema<Payload, Success, Error>(config),
  );
  return Effect.gen(function* () {
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
    return yield* decode({
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
    });
  });
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
) => Schema.encodeEffect(makeTaskSchema<Payload, Success, Error>(config))(task);

export type TaskSchema<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = ReturnType<typeof makeTaskSchema<Payload, Success, Error>>;
/**
 * Redis replies carrying msgpack fields are read in binary mode, so scalar
 * values arrive as `Buffer`s (or strings when injected on the TS side) —
 * decode either to a utf8 string.
 */
const TextFromBytes = Schema.Unknown.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transform((value: unknown): string =>
      typeof value === "string"
        ? value
        : Buffer.from(value as Uint8Array).toString("utf8"),
    ),
    encode: SchemaGetter.transform((value: string): unknown => value),
  }),
);

const NumberFromBytes = TextFromBytes.pipe(
  Schema.decodeTo(Schema.Number, {
    decode: SchemaGetter.transform(Number),
    encode: SchemaGetter.transform(String),
  }),
);

const BooleanFromBytes = TextFromBytes.pipe(
  Schema.decodeTo(Schema.Boolean, {
    decode: SchemaGetter.transform((value) => value === "1"),
    encode: SchemaGetter.transform((value) => (value ? "1" : "0")),
  }),
);

/** A list stored as a MessagePack blob. Wrong shapes are corruption errors. */
const msgpackListFromBytes = <S extends Schema.Top>(item: S) =>
  UnknownFromMsgpack.pipe(Schema.decodeTo(Schema.Array(item)));

/**
 * The engine task as returned by the Lua library's `getTask`: a record of raw
 * hash values — scalars as utf8 bytes, structured fields (`payload`,
 * `success`, `errors`, and `creator`) as msgpack blobs decoded here.
 * Lua never unpacks the payload, so it round-trips byte-exact.
 */
export const EngineTaskSchema = Schema.Struct({
  id: TextFromBytes,
  protocolVersion: NumberFromBytes,
  schemaId: TextFromBytes,
  generation: NumberFromBytes,
  name: TextFromBytes,
  delay: NumberFromBytes,
  maxRetries: NumberFromBytes,
  maxStalledCount: NumberFromBytes,
  maxErrorEntries: NumberFromBytes,
  maxRelationships: NumberFromBytes,
  maxEventEntries: NumberFromBytes,
  taskRecordRetentionMs: NumberFromBytes,
  resultRetentionMs: NumberFromBytes,
  terminalIndexRetentionMs: NumberFromBytes,
  deadLetterRetentionMs: NumberFromBytes,
  eventRetentionMs: NumberFromBytes,
  attempt: NumberFromBytes,
  handlerFailureCount: NumberFromBytes,
  stalledAttemptCount: NumberFromBytes,
  onSuccessPolicy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
  onFailurePolicy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
  createdAt: NumberFromBytes.pipe(Schema.decodeTo(DateFromNumberSchema)),
  updatedAt: NumberFromBytes.pipe(Schema.decodeTo(DateFromNumberSchema)),
  payload: UnknownFromMsgpack,
  success: UnknownFromMsgpack.pipe(Schema.optional),
  errors: msgpackListFromBytes(
    Schema.Struct({
      timestamp: Schema.Number,
      error: Schema.Unknown,
      retryAt: Schema.optional(Schema.Number),
    }),
  ),
  creator: UnknownFromMsgpack.pipe(
    Schema.decodeTo(TaskIdentitySchema),
    Schema.optional,
  ),
  outcome: TextFromBytes.pipe(
    Schema.decodeTo(TaskOutcomeSchema),
    Schema.optional,
  ),
});

export type EngineTask = typeof EngineTaskSchema.Type;

/** Generation-specific terminal outcome retained independently of task data. */
export const EngineTerminalResultSchema = Schema.Struct({
  protocolVersion: NumberFromBytes,
  schemaId: TextFromBytes,
  generation: NumberFromBytes,
  outcome: TextFromBytes.pipe(Schema.decodeTo(TaskOutcomeSchema)),
  settledAt: NumberFromBytes,
  success: UnknownFromMsgpack.pipe(Schema.optional),
  failure: UnknownFromMsgpack.pipe(Schema.optional),
});
export type EngineTerminalResult = typeof EngineTerminalResultSchema.Type;

export const EngineTaskInsertSchema = Schema.Struct({
  prefix: Schema.String,
  id: Schema.String,
  name: Schema.String,
  schemaId: Schema.String.pipe(Schema.optional),
  payload: Schema.Unknown,
  delay: Schema.Number,
  maxRetries: Schema.Number,
  maxStalledCount: Schema.Number.pipe(Schema.optional),
  maxErrorEntries: Schema.Number.pipe(Schema.optional),
  maxRelationships: Schema.Number.pipe(Schema.optional),
  maxEventEntries: Schema.Number.pipe(Schema.optional),
  taskRecordRetentionMs: Schema.Number.pipe(Schema.optional),
  resultRetentionMs: Schema.Number.pipe(Schema.optional),
  terminalIndexRetentionMs: Schema.Number.pipe(Schema.optional),
  deadLetterRetentionMs: Schema.Number.pipe(Schema.optional),
  eventRetentionMs: Schema.Number.pipe(Schema.optional),
  onSuccessPolicy: CompletionPolicySchema,

  onFailurePolicy: CompletionPolicySchema,
  onDuplicate: Schema.Literals(["return-existing", "new-generation"]).pipe(
    Schema.optional,
  ),
  retentionHolder: TaskIdentitySchema.pipe(Schema.optional),
  creator: TaskIdentitySchema.pipe(Schema.optional),
});
export type EngineTaskInsert = typeof EngineTaskInsertSchema.Type;

export const TaskLists = Schema.Literals([
  "wait",
  "scheduled",
  "active",
  "failed",
  "success",
]);

export const ExecutionStateSchema = Schema.Literals([
  "delayed",
  "waiting",
  "leased",
  "retry-scheduled",
  "succeeded",
  "failed",
]);

const eventBase = {
  id: Schema.String,
  taskId: Schema.String,
  generation: NumberFromBytes,
  protocolVersion: NumberFromBytes,
  schemaId: TextFromBytes,
};

/**
 * A queue lifecycle event as read from the Redis Stream. Events are published
 * as flat stream fields (raw msgpack values stay binary-safe bulk strings);
 * `TaskEngine.stream` reassembles them into `{id, taskId, _tag, payload}`
 * records — task snapshots arrive as raw-entry records decoded by
 * {@link EngineTaskSchema}.
 */
export const EventSchema = Schema.Union([
  Schema.TaggedStruct("task.created", {
    ...eventBase,
    payload: Schema.Struct({
      newTask: EngineTaskSchema,
      state: TextFromBytes.pipe(Schema.decodeTo(ExecutionStateSchema)),
    }),
  }),
  Schema.TaggedStruct("task.updated", {
    ...eventBase,
    payload: Schema.Struct({
      existingTask: EngineTaskSchema,
      newTask: EngineTaskSchema,
      state: TextFromBytes.pipe(Schema.decodeTo(ExecutionStateSchema)),
    }),
  }),
  Schema.TaggedStruct("task.failed", {
    ...eventBase,
    payload: Schema.Struct({
      policy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
      error: UnknownFromMsgpack,
      retryAt: NumberFromBytes.pipe(Schema.optional),
      failureKind: TextFromBytes.pipe(
        Schema.decodeTo(Schema.Literals(["handler", "stall"])),
      ),
      attempt: NumberFromBytes,
      terminal: BooleanFromBytes,
    }),
  }),
  Schema.TaggedStruct("task.completed", {
    ...eventBase,
    payload: Schema.Struct({
      // a void success is msgpack-encoded undefined; decodes back to undefined
      success: UnknownFromMsgpack.pipe(Schema.optional),
      policy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
    }),
  }),
  Schema.TaggedStruct("task.moved", {
    ...eventBase,
    payload: Schema.Struct({
      from: TextFromBytes.pipe(Schema.decodeTo(TaskLists), Schema.optional),
      to: TextFromBytes.pipe(Schema.decodeTo(TaskLists), Schema.optional),
      previousState: TextFromBytes.pipe(
        Schema.decodeTo(ExecutionStateSchema),
        Schema.optional,
      ),
      newState: TextFromBytes.pipe(
        Schema.decodeTo(ExecutionStateSchema),
        Schema.optional,
      ),
      attempt: NumberFromBytes,
      handlerFailureCount: NumberFromBytes,
      stalledAttemptCount: NumberFromBytes,
    }),
  }),
]);

export type Event = typeof EventSchema.Type;

const EventTypeSchema = EventSchema.mapMembers((member) =>
  member.map((member) => member.fields._tag),
);
export type EventType = typeof EventTypeSchema.Type;
