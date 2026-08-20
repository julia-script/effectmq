/** Redis-facing task record schemas. @internal */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import { UnknownFromMsgpack } from "./MessagePack.js";
import {
  CompletionPolicySchema,
  DateFromNumberSchema,
  TaskIdentitySchema,
  TaskOutcomeSchema,
} from "./TaskRecord.js";

export const TextFromBytes = Schema.Unknown.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformOrFail((value: unknown, options) => {
      if (typeof value === "string") return Effect.succeed(value);
      if (!(value instanceof Uint8Array)) {
        return Effect.fail(
          new SchemaIssue.InvalidValue(
            { message: "Expected a string or Uint8Array Redis value" },
            value,
            options,
          ),
        );
      }
      return Effect.try({
        try: () => Buffer.from(value).toString("utf8"),
        catch: (cause) =>
          new SchemaIssue.InvalidValue(
            { message: `Byte conversion failed: ${String(cause)}` },
            value,
            options,
          ),
      });
    }),
    encode: SchemaGetter.transform((value: string): unknown => value),
  }),
);

const numberFromText = (integer: boolean) =>
  SchemaGetter.transformOrFail((value: string, options) => {
    const pattern = integer
      ? /^-?(?:0|[1-9]\d*)$/
      : /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;
    const parsed = Number(value);
    return pattern.test(value) &&
      Number.isFinite(parsed) &&
      Math.abs(parsed) <= Number.MAX_SAFE_INTEGER
      ? Effect.succeed(parsed)
      : Effect.fail(
          new SchemaIssue.InvalidValue(
            {
              message: integer
                ? "Expected a safe decimal integer"
                : "Expected a safe decimal number",
            },
            value,
            options,
          ),
        );
  });

export const NumberFromBytes = TextFromBytes.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: numberFromText(false),
    encode: SchemaGetter.transform(String),
  }),
);

export const IntegerFromBytes = TextFromBytes.pipe(
  Schema.decodeTo(Schema.Int, {
    decode: numberFromText(true),
    encode: SchemaGetter.transform(String),
  }),
);

export const BooleanFromBytes = TextFromBytes.pipe(
  Schema.decodeTo(Schema.Boolean, {
    decode: SchemaGetter.transformOrFail((value, options) =>
      value === "1"
        ? Effect.succeed(true)
        : value === "0"
          ? Effect.succeed(false)
          : Effect.fail(
              new SchemaIssue.InvalidValue(
                { message: 'Expected Redis boolean "0" or "1"' },
                value,
                options,
              ),
            ),
    ),
    encode: SchemaGetter.transform((value) => (value ? "1" : "0")),
  }),
);

const msgpackListFromBytes = <S extends Schema.Top>(item: S) =>
  UnknownFromMsgpack.pipe(Schema.decodeTo(Schema.Array(item)));

export const EngineTaskSchema = Schema.Struct({
  id: TextFromBytes,
  protocolVersion: IntegerFromBytes,
  schemaId: TextFromBytes,
  generation: IntegerFromBytes,
  name: TextFromBytes,
  delay: NumberFromBytes,
  maxRetries: IntegerFromBytes,
  maxStalledCount: IntegerFromBytes,
  maxErrorEntries: IntegerFromBytes,
  maxRelationships: IntegerFromBytes,
  maxEventEntries: IntegerFromBytes,
  taskRecordRetentionMs: IntegerFromBytes,
  resultRetentionMs: IntegerFromBytes,
  terminalIndexRetentionMs: IntegerFromBytes,
  deadLetterRetentionMs: IntegerFromBytes,
  eventRetentionMs: IntegerFromBytes,
  attempt: IntegerFromBytes,
  handlerFailureCount: IntegerFromBytes,
  stalledAttemptCount: IntegerFromBytes,
  onSuccessPolicy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
  onFailurePolicy: TextFromBytes.pipe(Schema.decodeTo(CompletionPolicySchema)),
  createdAt: IntegerFromBytes.pipe(Schema.decodeTo(DateFromNumberSchema)),
  updatedAt: IntegerFromBytes.pipe(Schema.decodeTo(DateFromNumberSchema)),
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

export const EngineTerminalResultSchema = Schema.Struct({
  protocolVersion: IntegerFromBytes,
  schemaId: TextFromBytes,
  generation: IntegerFromBytes,
  outcome: TextFromBytes.pipe(Schema.decodeTo(TaskOutcomeSchema)),
  settledAt: IntegerFromBytes,
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
