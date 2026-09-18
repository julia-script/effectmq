/** Typed task-owned history, pagination, and progress failures. @module */
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { IntegerFromBytes, TextFromBytes } from "./EngineRecord.js";
import * as StorageProtocol from "./StorageProtocol.js";

/** Identity of one history; a task's result may outlive this resource. */
export interface Identity {
  readonly queue: string;
  readonly taskId: string;
  readonly generation: number;
}

/** The task record for this exact generation is no longer available. */
export class HistoryUnavailable extends Data.TaggedError(
  "HistoryUnavailable",
)<Identity> {}
/** This generation was offered without a progress schema. */
export class HistoryDisabled extends Data.TaggedError(
  "HistoryDisabled",
)<Identity> {}
/** The cursor or page size is invalid for this history. */
export class InvalidHistoryCursor extends Data.TaggedError(
  "InvalidHistoryCursor",
)<{
  readonly reason: string;
  readonly cursor?: string;
}> {}
/** Some entries after the requested position have been trimmed. */
export class HistoryCursorExpired extends Data.TaggedError(
  "HistoryCursorExpired",
)<{
  readonly requested: string;
  readonly earliestCursor: string;
}> {}
/** Stored history is structurally invalid. */
export class CorruptHistory extends Data.TaggedError("CorruptHistory")<{
  readonly cause: unknown;
}> {}

/** Operational failure of a managed handler's progress emission, not a business failure. */
export class ProgressWriteError extends Data.TaggedError("ProgressWriteError")<
  Identity & {
    readonly reason: "WriteFailed" | "IndeterminateWrite";
    readonly cause: unknown;
  }
> {}

/** Attempt-bound writer passed as the second argument to managed handlers. */
export interface Context<Progress extends Schema.Top> {
  readonly progress: (
    value: Progress["Type"],
  ) => Effect.Effect<string, ProgressWriteError, Progress["EncodingServices"]>;
}

/** Compact lifecycle metadata; payloads and results remain in their own APIs. */
export const LifecycleSchema = Schema.Struct({
  _tag: Schema.Literals([
    "task.created",
    "task.updated",
    "task.moved",
    "task.failed",
    "task.completed",
  ]),
  previousState: Schema.optional(Schema.String),
  newState: Schema.optional(Schema.String),
  from: Schema.optional(Schema.String),
  to: Schema.optional(Schema.String),
  terminal: Schema.optional(Schema.Boolean),
  retryAt: Schema.optional(Schema.Number),
  failureKind: Schema.optional(Schema.String),
  policy: Schema.optional(Schema.String),
});
export type LifecycleEvent = typeof LifecycleSchema.Type;

/** One append in an exact generation's ordered history. */
export interface Entry<Progress> {
  readonly id: string;
  readonly taskId: string;
  readonly generation: number;
  readonly attempt: number;
  readonly timestamp: Date;
  readonly event:
    | { readonly _tag: "Progress"; readonly data: Progress }
    | { readonly _tag: "Lifecycle"; readonly data: LifecycleEvent };
}

/** Live page, not a snapshot across requests; an empty page can be polled again. */
export interface Page<Progress> {
  readonly entries: ReadonlyArray<Entry<Progress>>;
  readonly cursor: string;
  readonly hasMore: boolean;
  readonly truncated: boolean;
}

/** Page sizes default to 100 and must be integers from 1 through 1,000. */
export interface ReadOptions {
  readonly after?: string;
  readonly limit?: number;
}

/** @internal */
export interface Position {
  readonly sequence: number;
  readonly id: string;
}
const cursorPrefix = "effectmq:history:v1:";
const validRedisId = (id: string): boolean =>
  /^(0|[1-9]\d*)-(0|[1-9]\d*)$/.test(id) &&
  id
    .split("-")
    .every(
      (part) =>
        part.length < 20 ||
        (part.length === 20 && part <= "18446744073709551615"),
    );
/** @internal */
export const cursor = (identity: Identity, position: Position): string =>
  cursorPrefix +
  Buffer.from(
    JSON.stringify([
      identity.queue,
      identity.taskId,
      identity.generation,
      position.sequence,
      position.id,
    ]),
  ).toString("base64url");

/** @internal */
export const parseCursor = (identity: Identity, value: string) =>
  Effect.try({
    try: (): Position => {
      if (!value.startsWith(cursorPrefix))
        throw new Error("Expected a history cursor");
      const encoded = value.slice(cursorPrefix.length);
      if (!/^[A-Za-z0-9_-]+$/.test(encoded))
        throw new Error("Invalid cursor encoding");
      const bytes = Buffer.from(encoded, "base64url");
      if (bytes.toString("base64url") !== encoded)
        throw new Error("Noncanonical cursor");
      const fields: unknown = JSON.parse(bytes.toString("utf8"));
      if (
        !Array.isArray(fields) ||
        fields.length !== 5 ||
        fields[0] !== identity.queue ||
        fields[1] !== identity.taskId ||
        fields[2] !== identity.generation ||
        !Number.isSafeInteger(fields[3]) ||
        fields[3] < 0 ||
        typeof fields[4] !== "string" ||
        !validRedisId(fields[4])
      )
        throw new Error("Mismatched or invalid cursor fields");
      return { sequence: fields[3], id: fields[4] };
    },
    catch: (cause) =>
      new InvalidHistoryCursor({ cursor: value, reason: String(cause) }),
  });

/** Redis field decoder. @internal */
export const RecordSchema = Schema.Struct({
  taskId: TextFromBytes,
  generation: IntegerFromBytes,
  attempt: IntegerFromBytes,
  timestamp: IntegerFromBytes,
  sequence: IntegerFromBytes,
  protocolVersion: IntegerFromBytes,
  schemaId: TextFromBytes,
  kind: TextFromBytes,
  data: TextFromBytes,
});

/** Validates a stored entry without requiring the application's progress schema. @internal */
export const decodeRecord = Effect.fnUntraced(function* (
  identity: Identity,
  schemaId: string,
  id: string,
  fields: unknown,
) {
  const record = yield* Schema.decodeUnknownEffect(RecordSchema)(fields);
  if (record.protocolVersion !== 1)
    return yield* new StorageProtocol.UnsupportedProtocolVersion({
      encountered: record.protocolVersion,
      supported: [1],
    });
  if (record.schemaId !== schemaId)
    return yield* new StorageProtocol.SchemaIdentityMismatch({
      expected: schemaId,
      encountered: record.schemaId,
    });
  if (
    record.taskId !== identity.taskId ||
    record.generation !== identity.generation ||
    record.attempt < 0 ||
    record.sequence < 1 ||
    record.timestamp < 0 ||
    record.timestamp > 8_640_000_000_000_000 ||
    !validRedisId(id)
  ) {
    return yield* new CorruptHistory({
      cause: "Invalid history entry identity or counters",
    });
  }
  let event: Entry<string>["event"];
  if (record.kind === "Progress")
    event = { _tag: "Progress", data: record.data };
  else if (record.kind === "Lifecycle") {
    const value = yield* Effect.try({
      try: (): unknown => JSON.parse(record.data),
      catch: (cause) => new CorruptHistory({ cause }),
    });
    event = {
      _tag: "Lifecycle",
      data: yield* Schema.decodeUnknownEffect(LifecycleSchema)(value),
    };
  } else
    return yield* new CorruptHistory({ cause: "Unknown history event kind" });
  const entry: Entry<string> = {
    id,
    taskId: record.taskId,
    generation: record.generation,
    attempt: record.attempt,
    timestamp: new Date(record.timestamp),
    event,
  };
  return { entry, sequence: record.sequence };
});
