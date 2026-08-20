/** Public schemas for queue lifecycle events. @module */
import * as Schema from "effect/Schema";
import {
  BooleanFromBytes,
  EngineTaskSchema,
  ExecutionStateSchema,
  IntegerFromBytes,
  NumberFromBytes,
  TaskLists,
  TextFromBytes,
} from "./EngineRecord.js";
import { UnknownFromMsgpack } from "./MessagePack.js";
import { CompletionPolicySchema } from "./TaskRecord.js";

const eventBase = {
  id: Schema.String,
  taskId: Schema.String,
  generation: IntegerFromBytes,
  protocolVersion: IntegerFromBytes,
  schemaId: TextFromBytes,
};

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
      attempt: IntegerFromBytes,
      terminal: BooleanFromBytes,
    }),
  }),
  Schema.TaggedStruct("task.completed", {
    ...eventBase,
    payload: Schema.Struct({
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
      attempt: IntegerFromBytes,
      handlerFailureCount: IntegerFromBytes,
      stalledAttemptCount: IntegerFromBytes,
    }),
  }),
]);

export type Event = typeof EventSchema.Type;

const EventTypeSchema = EventSchema.mapMembers((member) =>
  member.map((member) => member.fields._tag),
);
export type EventType = typeof EventTypeSchema.Type;
