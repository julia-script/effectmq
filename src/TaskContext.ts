/**
 * Ambient identity of the task generation currently being processed.
 * `TaskQueue.complete` provides it around the handler; nested offers use it as
 * informational creator provenance and, only when explicitly requested, as a
 * result-retention holder.
 *
 * @module
 */
import * as Context from "effect/Context";
import type { TaskIdentity } from "./TaskRecord.js";

/** The identity of the task generation whose handler is running, if any. */
export const currentTask = Context.Reference<TaskIdentity | undefined>(
  "@effectmq/core/TaskContext/currentTask",
  { defaultValue: () => undefined },
);
