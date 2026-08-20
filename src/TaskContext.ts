/**
 * Ambient identity of the task generation currently being processed.
 * `TaskQueue.complete` provides it around the handler; nested offers use it as
 * informational creator provenance and, only when explicitly requested, as a
 * result-retention holder.
 *
 * @module
 */
import { Context, Effect, Layer, Option } from "effect";
import type { TaskIdentity } from "./Schemas.js";

export interface TaskContext {
  readonly currentTask?: TaskIdentity;
}
export const TaskContext = Context.Service<TaskContext>(
  "~effectmq/TaskContext",
);

/** Provide a task context. Used by `TaskQueue.complete` around handler runs. */
export const layer = (options: TaskContext = {}) =>
  Layer.succeed(TaskContext, options);

/** The identity of the task generation whose handler is running, if any. */
export const currentTask: Effect.Effect<TaskIdentity | undefined> = Effect.map(
  Effect.serviceOption(TaskContext),
  (context) => (Option.isSome(context) ? context.value.currentTask : undefined),
);
