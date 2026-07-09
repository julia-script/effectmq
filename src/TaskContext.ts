/**
 * Ambient context carried while a {@link TaskQueue} handler runs: the ref of
 * the task currently being processed. `TaskQueue.complete` provides it around
 * the handler, and `TaskQueue.offer` reads it so tasks offered from inside a
 * handler are pinned by (`heldBy`) and attributed to (`createdBy`) the outer
 * task.
 *
 * @module
 */
import { Context, Effect, Layer, Option } from "effect";
import type { TaskRef } from "./Schemas.js";

interface TaskContext {
  readonly parentTask?: TaskRef;
}
export const TaskContext = Context.Service<TaskContext>(
  "~effectmq/TaskContext",
);

/** Provide a task context. Used by `TaskQueue.complete` around handler runs. */
export const layer = (options: TaskContext = {}) =>
  Layer.succeed(TaskContext, options);

/** The ref of the task whose handler is currently running, if any. */
export const parentTask: Effect.Effect<TaskRef | undefined> = Effect.map(
  Effect.serviceOption(TaskContext),
  (context) => (Option.isSome(context) ? context.value.parentTask : undefined),
);
