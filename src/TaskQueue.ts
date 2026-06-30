/**
 * The high-level, typed queue API over {@link TaskEngine}. A `TaskQueue` pairs
 * a queue name with a {@link Task} definition; use {@link offer} to enqueue
 * work, {@link complete} to process a task end-to-end, or the lower-level
 * {@link takeUnsafe}/{@link succeed}/{@link fail} primitives directly.
 *
 * @module
 */
import { Fiber, Schedule } from "effect";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Function from "effect/Function";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import { type CompletionPolicy, makeTaskSchema } from "./Schemas.js";
import type * as Task from "./Task.js";
import * as TaskEngine from "./TaskEngine.js";

const TypeId = "~effectmq/TaskQueue" as const;

/** A named queue bound to a typed {@link Task.TaskDefinition}. */
export interface TaskQueue<
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
> {
  readonly [TypeId]: typeof TypeId;
  readonly name: string;
  readonly task: Task.TaskDefinition<Payload, Success, Error>;
}
/** Create a {@link TaskQueue} from a queue `name` and a task definition. */
export const make = <
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  name: string,
  taskDefinition: Task.TaskDefinition<Payload, Success, Error>,
): TaskQueue<Payload, Success, Error> => {
  return {
    [TypeId]: TypeId,
    name,
    task: taskDefinition,
  };
};

interface TakeOptions {
  readonly lockTimeout?: Duration.Input;
  readonly poolInterval?: Duration.Input;
}
/**
 * Take the next available task from the queue, polling every `poolInterval`
 * until one is available, and lock it for `lockTimeout`. The returned task is
 * decoded into its typed form. "Unsafe" because the caller is responsible for
 * the lock lifecycle (extend/release) and for reporting success/failure;
 * prefer {@link complete} for the managed path.
 */
export const takeUnsafe = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  queue: TaskQueue<Payload, Success, Error>,
  options?: TakeOptions,
): Effect.fn.Return<
  Task.Task<Payload, Success, Error>,
  TaskEngine.TaskEngineError | Schema.SchemaError,
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const engine = yield* TaskEngine.TaskEngine;
  const poolInterval = Duration.toMillis(
    options?.poolInterval ?? Duration.seconds(1),
  );
  const lockTimeout = Duration.toMillis(
    options?.lockTimeout ?? Duration.seconds(30),
  );

  while (true) {
    const task = yield* engine.takeTask(
      TaskEngine.makePrefix(queue.name),
      lockTimeout,
    );

    if (task) {
      const taskSchema = makeTaskSchema({
        payload: queue.task.payloadSchema,
        success: queue.task.successSchema,
        error: queue.task.errorSchema,
      });
      const decode = Schema.decodeEffect(taskSchema);
      const decodedTask = yield* decode(task);
      return decodedTask as Task.Task<Payload, Success, Error>;
    }
    yield* Effect.sleep(poolInterval);
  }
});

const encodeToString = <A extends Schema.Top>(schema: A) =>
  Schema.encodeEffect(Schema.fromJsonString(schema));
export interface TaskOptions {
  delay?: number;
  maxRetries?: number;
  onSuccessPolicy?: CompletionPolicy;
  onFailurePolicy?: CompletionPolicy;
}
/**
 * Enqueue `payload` onto the queue. The payload is encoded via the task's
 * payload schema and the task id is derived from the definition's
 * idempotency key. Honors `delay` and the success/failure policy options.
 */
export const offer = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  payload: Payload["Type"],
  options?: TaskOptions,
) {
  const encodePayload = encodeToString(queue.task.payloadSchema);
  const id = queue.task.idempotencyKey(payload);
  const engine = yield* TaskEngine.TaskEngine;

  const task = yield* engine.createTask({
    prefix: queue.name,
    id,
    name: queue.task.name,
    payload: yield* encodePayload(payload),
    delay: options?.delay ?? 0,
    maxRetries: options?.maxRetries ?? 0,
    onSuccessPolicy: options?.onSuccessPolicy ?? "delete",
    onFailurePolicy: options?.onFailurePolicy ?? "delete",
  });
  return task;
});

export const extendLock = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  lockTimeout?: Duration.Input,
) {
  const engine = yield* TaskEngine.TaskEngine;

  yield* Effect.log(
    `extending lock for task ${task.id} with timeout ${lockTimeout}`,
  );
  return yield* engine.extendLock(
    queue.name,
    task.id,
    Duration.toMillis(lockTimeout ?? Duration.seconds(30)),
  );
});

export const release = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(queue: TaskQueue<Payload, Success, Error>, taskId: string) {
  const engine = yield* TaskEngine.TaskEngine;
  return yield* engine.removeLock(queue.name, taskId);
});

const succeed = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  success: Success["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  const encode = encodeToString(queue.task.successSchema);
  return yield* engine.writeSuccess(
    queue.name,
    task.id,
    yield* encode(success),
  );
});

/** Report a typed failure for a taken task, routing it per the queue's failure policy. */
export const fail = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  failure: Error["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  const encode = encodeToString(queue.task.errorSchema);
  return yield* engine.writeError(queue.name, task.id, yield* encode(failure));
});

export type TaskHandler<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R,
> = (
  task: Task.Task<Payload, Success, Error>,
) => Effect.Effect<Success["Type"], Error["Type"], R>;

/**
 * Take the next task and run it to completion: it locks the task, keeps the
 * lock alive with a background heartbeat, runs `handler`, then reports the
 * outcome to the engine. Resolves `true` when the handler succeeds and `false`
 * when it fails (the failure is routed per the queue's failure policy).
 * Dual-signature: `complete(queue, handler)` or `complete(handler)(queue)`.
 */
export const complete: {
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    handler: TaskHandler<Payload, Success, Error, R>,
  ): (
    self: TaskQueue<Payload, Success, Error>,
  ) => Effect.Effect<boolean, never, R>;
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    self: TaskQueue<Payload, Success, Error>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<boolean, never, R>;
} = Function.dual(
  2,
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    self: TaskQueue<Payload, Success, Error>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ) => {
    return Effect.gen(function* () {
      const task = yield* takeUnsafe(self);
      const lockTimeout = Duration.seconds(30);
      const lockRefresh = Duration.seconds(10);
      const policy = Schedule.spaced(lockRefresh);
      const heartBeat = yield* extendLock(self, task, lockTimeout)
        .pipe(Effect.repeat(policy))
        .pipe(Effect.forkChild);

      const result = yield* handler(task).pipe(Effect.result);
      yield* Fiber.interrupt(heartBeat);
      if (Result.isSuccess(result)) {
        yield* succeed(self, task, result.success);
        return true;
      } else {
        yield* fail(self, task, result.failure);
        return false;
      }
    });
  },
);
