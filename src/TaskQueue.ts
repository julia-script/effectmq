/**
 * The high-level, typed queue API over {@link TaskEngine}. A `TaskQueue` pairs
 * a queue name with a {@link Task} definition; use {@link offer} to enqueue
 * work and {@link complete} to process a task end-to-end (take, run the handler,
 * and report the outcome, applying the definition's retry policy on failure).
 *
 * @module
 */
import { Fiber, Schedule, Stream } from "effect";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Function from "effect/Function";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import { type CompletionPolicy, decodeTask } from "./Schemas.js";
import type * as Task from "./Task.js";
import * as TaskEngine from "./TaskEngine.js";
import { nextRunAt } from "./utils.js";

const TypeId = "~effectmq/TaskQueue" as const;

/** A named queue bound to a typed {@link Task.TaskDefinition}. */
export interface TaskQueue<
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
> {
  readonly [TypeId]: typeof TypeId;
  readonly name: string;
  readonly task: Task.TaskDefinition<Payload, Success, Error, R>;
}
/** Create a {@link TaskQueue} from a queue `name` and a task definition. */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
>(
  name: string,
  taskDefinition: Task.TaskDefinition<Payload, Success, Error, R>,
): TaskQueue<Payload, Success, Error, R> => {
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
const takeUnsafe = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
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

  const task = yield* engine
    .takeTask(TaskEngine.makePrefix(queue.name), lockTimeout)
    .pipe(
      Effect.repeat({
        until: (task) => task !== null,
        schedule: Schedule.spaced(poolInterval),
      }),
    );

  const decodedTask = yield* decodeTask(queue.task, task);
  return decodedTask;
});

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
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  payload: Payload["Type"],
  options?: TaskOptions,
): Effect.fn.Return<
  Task.Task<Payload, Success, Error>,
  TaskEngine.TaskEngineError | Schema.SchemaError,
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const encodePayload = Schema.encodeEffect(queue.task.payloadSchema);
  const id = queue.task.idempotencyKey(payload);
  const engine = yield* TaskEngine.TaskEngine;

  const task = yield* engine.createTask({
    prefix: queue.name,
    id,
    name: queue.task.name,
    payload: yield* encodePayload(payload),
    delay: options?.delay ?? 0,
    maxRetries: options?.maxRetries ?? -1,
    onSuccessPolicy: options?.onSuccessPolicy ?? "delete",
    onFailurePolicy: options?.onFailurePolicy ?? "delete",
  });

  const res = yield* decodeTask(queue.task, task);
  return res satisfies Task.Task<Payload, Success, Error>;
});

export const extendLock = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
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
  R = never,
>(queue: TaskQueue<Payload, Success, Error, R>, taskId: string) {
  const engine = yield* TaskEngine.TaskEngine;
  return yield* engine.removeLock(queue.name, taskId);
});

const succeed = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  task: Task.Task<Payload, Success, Error>,
  success: Success["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  // encode to the schema's value (not a JSON string); the engine script
  // JSON-encodes it once, mirroring how `fail` hands off the raw error value
  const encode = Schema.encodeEffect(queue.task.successSchema);
  return yield* engine.writeSuccess(
    queue.name,
    task.id,
    yield* encode(success),
  );
});

/** Report a typed failure for a taken task, routing it per the queue's failure policy. */
const fail = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  task: Task.Task<Payload, Success, Error>,
  failure: Error["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;

  // the per-offer maxRetries (-1 when unset) overrides the definition's cap
  const maxRetries =
    task.maxRetries !== -1 ? task.maxRetries : queue.task.maxRetries;

  const retryAt =
    queue.task.retrySchedule && task.errors.length < maxRetries
      ? yield* nextRunAt(
          queue.task.retrySchedule,
          new Date(task.createdAt.getTime() + task.delay),
          [...task.errors, { timestamp: new Date(), error: failure }],
        )
      : undefined;
  return yield* engine.writeError(queue.name, task.id, failure, retryAt);
});

export type TaskHandler<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
> = (
  task: Task.Task<Payload, Success, Error>,
) => Effect.Effect<Success["Type"], Error["Type"], R>;

/**
 * Take the next task and run it to completion: it locks the task, keeps the
 * lock alive with a background heartbeat, runs `handler`, then reports the
 * outcome to the engine.
 * @returns The task id
 */
export const complete: {
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    TR = never,
    R = never,
  >(
    handler: TaskHandler<Payload, Success, Error, R>,
  ): (
    self: TaskQueue<Payload, Success, Error, TR>,
  ) => Effect.Effect<
    string,
    TaskEngine.TaskEngineError | Schema.SchemaError,
    TR | R
  >;
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    TR = never,
    R = never,
  >(
    self: TaskQueue<Payload, Success, Error, TR>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<
    string,
    TaskEngine.TaskEngineError | Schema.SchemaError,
    TR | R
  >;
} = Function.dual(
  2,
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    TR = never,
    R = never,
  >(
    self: TaskQueue<Payload, Success, Error, TR>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<
    string,
    Schema.SchemaError | TaskEngine.TaskEngineError,
    | TaskEngine.TaskEngine
    | TR
    | R
    | Payload["DecodingServices"]
    | Success["EncodingServices"]
    | Error["EncodingServices"]
  > => {
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
      } else {
        yield* fail(self, task, result.failure);
      }
      return task.id;
    });
  },
);

/**
 * Stream this queue's lifecycle events, decoded against the queue's schemas.
 *
 * Wraps the engine's raw event stream and decodes each event's task-shaped
 * payload: `task.created`/`task.updated` yield typed {@link Task.Task}s,
 * `task.failed` yields a typed error, and `task.completed` yields a typed
 * success value. `cursor` resumes from a prior event id (defaults to now, so
 * only future events are delivered).
 */
export const stream = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  {
    cursor = `${Date.now()}-0`,
  }: {
    cursor?: string;
  } = {},
) =>
  Effect.gen(function* () {
    const engine = yield* TaskEngine.TaskEngine;

    return engine.stream(queue.name, { cursor }).pipe(
      Stream.mapEffect((event) =>
        Effect.gen(function* () {
          if (event._tag === "task.created") {
            const task = event.payload.newTask;
            return {
              ...event,
              payload: {
                newTask: yield* decodeTask(queue.task, task),
              },
            };
          }
          if (event._tag === "task.updated") {
            const { existingTask, newTask } = event.payload;
            return {
              ...event,
              payload: {
                existingTask: yield* decodeTask(queue.task, existingTask),
                newTask: yield* decodeTask(queue.task, newTask),
              },
            };
          }
          if (event._tag === "task.failed") {
            const decodeError = Schema.decodeEffect(queue.task.errorSchema);
            return {
              ...event,
              payload: {
                ...event.payload,
                error: yield* decodeError(event.payload.error),
              },
            };
          }
          if (event._tag === "task.completed") {
            const decodeSuccess = Schema.decodeEffect(queue.task.successSchema);
            return {
              ...event,
              payload: {
                ...event.payload,
                success: yield* decodeSuccess(event.payload.success),
              },
            };
          }

          return event;
        }),
      ),
    );
  }).pipe(Stream.unwrap);

/**
 * Await a task's terminal event: resolves with its decoded success value once
 * the task completes, or fails with its decoded error if it fails terminally.
 * Watches the queue's event {@link stream} for the matching `taskId`.
 */
export const wait = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  taskId: string,
): Effect.fn.Return<
  Success["Type"],
  Error["Type"],
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const events = stream(queue);
  const [result] = yield* events.pipe(
    Stream.filter(
      (event) =>
        event.taskId === taskId &&
        (event._tag === "task.completed" || event._tag === "task.failed"),
    ),
    Stream.take(1),
    Stream.runCollect,
  );
  if (result._tag === "task.completed") {
    return result.payload.success;
  } else if (result._tag === "task.failed") {
    return yield* Effect.fail(result.payload.error);
  }
});
/**
 * Offer a task and await its outcome in one call: resolves with the decoded
 * success value or fails with the decoded error. Opens the event stream before
 * offering so a fast handler's terminal event isn't missed.
 */
export const execute = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  payload: Payload["Type"],
  options?: TaskOptions,
): Effect.fn.Return<
  Success["Type"],
  Error["Type"],
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const events = stream(queue);
  const task = yield* offer(queue, payload, options);

  const [result] = yield* events.pipe(
    Stream.filter((event) => {
      return (
        event.taskId === task.id &&
        (event._tag === "task.completed" || event._tag === "task.failed")
      );
    }),
    Stream.take(1),
    Stream.runCollect,
  );
  if (result._tag === "task.completed") {
    return result.payload.success;
  } else if (result._tag === "task.failed") {
    return yield* Effect.fail(result.payload.error);
  }
});
