/**
 * The high-level, typed queue API over `TaskEngine`. A `TaskQueue` pairs
 * a queue name with a {@link Task} definition; use {@link offer} to enqueue
 * work and {@link complete} to process a task end-to-end (take, run the handler,
 * and report the outcome, applying the definition's retry policy on failure).
 *
 * @module
 */
import { Schedule, Stream } from "effect";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Function from "effect/Function";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import {
  type CompletionPolicy,
  decodeTask,
  type EngineTerminalResult,
  TaskErrorSchema,
} from "./Schemas.js";
import * as StorageProtocol from "./StorageProtocol.js";
import type * as Task from "./Task.js";
import * as TaskContext from "./TaskContext.js";
import * as TaskEngine from "./TaskEngine.js";
import { nextRunAt } from "./utils.js";

const TypeId = "~effectmq/TaskQueue" as const;

/**
 * A queue name bound to one typed {@link Task.TaskDefinition}.
 *
 * This is a pure descriptor; it does not allocate Redis state or start a
 * worker. Queue state is created by the first operation that needs it.
 *
 * @category Models
 * @since 0.1.0
 */
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
/**
 * Creates a typed queue descriptor from a stable name and task definition.
 *
 * @category Constructors
 * @since 0.1.0
 */
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
  readonly poll?: boolean;
}

interface TaskAttempt<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly task: Task.Task<Payload, Success, Error>;
  readonly leaseToken: string;
}
/**
 * Take the next available task from the queue, polling every `poolInterval`
 * until one is available, and lock it for `lockTimeout`. The returned task is
 * decoded into its typed form. "Unsafe" because the caller is responsible for
 * the lock lifecycle (extend/release) and for reporting success/failure;
 * prefer {@link complete} for the managed path.
 */
const takeAvailable = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  options?: TakeOptions,
): Effect.fn.Return<
  TaskAttempt<Payload, Success, Error> | null,
  | StorageProtocol.StorageProtocolError
  | TaskEngine.TaskEngineError
  | Schema.SchemaError,
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const engine = yield* TaskEngine.TaskEngine;
  const poolInterval = Duration.toMillis(
    options?.poolInterval ?? Duration.seconds(1),
  );
  const lockTimeout = Duration.toMillis(
    options?.lockTimeout ?? Duration.seconds(30),
  );

  const take = engine.takeTask(TaskEngine.makePrefix(queue.name), lockTimeout);
  const attempt =
    options?.poll === false
      ? yield* take
      : yield* take.pipe(
          Effect.repeat({
            until: (task) => task !== null,
            schedule: Schedule.spaced(poolInterval),
          }),
        );

  if (attempt === null) return null;

  return {
    leaseToken: attempt.leaseToken,
    task: yield* decodeTask(queue.task, attempt.task),
  };
});

const takeUnsafe = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
  R = never,
>(queue: TaskQueue<Payload, Success, Error, R>, options?: TakeOptions) {
  const attempt = yield* takeAvailable(queue, options);
  if (attempt === null) {
    return yield* Effect.die("Polling take unexpectedly returned null");
  }
  return attempt;
});

/**
 * Controls the identity, timing, retry cap, and terminal retention of one offer.
 *
 * `delay` is measured in milliseconds. Completion policies default to
 * `delete`, duplicate offers default to `return-existing`, and
 * `maxStalledCount` defaults to one.
 *
 * @category Configuration
 * @since 0.1.0
 */
export interface TaskOptions {
  /** Explicit task identity override, used by durable scheduler tick tasks. */
  readonly taskId?: string;
  delay?: number;
  maxRetries?: number;
  maxStalledCount?: number;
  onSuccessPolicy?: CompletionPolicy;
  onFailurePolicy?: CompletionPolicy;
  /** Keep this task's terminal result until the currently running task settles. */
  retainResultUntil?: "current-task-settles";
  /** Behavior when the idempotency identity already has a stored generation. */
  onDuplicate?: "return-existing" | "new-generation";
}

/**
 * Redis connection loss made it impossible to determine whether an offer was
 * committed. Retry with the same queue payload/idempotency identity; the
 * default duplicate behavior will return the committed generation unchanged.
 *
 * @category Errors
 * @since 0.3.0
 */
export class IndeterminateWriteError extends Data.TaggedError(
  "IndeterminateWriteError",
)<{
  readonly queue: string;
  readonly taskId: string;
  readonly cause: TaskEngine.TaskEngineError;
}> {}

/**
 * Indicates that current-task result retention was requested outside a handler.
 *
 * @category Errors
 * @since 0.3.0
 */
export class RetentionContextRequired extends Data.TaggedError(
  "RetentionContextRequired",
)<{
  readonly queue: string;
  readonly taskId: string;
}> {}

const causeText = (cause: unknown, depth = 0): string => {
  if (depth >= 4) return String(cause);
  if (typeof cause !== "object" || cause === null || !("cause" in cause)) {
    return String(cause);
  }
  return `${String(cause)} ${causeText(cause.cause, depth + 1)}`;
};

const isIndeterminateConnectionFailure = (
  error: TaskEngine.TaskEngineError,
): boolean =>
  /ECONNRESET|ECONNREFUSED|ETIMEDOUT|EPIPE|socket closed|connection (?:is )?closed|connection lost|read only/i.test(
    causeText(error.cause),
  );

const relationshipLimit = (
  error: TaskEngine.TaskEngineError,
): StorageProtocol.StorageCountLimitExceeded | undefined => {
  const match = causeText(error.cause).match(
    /STORAGE_RELATIONSHIP_LIMIT (holder|retained) (\d+)/,
  );
  if (!match) return undefined;
  const maxCount = Number(match[2]);
  return new StorageProtocol.StorageCountLimitExceeded({
    resource: "relationships",
    scope: match[1] as "holder" | "retained",
    actualCount: maxCount,
    maxCount,
  });
};

declare const TaskHandleSuccess: unique symbol;
declare const TaskHandleError: unique symbol;

/**
 * A durable, schema-aware reference to exactly one offered task generation.
 *
 * Persist the entire handle when a later process will call {@link wait}. Its
 * protocol and schema identities prevent a different decoder from silently
 * interpreting the stored result.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskHandle<Success, Error> {
  readonly _tag: "TaskHandle";
  readonly queue: string;
  readonly taskId: string;
  readonly generation: number;
  readonly cursor: string;
  readonly taskName: string;
  readonly schemaId: string;
  readonly protocolVersion: 1;
  readonly [TaskHandleSuccess]?: (_: Success) => Success;
  readonly [TaskHandleError]?: (_: Error) => Error;
}

/**
 * Carries the typed terminal failure observed by {@link wait}.
 *
 * @category Errors
 * @since 0.3.0
 */
export class TaskFailed<Failure> extends Data.TaggedError("TaskFailed")<{
  readonly handle: TaskHandle<unknown, Failure>;
  readonly failure: Failure;
}> {}

/**
 * Indicates that neither a task record nor a result exists for a handle.
 *
 * @category Errors
 * @since 0.3.0
 */
export class TaskNotFound extends Data.TaggedError("TaskNotFound")<{
  readonly handle: TaskHandle<unknown, unknown>;
}> {}

/**
 * Indicates that a handle's exact generation no longer has a retained result.
 *
 * `latestGeneration` distinguishes expiry or removal from replacement by a
 * newer generation.
 *
 * @category Errors
 * @since 0.3.0
 */
export class ResultExpired extends Data.TaggedError("ResultExpired")<{
  readonly handle: TaskHandle<unknown, unknown>;
  readonly latestGeneration: number;
}> {}

/**
 * Indicates that the caller's wait deadline elapsed without canceling the task.
 *
 * @category Errors
 * @since 0.3.0
 */
export class CallerTimeout extends Data.TaggedError("CallerTimeout")<{
  readonly handle: TaskHandle<unknown, unknown>;
  readonly timeout: Duration.Input;
}> {}

/**
 * The generation-safe result of offering a task.
 *
 * `TaskExisting` means the configured identity already had a stored generation;
 * its state is returned unchanged. `TaskCreated` identifies a new generation.
 *
 * @category Models
 * @since 0.3.0
 */
export type OfferOutcome<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = {
  readonly _tag: "TaskCreated" | "TaskExisting";
  readonly task: Task.Task<Payload, Success, Error>;
  readonly handle: TaskHandle<Success["Type"], Error["Type"]>;
};
/**
 * Enqueues a typed payload and returns its exact generation handle.
 *
 * The payload is encoded via the task's
 * payload schema and the task id is derived from the definition's
 * idempotency key. Honors `delay` and the success/failure policy options.
 *
 * Inside a {@link complete} handler, the new task records immutable creator
 * provenance. Nested offers remain execution-independent by default. Request
 * `retainResultUntil: "current-task-settles"` only when the spawned task's
 * terminal record must remain readable until the current task settles.
 *
 * **Gotchas**
 *
 * If this operation fails with {@link IndeterminateWriteError}, retry the same
 * payload and identity with the default duplicate policy. Creating a new
 * identity could enqueue the work twice.
 *
 * **Example: Offer and retain a handle**
 *
 * ```ts
 * import { Effect, Schema } from "effect"
 * import { Task, TaskEngine, TaskQueue } from "@effectmq/core"
 *
 * const resize = Task.make({
 *   name: "resize-image",
 *   payload: { imageId: Schema.String },
 *   success: Schema.String,
 *   error: Schema.String,
 *   idempotencyKey: ({ imageId }) => imageId
 * })
 * const images = TaskQueue.make("images", resize)
 *
 * const enqueue = TaskQueue.offer(images, { imageId: "img-42" }).pipe(
 *   Effect.map(({ handle }) => handle),
 *   Effect.provide(TaskEngine.layer())
 * )
 * ```
 *
 * @category Operations
 * @since 0.1.0
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
  OfferOutcome<Payload, Success, Error>,
  | IndeterminateWriteError
  | RetentionContextRequired
  | StorageProtocol.StorageProtocolError
  | TaskEngine.TaskEngineError
  | Schema.SchemaError,
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const encodePayload = Schema.encodeEffect(queue.task.payloadSchema);
  const id = options?.taskId ?? queue.task.idempotencyKey(payload);
  const engine = yield* TaskEngine.TaskEngine;
  const currentTask = yield* TaskContext.currentTask;
  if (options?.retainResultUntil && currentTask === undefined) {
    return yield* new RetentionContextRequired({
      queue: queue.name,
      taskId: id,
    });
  }

  const offered = yield* engine
    .offerTask({
      retentionHolder:
        currentTask && options?.retainResultUntil ? currentTask : undefined,
      creator: currentTask,
      prefix: queue.name,
      id,
      name: queue.task.name,
      payload: yield* StorageProtocol.encodeValue(
        queue.task.schemaId,
        "payload",
        yield* encodePayload(payload),
        queue.task.storageLimits,
      ),
      schemaId: queue.task.schemaId,
      delay: options?.delay ?? 0,
      maxRetries: options?.maxRetries ?? -1,
      maxStalledCount: options?.maxStalledCount ?? 1,
      maxErrorEntries: queue.task.storageLimits.maxErrorEntries,
      maxRelationships: queue.task.storageLimits.maxRelationships,
      maxEventEntries: queue.task.storageLimits.maxEventEntries,
      taskRecordRetentionMs: queue.task.retention.taskRecordMs,
      resultRetentionMs: queue.task.retention.resultMs,
      terminalIndexRetentionMs: queue.task.retention.terminalIndexMs,
      deadLetterRetentionMs: queue.task.retention.deadLetterMs,
      eventRetentionMs: queue.task.retention.eventMs,
      onSuccessPolicy: options?.onSuccessPolicy ?? "delete",
      onFailurePolicy: options?.onFailurePolicy ?? "delete",
      onDuplicate: options?.onDuplicate ?? "return-existing",
    })
    .pipe(
      Effect.catchIf(
        () => true,
        (
          cause,
        ): Effect.Effect<
          never,
          | IndeterminateWriteError
          | StorageProtocol.StorageCountLimitExceeded
          | TaskEngine.TaskEngineError
        > => {
          const limit = relationshipLimit(cause);
          if (limit) return Effect.fail(limit);
          return isIndeterminateConnectionFailure(cause)
            ? Effect.fail(
                new IndeterminateWriteError({
                  cause,
                  queue: queue.name,
                  taskId: id,
                }),
              )
            : Effect.fail(cause);
        },
      ),
    );

  const task = yield* decodeTask(queue.task, offered.task);
  return {
    _tag: offered.status === "created" ? "TaskCreated" : "TaskExisting",
    task,
    handle: {
      _tag: "TaskHandle",
      queue: queue.name,
      taskId: task.id,
      generation: task.generation,
      cursor: offered.cursor,
      taskName: queue.task.name,
      schemaId: queue.task.schemaId,
      protocolVersion: StorageProtocol.protocolVersion,
    },
  } satisfies OfferOutcome<Payload, Success, Error>;
});

/**
 * Renews a low-level task attempt using its exact lease token.
 *
 * Managed handlers receive heartbeat supervision automatically. This operation
 * is intended for custom worker integrations that already hold an acquired
 * attempt and are prepared to handle {@link TaskEngine.LeaseLost}.
 *
 * @category Operations
 * @since 0.1.0
 */
export const extendLock = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  attempt: TaskAttempt<Payload, Success, Error>,
  lockTimeout?: Duration.Input,
) {
  const engine = yield* TaskEngine.TaskEngine;

  yield* Effect.logDebug(
    `extending lock for task ${attempt.task.id} with timeout ${lockTimeout}`,
  );
  return yield* engine.extendLock(
    queue.name,
    attempt.task.id,
    attempt.leaseToken,
    Duration.toMillis(lockTimeout ?? Duration.seconds(30)),
  );
});

/**
 * Voluntarily releases a low-level attempt back to runnable work.
 *
 * The exact lease token is required. Releasing does not record a stalled
 * failure; lease expiry recovery does.
 *
 * @category Operations
 * @since 0.1.0
 */
export const release = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  attempt: TaskAttempt<Payload, Success, Error>,
) {
  const engine = yield* TaskEngine.TaskEngine;
  return yield* engine.removeLock(
    queue.name,
    attempt.task.id,
    attempt.leaseToken,
  );
});

const succeed = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  attempt: TaskAttempt<Payload, Success, Error>,
  success: Success["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  // Encode to the schema's wire value before wrapping it in the storage
  // envelope. Lua treats that envelope as an opaque ASCII-safe byte string.
  const encode = Schema.encodeEffect(queue.task.successSchema);
  const encoded = yield* encode(success);
  return yield* engine.writeSuccess(
    queue.name,
    attempt.task.id,
    attempt.leaseToken,
    yield* StorageProtocol.encodeValue(
      queue.task.schemaId,
      "success",
      encoded,
      queue.task.storageLimits,
    ),
  );
});

/** Report a typed failure for a taken task, routing it per the queue's failure policy. */
const fail = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
>(
  queue: TaskQueue<Payload, Success, Error, R>,
  attempt: TaskAttempt<Payload, Success, Error>,
  failure: Error["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  const encodeFailure = Schema.encodeEffect(queue.task.errorSchema);

  // the per-offer maxRetries (-1 when unset) overrides the definition's cap
  const maxRetries =
    attempt.task.maxRetries !== -1
      ? attempt.task.maxRetries
      : queue.task.maxRetries;

  const retryAt =
    queue.task.retrySchedule && attempt.task.handlerFailureCount < maxRetries
      ? yield* nextRunAt(
          queue.task.retrySchedule,
          new Date(attempt.task.createdAt.getTime() + attempt.task.delay),
          [...attempt.task.errors, { timestamp: new Date(), error: failure }],
        )
      : undefined;
  return yield* engine.writeError(
    queue.name,
    attempt.task.id,
    attempt.leaseToken,
    yield* StorageProtocol.encodeValue(
      queue.task.schemaId,
      "failure",
      yield* encodeFailure(failure),
      queue.task.storageLimits,
    ),
    retryAt,
  );
});

/**
 * Processes one acquired task and returns its typed success or failure.
 *
 * Handler failure is persisted through the task's retry and terminal policy;
 * it is not re-emitted as the processing operation's infrastructure failure.
 * Handlers may execute more than once after lease loss and must make external
 * side effects idempotent.
 *
 * @category Models
 * @since 0.1.0
 */
export type TaskHandler<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never,
> = (
  task: Task.Task<Payload, Success, Error>,
) => Effect.Effect<Success["Type"], Error["Type"], R>;

/**
 * Configures acquisition leases and bounded heartbeat recovery.
 *
 * Defaults are a 30-second lease, a 10-second refresh interval, 250 ms between
 * heartbeat retries, and at most three transport retries within the remaining
 * lease safety window.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface ProcessingOptions {
  readonly lockTimeout?: Duration.Input;
  readonly lockRefresh?: Duration.Input;
  readonly heartbeatRetryDelay?: Duration.Input;
  readonly heartbeatRetryCount?: number;
}

const processAttempt = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  TR = never,
  R = never,
>(
  self: TaskQueue<Payload, Success, Error, TR>,
  attempt: TaskAttempt<Payload, Success, Error>,
  handler: TaskHandler<Payload, Success, Error, R>,
  options?: ProcessingOptions,
) {
  const lockTimeout = Duration.toMillis(
    options?.lockTimeout ?? Duration.seconds(30),
  );
  const lockRefresh = Duration.toMillis(
    options?.lockRefresh ?? Duration.seconds(10),
  );
  const retryDelay = Math.max(
    1,
    Duration.toMillis(options?.heartbeatRetryDelay ?? Duration.millis(250)),
  );
  const safetyWindow = Math.max(0, lockTimeout - lockRefresh);
  const heartbeatRetryCount = Math.min(
    Math.max(0, options?.heartbeatRetryCount ?? 3),
    Math.floor(safetyWindow / retryDelay),
  );

  const renew = extendLock(self, attempt, lockTimeout).pipe(
    Effect.retry({
      while: (error) => error._tag === "TaskEngineError",
      times: heartbeatRetryCount,
      schedule: Schedule.spaced(retryDelay),
    }),
  );
  const heartBeat = renew.pipe(
    Effect.repeat(Schedule.spaced(lockRefresh)),
    // The heartbeat has no successful completion: only ownership loss or an
    // exhausted bounded transport retry may win the attempt race.
    Effect.flatMap(() => Effect.never),
  );

  const handlerResult = handler(attempt.task).pipe(
    Effect.provide(
      TaskContext.layer({
        currentTask: {
          queue: self.name,
          id: attempt.task.id,
          generation: attempt.task.generation,
        },
      }),
    ),
    Effect.result,
  );
  const result = yield* Effect.raceFirst(handlerResult, heartBeat);
  if (Result.isSuccess(result)) {
    yield* succeed(self, attempt, result.success);
  } else {
    yield* fail(self, attempt, result.failure);
  }
  return attempt.task.id;
});

/**
 * Takes the next task, supervises its lease, and persists the handler outcome.
 *
 * This operation polls until work is available, then returns the processed task
 * identifier. It is available in data-first and data-last forms. Handler
 * failures are recorded on the task and may schedule a retry; only decoding,
 * storage, Redis, or ownership failures remain in the Effect failure channel.
 *
 * **Gotchas**
 *
 * Use `Worker.run` for production worker loops. A handler can run more
 * than once if its lease expires or ownership is lost.
 *
 * **Example: Process one task**
 *
 * ```ts
 * import { Effect, Schema } from "effect"
 * import { Task, TaskQueue } from "@effectmq/core"
 *
 * const greet = Task.make({
 *   name: "greet",
 *   payload: { name: Schema.String },
 *   success: Schema.String,
 *   error: Schema.String
 * })
 * const greetings = TaskQueue.make("greetings", greet)
 *
 * const processNext = TaskQueue.complete(
 *   greetings,
 *   ({ payload }) => Effect.succeed(`Hello, ${payload.name}!`)
 * )
 * ```
 *
 * @category Operations
 * @since 0.1.0
 */
export const complete: {
  <
    Payload extends Schema.Top,
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
    | StorageProtocol.StorageProtocolError
    | TaskEngine.LeaseLost
    | TaskEngine.TaskEngineError
    | Schema.SchemaError,
    TR | R
  >;
  <
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
    TR = never,
    R = never,
  >(
    self: TaskQueue<Payload, Success, Error, TR>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<
    string,
    | StorageProtocol.StorageProtocolError
    | TaskEngine.LeaseLost
    | TaskEngine.TaskEngineError
    | Schema.SchemaError,
    TR | R
  >;
} = Function.dual(
  2,
  <
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
    TR = never,
    R = never,
  >(
    self: TaskQueue<Payload, Success, Error, TR>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<
    string,
    | StorageProtocol.StorageProtocolError
    | Schema.SchemaError
    | TaskEngine.LeaseLost
    | TaskEngine.TaskEngineError,
    | TaskEngine.TaskEngine
    | TR
    | R
    | Payload["DecodingServices"]
    | Success["EncodingServices"]
    | Error["EncodingServices"]
  > => {
    return Effect.gen(function* () {
      const attempt = yield* takeUnsafe(self);
      return yield* processAttempt(self, attempt, handler);
    });
  },
);

/**
 * Try to acquire and process one currently available task without polling.
 * Returns `false` when the queue is empty. Intended for managed worker loops.
 *
 * @category Operations
 * @since 0.3.0
 */
export const completeOne = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  TR = never,
  R = never,
>(
  self: TaskQueue<Payload, Success, Error, TR>,
  handler: TaskHandler<Payload, Success, Error, R>,
  options?: ProcessingOptions,
) {
  const attempt = yield* takeAvailable(self, {
    poll: false,
    lockTimeout: options?.lockTimeout,
  });
  if (attempt === null) return false;
  yield* processAttempt(self, attempt, handler, options);
  return true;
});

/**
 * Stream this queue's lifecycle events, decoded against the queue's schemas.
 *
 * Wraps the engine's raw event stream and decodes each event's task-shaped
 * payload: `task.created`/`task.updated` yield typed {@link Task.Task}s,
 * `task.failed` yields a typed error, and `task.completed` yields a typed
 * success value. `cursor` resumes from a prior event id (defaults to now, so
 * only future events are delivered).
 *
 * **Gotchas**
 *
 * Event retention is finite. A cursor into a trimmed interval fails with
 * {@link TaskEngine.CursorExpired}; resume from its `earliest` cursor only when
 * skipping the missing events is acceptable.
 *
 * @category Streaming
 * @since 0.2.0
 */
export const stream = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  {
    cursor,
    pollInterval,
  }: {
    cursor?: string;
    pollInterval?: Duration.Duration;
  } = {},
) =>
  Effect.gen(function* () {
    const engine = yield* TaskEngine.TaskEngine;

    return engine.stream(queue.name, { cursor, pollInterval }).pipe(
      Stream.mapEffect((event) =>
        Effect.gen(function* () {
          if (event._tag === "task.created") {
            const task = event.payload.newTask;
            return {
              ...event,
              payload: {
                ...event.payload,
                newTask: yield* decodeTask(queue.task, task),
              },
            };
          }
          if (event._tag === "task.updated") {
            const { existingTask, newTask } = event.payload;
            return {
              ...event,
              payload: {
                ...event.payload,
                existingTask: yield* decodeTask(queue.task, existingTask),
                newTask: yield* decodeTask(queue.task, newTask),
              },
            };
          }
          if (event._tag === "task.failed") {
            const decodeError = Schema.decodeEffect(queue.task.errorSchema);
            const rawFailure = event.payload.error;
            const builtIn =
              typeof rawFailure === "object" &&
              rawFailure !== null &&
              "_tag" in rawFailure &&
              Object.values(StorageProtocol.builtInErrorTags).includes(
                rawFailure._tag as never,
              );
            const failure = builtIn
              ? rawFailure
              : yield* StorageProtocol.decodeValue(
                  rawFailure,
                  queue.task.schemaId,
                  "failure",
                );
            return {
              ...event,
              payload: {
                ...event.payload,
                error: builtIn ? failure : yield* decodeError(failure),
              },
            };
          }
          if (event._tag === "task.completed") {
            const decodeSuccess = Schema.decodeEffect(queue.task.successSchema);
            const success = yield* StorageProtocol.decodeValue(
              event.payload.success,
              queue.task.schemaId,
              "success",
            );
            return {
              ...event,
              payload: {
                ...event.payload,
                success: yield* decodeSuccess(success),
              },
            };
          }

          return event;
        }),
      ),
    );
  }).pipe(Stream.unwrap);

/**
 * Configures a caller-local deadline for {@link wait}.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface WaitOptions {
  readonly timeout?: Duration.Input;
}

/**
 * Awaits the exact task generation named by a durable handle.
 *
 * The operation checks retained state, subscribes to lifecycle events, and
 * checks state again before awaiting an event. This closes the completion race
 * around subscription while preserving a durable fast path for already-settled
 * tasks.
 *
 * A terminal task failure becomes {@link TaskFailed}. Missing records, expired
 * results, caller timeouts, incompatible storage metadata, and trimmed cursors
 * remain distinct typed failures. A caller timeout does not cancel queue work.
 *
 * @category Operations
 * @since 0.2.0
 */
export const wait = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  handle: TaskHandle<Success["Type"], Error["Type"]>,
  options: WaitOptions = {},
) => {
  const operation = Effect.gen(function* () {
    if (handle.protocolVersion !== StorageProtocol.protocolVersion) {
      return yield* new StorageProtocol.UnsupportedProtocolVersion({
        encountered: handle.protocolVersion,
        supported: StorageProtocol.readableProtocolVersions,
      });
    }
    if (handle.schemaId !== queue.task.schemaId) {
      return yield* new StorageProtocol.SchemaIdentityMismatch({
        expected: queue.task.schemaId,
        encountered: handle.schemaId,
      });
    }

    const engine = yield* TaskEngine.TaskEngine;
    const decodeTerminalResult = Effect.fnUntraced(function* (
      result: EngineTerminalResult,
    ) {
      if (
        !StorageProtocol.readableProtocolVersions.includes(
          result.protocolVersion as 1,
        )
      ) {
        return yield* new StorageProtocol.UnsupportedProtocolVersion({
          encountered: result.protocolVersion,
          supported: StorageProtocol.readableProtocolVersions,
        });
      }
      if (result.schemaId !== queue.task.schemaId) {
        return yield* new StorageProtocol.SchemaIdentityMismatch({
          expected: queue.task.schemaId,
          encountered: result.schemaId,
        });
      }
      if (result.generation !== handle.generation) {
        return yield* new StorageProtocol.CorruptStorageValue({
          message: "Terminal result generation does not match its key",
        });
      }
      if (result.outcome === "success") {
        if (result.success === undefined) {
          return yield* new StorageProtocol.CorruptStorageValue({
            message: "Terminal success has no result value",
          });
        }
        const value = yield* StorageProtocol.decodeValue(
          result.success,
          queue.task.schemaId,
          "success",
        );
        return {
          _tag: "Success",
          value: yield* Schema.decodeEffect(queue.task.successSchema)(value),
        } as const;
      }
      if (result.failure === undefined) {
        return yield* new StorageProtocol.CorruptStorageValue({
          message: "Terminal failure has no error value",
        });
      }
      const builtIn =
        typeof result.failure === "object" &&
        result.failure !== null &&
        "_tag" in result.failure &&
        Object.values(StorageProtocol.builtInErrorTags).includes(
          result.failure._tag as never,
        );
      const failure = builtIn
        ? result.failure
        : yield* StorageProtocol.decodeValue(
            result.failure,
            queue.task.schemaId,
            "failure",
          );
      const decoded = yield* Schema.decodeEffect(
        Schema.Union([TaskErrorSchema, queue.task.errorSchema]),
      )(failure);
      return yield* new TaskFailed({ handle, failure: decoded });
    });

    const readDurable = Effect.gen(function* () {
      const stored = yield* engine.getTask(handle.queue, handle.taskId);
      if (stored === null) {
        const result = yield* engine.getResult(
          handle.queue,
          handle.taskId,
          handle.generation,
        );
        if (result !== null) return yield* decodeTerminalResult(result);
        const latestGeneration = yield* engine.getGeneration(
          handle.queue,
          handle.taskId,
        );
        return latestGeneration === 0
          ? yield* new TaskNotFound({ handle })
          : yield* new ResultExpired({ handle, latestGeneration });
      }
      if (stored.generation !== handle.generation) {
        const result = yield* engine.getResult(
          handle.queue,
          handle.taskId,
          handle.generation,
        );
        if (result !== null) return yield* decodeTerminalResult(result);
        return yield* new ResultExpired({
          handle,
          latestGeneration: stored.generation,
        });
      }
      if (stored.outcome === undefined) return { _tag: "Pending" } as const;

      const task = yield* decodeTask(queue.task, stored);
      if (stored.outcome === "success") {
        return { _tag: "Success", value: task.success } as const;
      }
      const lastFailure = task.errors.at(-1)?.error;
      if (lastFailure === undefined) {
        return yield* new StorageProtocol.CorruptStorageValue({
          message: "Terminal failure has no error entry",
        });
      }
      return yield* new TaskFailed({ handle, failure: lastFailure });
    });

    const initial = yield* readDurable;
    if (initial._tag === "Success") return initial.value as Success["Type"];

    const eventFiber = yield* stream(queue, {
      cursor: handle.cursor,
      pollInterval: Duration.millis(100),
    }).pipe(
      Stream.filter(
        (event) =>
          event.taskId === handle.taskId &&
          event.generation === handle.generation &&
          (event._tag === "task.completed" || event._tag === "task.failed"),
      ),
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );

    // Let XREAD begin, then re-read durable state. If settlement raced the
    // subscription, either this read or the stream necessarily observes it.
    yield* Effect.yieldNow;
    const rechecked = yield* readDurable;
    if (rechecked._tag === "Success") {
      yield* Fiber.interrupt(eventFiber);
      return rechecked.value as Success["Type"];
    }

    const [event] = yield* Fiber.join(eventFiber);
    if (event._tag === "task.completed") return event.payload.success;
    if (event._tag === "task.failed") {
      return yield* new TaskFailed({ handle, failure: event.payload.error });
    }
    return yield* new StorageProtocol.CorruptStorageValue({
      message: "Wait stream ended without a terminal event",
    });
  });

  const timeout = options.timeout;
  return timeout === undefined
    ? operation
    : operation.pipe(
        Effect.timeoutOrElse({
          duration: timeout,
          orElse: () =>
            Effect.fail(
              new CallerTimeout({
                handle,
                timeout,
              }),
            ),
        }),
      );
};
/**
 * Offer a task and await its outcome through the same generation-safe handle
 * protocol as {@link wait}.
 *
 * **Gotchas**
 *
 * This convenience operation has no caller-timeout option. Use {@link offer}
 * followed by {@link wait} when the waiting fiber needs its own deadline or the
 * handle must be persisted elsewhere.
 *
 * @category Operations
 * @since 0.2.0
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
  const offered = yield* offer(queue, payload, options);
  return yield* wait(queue, offered.handle);
});
