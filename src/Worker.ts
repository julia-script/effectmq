/**
 * Long-lived, scoped queue workers with bounded concurrency, supervised
 * heartbeats, isolated Redis roles, maintenance, and graceful draining.
 *
 * @module
 */
import type * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import type * as Schema from "effect/Schema";
import { RedisConnectionRoles } from "./RedisPool.js";
import * as TaskEngine from "./TaskEngine.js";
import * as TaskInvariant from "./TaskInvariant.js";
import * as TaskQueue from "./TaskQueue.js";

const TypeId = "~effectmq/Worker" as const;

/**
 * Configures worker concurrency, polling, maintenance, and graceful shutdown.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface WorkerOptions {
  /** Number of independent acquire/process loops (1-1000). Defaults to `1`. */
  readonly concurrency?: number;
  /** Delay after an empty acquisition. Defaults to one second. */
  readonly pollInterval?: Duration.Input;
  /** Interval between maintenance sweeps. Defaults to one second. */
  readonly maintenanceInterval?: Duration.Input;
  /** Maximum time to let in-flight handlers settle on shutdown. */
  readonly drainTimeout?: Duration.Input;
  /** Lease and heartbeat supervision settings. */
  readonly processing?: TaskQueue.ProcessingOptions;
}

/**
 * A queue, handler, and runtime policy ready to be run as a managed worker.
 *
 * This value is only a description; creating it does not acquire Redis
 * connections or start background fibers.
 *
 * @category Models
 * @since 0.3.0
 */
export interface Worker<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  QueueIdentityR = never,
  HandlerR = never,
> {
  readonly [TypeId]: typeof TypeId;
  readonly queue: TaskQueue.TaskQueue<
    Payload,
    Success,
    Error,
    QueueR,
    QueueIdentityR
  >;
  readonly handler: TaskQueue.TaskHandler<Payload, Success, Error, HandlerR>;
  readonly options: WorkerOptions;
}

/**
 * Describes a worker without starting it.
 *
 * **Example: Build a two-slot worker**
 *
 * ```ts
 * import { Effect, Schema } from "effect"
 * import { Task, TaskQueue, Worker } from "@effectmq/core"
 *
 * const email = Task.make({
 *   name: "email",
 *   payload: { address: Schema.String },
 *   success: Schema.Void,
 *   error: Schema.String
 * })
 * const emails = TaskQueue.make("emails", email)
 * const worker = Worker.make(
 *   emails,
 *   ({ payload }) => Effect.log(`Emailing ${payload.address}`),
 *   { concurrency: 2 }
 * )
 * ```
 *
 * @category Constructors
 * @since 0.3.0
 */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  QueueIdentityR = never,
  HandlerR = never,
>(
  queue: TaskQueue.TaskQueue<Payload, Success, Error, QueueR, QueueIdentityR>,
  handler: TaskQueue.TaskHandler<Payload, Success, Error, HandlerR>,
  options: WorkerOptions = {},
): Worker<Payload, Success, Error, QueueR, QueueIdentityR, HandlerR> => ({
  [TypeId]: TypeId,
  queue,
  handler,
  options,
});

class WorkerSlotStopped extends Data.TaggedError("WorkerSlotStopped") {}

/** Indicates that a worker option cannot produce a bounded runtime. */
export class WorkerConfigurationError extends Data.TaggedError(
  "WorkerConfigurationError",
)<{
  readonly field:
    | keyof Omit<WorkerOptions, "processing">
    | `processing.${keyof TaskQueue.ProcessingOptions}`;
  readonly constraint: string;
  readonly actual: unknown;
}> {}

interface ResolvedWorkerOptions {
  readonly concurrency: number;
  readonly pollInterval: number;
  readonly maintenanceInterval: number;
  readonly drainTimeout: number;
  readonly processing: TaskQueue.ProcessingOptions;
}

const invalidWorkerOption = (
  field: WorkerConfigurationError["field"],
  constraint: string,
  actual: unknown,
) => new WorkerConfigurationError({ field, constraint, actual });

const workerDuration = Effect.fnUntraced(function* (
  field: WorkerConfigurationError["field"],
  input: Duration.Input,
  allowZero: boolean,
  requireWholeMilliseconds = false,
) {
  const value = yield* Effect.try({
    try: () => Duration.toMillis(input),
    catch: () => invalidWorkerOption(field, "a valid finite duration", input),
  });
  if (
    !Number.isFinite(value) ||
    value < 0 ||
    (!allowZero && value === 0) ||
    (requireWholeMilliseconds && !Number.isSafeInteger(value))
  ) {
    return yield* invalidWorkerOption(
      field,
      requireWholeMilliseconds
        ? "a positive safe-integer number of milliseconds"
        : allowZero
          ? "a finite non-negative duration"
          : "a finite duration greater than zero",
      input,
    );
  }
  return value;
});

const resolveWorkerOptions = Effect.fnUntraced(function* (
  options: WorkerOptions,
): Effect.fn.Return<ResolvedWorkerOptions, WorkerConfigurationError> {
  const concurrency = options.concurrency ?? 1;
  if (
    !Number.isSafeInteger(concurrency) ||
    concurrency < 1 ||
    concurrency > 1_000
  ) {
    return yield* invalidWorkerOption(
      "concurrency",
      "a safe integer between 1 and 1000",
      concurrency,
    );
  }
  const pollInterval = yield* workerDuration(
    "pollInterval",
    options.pollInterval ?? Duration.seconds(1),
    false,
  );
  const maintenanceInterval = yield* workerDuration(
    "maintenanceInterval",
    options.maintenanceInterval ?? Duration.seconds(1),
    false,
  );
  const drainTimeout = yield* workerDuration(
    "drainTimeout",
    options.drainTimeout ?? Duration.seconds(30),
    true,
  );
  const processing = options.processing ?? {};
  const lockTimeout = yield* workerDuration(
    "processing.lockTimeout",
    processing.lockTimeout ?? Duration.seconds(30),
    false,
    true,
  );
  const lockRefresh = yield* workerDuration(
    "processing.lockRefresh",
    processing.lockRefresh ?? Duration.seconds(10),
    false,
    true,
  );
  if (lockRefresh >= lockTimeout) {
    return yield* invalidWorkerOption(
      "processing.lockRefresh",
      "a duration shorter than processing.lockTimeout",
      processing.lockRefresh ?? Duration.seconds(10),
    );
  }
  const heartbeatRetryDelay = yield* workerDuration(
    "processing.heartbeatRetryDelay",
    processing.heartbeatRetryDelay ?? Duration.millis(250),
    false,
  );
  const heartbeatRetryCount = processing.heartbeatRetryCount ?? 3;
  if (!Number.isSafeInteger(heartbeatRetryCount) || heartbeatRetryCount < 0) {
    return yield* invalidWorkerOption(
      "processing.heartbeatRetryCount",
      "a non-negative safe integer",
      heartbeatRetryCount,
    );
  }
  return {
    concurrency,
    pollInterval,
    maintenanceInterval,
    drainTimeout,
    processing: {
      lockTimeout,
      lockRefresh,
      heartbeatRetryDelay,
      heartbeatRetryCount,
    },
  };
});

/**
 * Runs a worker until interrupted.
 *
 * Independent acquisition fibers use the worker Redis role while a maintenance
 * fiber uses the maintenance role. Interruption stops new acquisitions, keeps
 * heartbeats alive while handlers drain, then interrupts any remainder after
 * `drainTimeout`. Invalid concurrency, timing, or processing configuration fails with
 * {@link WorkerConfigurationError} before any worker fibers start.
 *
 * **Gotchas**
 *
 * Queue handlers have at-least-once delivery and must make externally visible
 * effects idempotent. Attempt and maintenance failures are logged and the loops
 * continue. After configuration validation, operational failures are observed
 * and the long-running worker does not fail.
 *
 * @category Operations
 * @since 0.3.0
 */
export const run = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR,
  QueueIdentityR,
  HandlerR,
>(
  worker: Worker<Payload, Success, Error, QueueR, QueueIdentityR, HandlerR>,
): Effect.fn.Return<
  never,
  WorkerConfigurationError,
  | RedisConnectionRoles
  | Crypto.Crypto
  | QueueR
  | HandlerR
  | Payload["DecodingServices"]
  | Success["EncodingServices"]
  | Error["EncodingServices"]
> {
  yield* TaskInvariant.validate(worker.queue.task);
  const options = yield* resolveWorkerOptions(worker.options);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const roles = yield* RedisConnectionRoles;
      const workerEngine = yield* TaskEngine.makeWithRedis(roles.worker).pipe(
        Effect.orDie,
      );
      const maintenanceEngine = yield* TaskEngine.makeWithRedis(
        roles.maintenance,
      ).pipe(Effect.orDie);
      const accepting = yield* Ref.make(true);
      const slots = yield* FiberSet.make<void, never>();
      const { concurrency, drainTimeout, maintenanceInterval, pollInterval } =
        options;
      // Registered after FiberSet.make, so this LIFO finalizer drains before
      // the set's own finalizer interrupts any remaining handlers.
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Ref.set(accepting, false);
          const drained = yield* FiberSet.awaitEmpty(slots).pipe(
            Effect.timeoutOption(drainTimeout),
          );
          if (Option.isNone(drained)) yield* FiberSet.clear(slots);
        }),
      );

      const iteration = Effect.gen(function* () {
        if (!(yield* Ref.get(accepting))) return yield* new WorkerSlotStopped();
        const processed = yield* TaskQueue.completeOne(
          worker.queue,
          worker.handler,
          options.processing,
        ).pipe(
          Effect.provideService(TaskEngine.TaskEngine, workerEngine),
          Effect.matchEffect({
            onFailure: (error) =>
              Effect.logError("effectmq worker attempt failed", error).pipe(
                Effect.as(false),
              ),
            onSuccess: Effect.succeed,
          }),
        );
        if (!processed) yield* Effect.sleep(pollInterval);
      });
      const slot = iteration.pipe(
        Effect.forever,
        Effect.catchTag("WorkerSlotStopped", () => Effect.void),
      );
      for (let index = 0; index < concurrency; index++) {
        yield* FiberSet.run(slots, slot);
      }

      yield* maintenanceEngine.maintain(worker.queue.name).pipe(
        Effect.matchEffect({
          onFailure: (error) =>
            Effect.logError("effectmq maintenance sweep failed", error),
          onSuccess: () => Effect.void,
        }),
        Effect.repeat(Schedule.spaced(maintenanceInterval)),
        Effect.forkScoped,
      );

      return yield* Effect.never;
    }),
  );
});
