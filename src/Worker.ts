/**
 * Long-lived, scoped queue workers with bounded concurrency, supervised
 * heartbeats, isolated Redis roles, maintenance, and graceful draining.
 *
 * @module
 */
import {
  Data,
  Duration,
  Effect,
  FiberSet,
  Option,
  Ref,
  Schedule,
  type Schema,
} from "effect";
import { RedisConnectionRoles } from "./RedisPool.js";
import * as TaskEngine from "./TaskEngine.js";
import * as TaskQueue from "./TaskQueue.js";

const TypeId = "~effectmq/Worker" as const;

export interface WorkerOptions {
  /** Number of independent acquire/process loops. Defaults to `1`. */
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

export interface Worker<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  HandlerR = never,
> {
  readonly [TypeId]: typeof TypeId;
  readonly queue: TaskQueue.TaskQueue<Payload, Success, Error, QueueR>;
  readonly handler: TaskQueue.TaskHandler<Payload, Success, Error, HandlerR>;
  readonly options: WorkerOptions;
}

/** Describe a worker. Use {@link run} inside an application scope. */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  HandlerR = never,
>(
  queue: TaskQueue.TaskQueue<Payload, Success, Error, QueueR>,
  handler: TaskQueue.TaskHandler<Payload, Success, Error, HandlerR>,
  options: WorkerOptions = {},
): Worker<Payload, Success, Error, QueueR, HandlerR> => ({
  [TypeId]: TypeId,
  queue,
  handler,
  options,
});

class WorkerSlotStopped extends Data.TaggedError("WorkerSlotStopped") {}

/**
 * Run until interrupted. Interruption stops new acquisitions, waits up to
 * `drainTimeout` for active handlers (whose heartbeats keep running), then
 * interrupts any remainder and releases the role-specific Redis resources.
 */
export const run = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR,
  HandlerR,
>(
  worker: Worker<Payload, Success, Error, QueueR, HandlerR>,
): Effect.Effect<
  never,
  never,
  | RedisConnectionRoles
  | QueueR
  | HandlerR
  | Payload["DecodingServices"]
  | Success["EncodingServices"]
  | Error["EncodingServices"]
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const roles = yield* RedisConnectionRoles;
      const workerEngine = yield* TaskEngine.makeWithRedis(roles.worker);
      const maintenanceEngine = yield* TaskEngine.makeWithRedis(
        roles.maintenance,
      );
      const accepting = yield* Ref.make(true);
      const slots = yield* FiberSet.make<void, never>();
      const pollInterval = worker.options.pollInterval ?? Duration.seconds(1);
      const maintenanceInterval =
        worker.options.maintenanceInterval ?? Duration.seconds(1);
      const drainTimeout = worker.options.drainTimeout ?? Duration.seconds(30);
      const concurrency = Math.max(
        1,
        Math.floor(worker.options.concurrency ?? 1),
      );

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
          worker.options.processing,
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
