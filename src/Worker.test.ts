import { Deferred, Effect, Fiber, Schedule, Schema } from "effect";
import { expect, layer } from "@effect/vitest";
import { RedisPool, Task, TaskEngine, TaskQueue, Worker } from "./index.js";
import { TestLayer } from "./testing/redisLayer.js";

const makeQueue = (name: string) =>
  Task.make({
    name,
    payload: { id: Schema.String },
    success: Schema.String,
    error: Schema.Struct({ reason: Schema.String }),
    idempotencyKey: (payload) => payload.id,
  }).pipe(Effect.map((definition) => TaskQueue.make(name, definition)));

const waitUntilRemoved = (
  engine: TaskEngine.TaskEngineService,
  queue: string,
  id: string,
) =>
  engine.getTask(queue, id).pipe(
    Effect.repeat({
      until: (task) => task === null,
      schedule: Schedule.spaced("5 millis"),
    }),
  );

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "Worker (real Redis time)",
  (it) => {
    it.effect("uses isolated worker and maintenance Redis roles", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const base = yield* RedisPool.RedisPool;
        const queue = yield* makeQueue("worker-roles");
        yield* TaskQueue.offer(queue, { id: "one" });

        const calls = { producer: 0, worker: 0, maintenance: 0 };
        const counted = (
          role: keyof typeof calls,
        ): RedisPool.RedisPoolService => ({
          ...base,
          evalScript: (source, options, ...args) => {
            calls[role]++;
            return base.evalScript(source, options, ...args);
          },
        });
        const roles = RedisPool.makeConnectionRoles(
          counted("producer"),
          counted("worker"),
          counted("maintenance"),
        );
        const handled = yield* Deferred.make<void>();
        const worker = Worker.make(
          queue,
          () =>
            Deferred.succeed(handled, undefined).pipe(Effect.as("processed")),
          {
            pollInterval: "5 millis",
            maintenanceInterval: "5 millis",
          },
        );

        const fiber = yield* Worker.run(worker).pipe(
          Effect.provideService(RedisPool.RedisConnectionRoles, roles),
          Effect.forkChild,
        );
        yield* Deferred.await(handled);
        yield* waitUntilRemoved(engine, queue.name, "one");
        yield* Fiber.interrupt(fiber);

        expect(calls.worker).toBeGreaterThan(0);
        expect(calls.maintenance).toBeGreaterThan(0);
        expect(calls.producer).toBe(0);
      }),
    );

    it.effect(
      "shutdown stops acquisition and drains an in-flight handler",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const queue = yield* makeQueue("worker-drain");
          yield* TaskQueue.offer(queue, { id: "drain" });
          const started = yield* Deferred.make<void>();
          const finish = yield* Deferred.make<void>();
          const worker = Worker.make(
            queue,
            () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(finish)),
                Effect.as("drained"),
              ),
            {
              pollInterval: "5 millis",
              drainTimeout: "2 seconds",
            },
          );

          const workerFiber = yield* Worker.run(worker).pipe(Effect.forkChild);
          yield* Deferred.await(started);
          const stopFiber = yield* Fiber.interrupt(workerFiber).pipe(
            Effect.forkChild,
          );

          // The worker is shutting down, but its owned attempt remains alive until
          // the cooperative handler finishes and acknowledges.
          expect(
            (yield* engine.getTask(queue.name, "drain"))?.outcome,
          ).toBeUndefined();
          yield* Deferred.succeed(finish, undefined);
          yield* Fiber.join(stopFiber);
          expect(yield* engine.getTask(queue.name, "drain")).toBeNull();
        }),
    );
  },
);
