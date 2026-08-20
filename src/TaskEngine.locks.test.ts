import { Effect, Schedule, Schema } from "effect";
import { expect, layer } from "@effect/vitest";
import { RedisPool, Task, TaskEngine, TaskQueue } from "./index.js";
import { getLists, TestLayer } from "./testing/redisLayer.js";
import {
  extendLock,
  removeLock,
  takeTask,
} from "./testing/TaskAttemptHarness.js";

const requireAttempt = (attempt: TaskEngine.TaskAttempt | null) => {
  expect(attempt).not.toBeNull();
  if (attempt === null) throw new Error("Expected an acquired task attempt");
  return attempt;
};

const createTask = (
  engine: TaskEngine.TaskEngineService,
  prefix: string,
  id: string,
) =>
  engine.createTask({
    id,
    name: "t",
    payload: "p",
    delay: 0,
    maxRetries: 0,
    onSuccessPolicy: "delete",
    onFailurePolicy: "mark-as-failure",
    prefix,
  });

const waitForLockExpiry = (
  redis: RedisPool.RedisPoolService,
  lockKey: string,
) =>
  redis.send<number>("PTTL", lockKey).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("10 millis"),
      until: (ttl) => ttl <= 0,
    }),
    Effect.timeoutOrElse({
      duration: "2 seconds",
      orElse: () => Effect.die(`Timed out waiting for Redis lock ${lockKey}`),
    }),
  );

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "TaskEngine locking (real Redis time)",
  (it) => {
    it.effect(
      "one maintenance pass recovers at most the configured lease batch",
      () =>
        Effect.gen(function* () {
          const redis = yield* RedisPool.RedisPool;
          const engine = yield* TaskEngine.make({
            debugMode: true,
            maintenanceBatchSize: 2,
          });
          const prefix = "bounded-expired-leases";
          yield* TaskEngine.setMockTime(2_000_000);
          for (const id of ["one", "two", "three"]) {
            yield* engine.createTask({
              prefix,
              id,
              name: "bounded lease",
              payload: null,
              delay: 0,
              maxRetries: 0,
              maxStalledCount: 1,
              onSuccessPolicy: "keep",
              onFailurePolicy: "keep",
            });
            yield* engine.takeTask(prefix, 100);
            yield* redis.send("DEL", `~effectmq:v1:${prefix}:lock:${id}`);
          }
          yield* TaskEngine.stepMockTime(101);
          yield* engine.maintain(prefix);

          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:active`),
          ).toBe(1);
          expect(yield* redis.send("LLEN", `~effectmq:v1:${prefix}:wait`)).toBe(
            2,
          );
        }),
    );

    it.effect("takeTask returns null on an empty queue", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const taken = yield* takeTask(engine, "locks-empty", 30_000);
        expect(taken).toBeNull();
      }),
    );

    it.effect("writeSuccess on a task that does not exist fails", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const error = yield* engine
          .writeSuccess("locks-missing", "ghost", "lease/missing", "ok")
          .pipe(Effect.flip);
        expect(error._tag).toBe("TaskEngineError");
      }),
    );

    it.effect("a stale token cannot complete, fail, or extend an attempt", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        yield* TaskEngine.setMockTime(1000000000000);
        const prefix = "locks-foreign";
        yield* createTask(engine, prefix, "f1");
        const attempt = requireAttempt(yield* engine.takeTask(prefix, 30_000));

        const successError = yield* engine
          .writeSuccess(prefix, "f1", "lease/stale", "ok")
          .pipe(Effect.flip);
        expect(successError._tag).toBe("LeaseLost");

        const failError = yield* engine
          .writeError(prefix, "f1", "lease/stale", { reason: "nope" })
          .pipe(Effect.flip);
        expect(failError._tag).toBe("LeaseLost");

        const extendError = yield* engine
          .extendLock(prefix, "f1", "lease/stale", 60_000)
          .pipe(Effect.flip);
        expect(extendError._tag).toBe("LeaseLost");

        // the actual holder can still complete it
        yield* engine.writeSuccess(prefix, "f1", attempt.leaseToken, "ok");
      }),
    );

    it.effect("extendLock by the holder refreshes the lock TTL", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const redis = yield* RedisPool.RedisPool;
        yield* TaskEngine.setMockTime(1000000000000);
        const prefix = "locks-extend";
        yield* createTask(engine, prefix, "e1");
        yield* takeTask(engine, prefix, 30_000);

        const lockKey = `~effectmq:v1:${prefix}:lock:e1`;
        const initialTtl = yield* redis.send<number>("PTTL", lockKey);
        expect(initialTtl).toBeGreaterThan(0);
        expect(initialTtl).toBeLessThanOrEqual(30_000);

        yield* extendLock(engine, prefix, "e1", 120_000);
        const extendedTtl = yield* redis.send<number>("PTTL", lockKey);
        expect(extendedTtl).toBeGreaterThan(30_000);
      }),
    );

    it.effect(
      "an expired lock stalls the task back to wait with a typed Stalled error",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          yield* TaskEngine.setMockTime(1000000000000);
          const prefix = "locks-stall";
          yield* createTask(engine, prefix, "s1");
          // a 250ms lock, then wait for it to expire for real (lock TTLs are
          // real-time Redis key expiry, not mock time)
          yield* takeTask(engine, prefix, 250);
          expect((yield* getLists(prefix)).active).toEqual(["s1"]);

          yield* waitForLockExpiry(redis, `~effectmq:v1:${prefix}:lock:s1`);
          yield* TaskEngine.stepMockTime(400);

          // any engine call runs syncLocks; the unlocked active task is stalled
          // and requeued for an immediate retry (stalls bypass the failure policy)
          const lists = yield* getLists(prefix);
          expect(lists.wait).toEqual(["s1"]);
          expect(lists.active).toEqual([]);
          expect(lists.failed).toEqual([]);

          const task = yield* engine.getTask(prefix, "s1");
          expect(task?.errors).toHaveLength(1);
          expect((task?.errors[0].error as { _tag: string })._tag).toBe(
            "~effectmq/Error/Stalled",
          );
        }),
    );

    it.effect(
      "a stalled task can be re-taken and decoded by a typed queue",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const def = yield* Task.make({
            name: "locks-recover",
            payload: { userId: Schema.String },
            success: Schema.String,
            error: Schema.Struct({ reason: Schema.String }),
            idempotencyKey: (p) => p.userId,
          });
          const queue = TaskQueue.make("locks-recover", def);
          yield* TaskQueue.offer(queue, { userId: "r1" });

          // simulate a worker that took the task and died: lock expires
          yield* takeTask(engine, queue.name, 250);
          yield* waitForLockExpiry(redis, `~effectmq:v1:${queue.name}:lock:r1`);
          yield* TaskEngine.stepMockTime(400);

          // the next complete() must decode the task, stalled-error entry included
          let seenErrors = 0;
          const done = yield* TaskQueue.complete(queue, (task) => {
            seenErrors = task.errors.length;
            return Effect.succeed("recovered");
          });
          expect(done).toBe("r1");
          expect(seenErrors).toBe(1);
        }),
    );

    it.effect("removeLock voluntarily requeues without recording a stall", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        yield* TaskEngine.setMockTime(1000000000000);
        const prefix = "locks-release";
        yield* createTask(engine, prefix, "u1");
        yield* takeTask(engine, prefix, 30_000);
        yield* removeLock(engine, prefix, "u1");

        const lists = yield* getLists(prefix);
        expect(lists.wait).toEqual(["u1"]);
        expect(lists.active).toEqual([]);
        const task = yield* engine.getTask(prefix, "u1");
        expect(task?.stalledAttemptCount).toBe(0);
        expect(task?.errors).toEqual([]);
      }),
    );

    it.effect("a previous attempt token cannot mutate a re-acquired task", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const redis = yield* RedisPool.RedisPool;
        yield* TaskEngine.setMockTime(1000000000000);
        const prefix = "locks-fenced-reacquire";
        yield* createTask(engine, prefix, "fenced");

        const first = requireAttempt(yield* engine.takeTask(prefix, 100));
        expect(first.task.attempt).toBe(1);

        // Model a crashed process: its lock disappears and maintenance observes
        // the server-time deadline before another worker acquires the retry.
        yield* redis.send("DEL", `~effectmq:v1:${prefix}:lock:fenced`);
        yield* TaskEngine.stepMockTime(101);
        expect((yield* getLists(prefix)).wait).toEqual(["fenced"]);

        const second = requireAttempt(yield* engine.takeTask(prefix, 30_000));
        expect(second.task.attempt).toBe(2);
        expect(second.leaseToken).not.toBe(first.leaseToken);

        const staleEffects = [
          engine.writeSuccess(prefix, "fenced", first.leaseToken, "late"),
          engine.writeError(prefix, "fenced", first.leaseToken, {
            reason: "late",
          }),
          engine.extendLock(prefix, "fenced", first.leaseToken, 30_000),
          engine.removeLock(prefix, "fenced", first.leaseToken),
        ];
        for (const effect of staleEffects) {
          const error = yield* effect.pipe(Effect.flip);
          expect(error._tag).toBe("LeaseLost");
        }

        expect(yield* getLists(prefix)).toMatchObject({
          active: ["fenced"],
          wait: [],
        });
        expect((yield* engine.getTask(prefix, "fenced"))?.errors).toHaveLength(
          1,
        );
        yield* engine.writeSuccess(
          prefix,
          "fenced",
          second.leaseToken,
          "current",
        );
      }),
    );

    it.effect(
      "attempt, handler failure, and stalled counts are independent and stalls terminate",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const now = 1000000000000;
          yield* TaskEngine.setMockTime(now);
          const prefix = "locks-counts";
          yield* engine.createTask({
            id: "counts",
            name: "counts",
            payload: null,
            delay: 0,
            maxRetries: 5,
            maxStalledCount: 1,
            onSuccessPolicy: "keep",
            onFailurePolicy: "mark-as-failure",
            prefix,
          });

          const first = requireAttempt(yield* engine.takeTask(prefix, 100));
          yield* engine.writeError(
            prefix,
            "counts",
            first.leaseToken,
            { reason: "handler" },
            now,
          );
          const second = yield* engine.takeTask(prefix, 100);
          expect(second?.task).toMatchObject({
            attempt: 2,
            handlerFailureCount: 1,
            stalledAttemptCount: 0,
          });

          yield* redis.send("DEL", `~effectmq:v1:${prefix}:lock:counts`);
          yield* TaskEngine.stepMockTime(101);
          yield* getLists(prefix);
          const third = yield* engine.takeTask(prefix, 100);
          expect(third?.task).toMatchObject({
            attempt: 3,
            handlerFailureCount: 1,
            stalledAttemptCount: 1,
          });

          yield* redis.send("DEL", `~effectmq:v1:${prefix}:lock:counts`);
          yield* TaskEngine.stepMockTime(101);
          expect(yield* getLists(prefix)).toMatchObject({
            active: [],
            failed: ["counts"],
            wait: [],
          });
          const terminal = yield* engine.getTask(prefix, "counts");
          expect(terminal).toMatchObject({
            attempt: 3,
            handlerFailureCount: 1,
            stalledAttemptCount: 2,
            outcome: "failure",
          });
          expect(terminal?.errors).toHaveLength(3);
        }),
    );

    it.effect("error history retains only the configured newest entries", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const prefix = "locks-error-limit";
        yield* engine.createTask({
          id: "limited",
          name: "limited",
          payload: null,
          delay: 0,
          maxRetries: 5,
          maxErrorEntries: 2,
          onSuccessPolicy: "keep",
          onFailurePolicy: "keep",
          prefix,
        });

        for (const sequence of [1, 2, 3]) {
          const attempt = requireAttempt(
            yield* engine.takeTask(prefix, 30_000),
          );
          yield* engine.writeError(
            prefix,
            "limited",
            attempt.leaseToken,
            { sequence },
            1,
          );
        }

        const task = yield* engine.getTask(prefix, "limited");
        expect(task?.errors.map((entry) => entry.error)).toEqual([
          { sequence: 2 },
          { sequence: 3 },
        ]);
      }),
    );

    it.effect("non-debug lease deadlines come from Redis server time", () =>
      Effect.gen(function* () {
        const redis = yield* RedisPool.RedisPool;
        const engine = yield* TaskEngine.make({ debugMode: false });
        const prefix = "locks-server-time";
        yield* createTask(engine, prefix, "clock");

        const redisTime = yield* redis.send<[string, string]>("TIME");
        const before =
          Number(redisTime[0]) * 1000 + Number(redisTime[1]) / 1000;
        yield* engine.takeTask(prefix, 30_000);
        const score = Number(
          yield* redis.send<string>(
            "ZSCORE",
            `~effectmq:v1:${prefix}:active`,
            "clock",
          ),
        );

        expect(score).toBeGreaterThanOrEqual(before + 29_000);
        expect(score).toBeLessThanOrEqual(before + 31_000);
      }),
    );
  },
);
