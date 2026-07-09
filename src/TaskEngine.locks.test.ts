import { Effect, Schema } from "effect";
import { describe, expect, test } from "vitest";
import { RedisPool, Task, TaskEngine, TaskQueue } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

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

describe("TaskEngine locking", () => {
  test("takeTask returns null on an empty queue", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const taken = yield* engine.takeTask("locks-empty", 30_000);
      expect(taken).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("writeSuccess on a task that does not exist fails", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const error = yield* engine
        .writeSuccess("locks-missing", "ghost", "ok")
        .pipe(Effect.flip);
      expect(error._tag).toBe("TaskEngineError");
    }).pipe(TestRuntime.runPromise));

  test("another worker cannot complete, fail, or extend a task it does not hold", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const other = yield* TaskEngine.make({
        debugMode: true,
        workerId: "worker/other",
      });
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "locks-foreign";
      yield* createTask(engine, prefix, "f1");
      yield* engine.takeTask(prefix, 30_000);

      const successError = yield* other
        .writeSuccess(prefix, "f1", "ok")
        .pipe(Effect.flip);
      expect(successError._tag).toBe("TaskEngineError");

      const failError = yield* other
        .writeError(prefix, "f1", { reason: "nope" })
        .pipe(Effect.flip);
      expect(failError._tag).toBe("TaskEngineError");

      const extendError = yield* other
        .extendLock(prefix, "f1", 60_000)
        .pipe(Effect.flip);
      expect(extendError._tag).toBe("TaskEngineError");

      // the actual holder can still complete it
      yield* engine.writeSuccess(prefix, "f1", "ok");
    }).pipe(TestRuntime.runPromise));

  test("extendLock by the holder refreshes the lock TTL", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "locks-extend";
      yield* createTask(engine, prefix, "e1");
      yield* engine.takeTask(prefix, 30_000);

      const lockKey = `~effectmq:${prefix}:lock:e1`;
      const initialTtl = yield* redis.send<number>("PTTL", lockKey);
      expect(initialTtl).toBeGreaterThan(0);
      expect(initialTtl).toBeLessThanOrEqual(30_000);

      yield* engine.extendLock(prefix, "e1", 120_000);
      const extendedTtl = yield* redis.send<number>("PTTL", lockKey);
      expect(extendedTtl).toBeGreaterThan(30_000);
    }).pipe(TestRuntime.runPromise));

  test("an expired lock stalls the task back to wait with a typed Stalled error", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "locks-stall";
      yield* createTask(engine, prefix, "s1");
      // a 250ms lock, then wait for it to expire for real (lock TTLs are
      // real-time Redis key expiry, not mock time)
      yield* engine.takeTask(prefix, 250);
      expect((yield* getLists(prefix)).active).toEqual(["s1"]);

      yield* Effect.sleep("400 millis");

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
    }).pipe(TestRuntime.runPromise));

  test("a stalled task can be re-taken and decoded by a typed queue", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const def = Task.make({
        name: "locks-recover",
        payload: { userId: Schema.String },
        success: Schema.String,
        error: Schema.Struct({ reason: Schema.String }),
        idempotencyKey: (p) => p.userId,
      });
      const queue = TaskQueue.make("locks-recover", def);
      yield* TaskQueue.offer(queue, { userId: "r1" });

      // simulate a worker that took the task and died: lock expires
      yield* engine.takeTask(queue.name, 250);
      yield* Effect.sleep("400 millis");

      // the next complete() must decode the task, stalled-error entry included
      let seenErrors = 0;
      const done = yield* TaskQueue.complete(queue, (task) => {
        seenErrors = task.errors.length;
        return Effect.succeed("recovered");
      });
      expect(done).toBe("r1");
      expect(seenErrors).toBe(1);
    }).pipe(TestRuntime.runPromise));

  test("removeLock releases the lock and the task is stalled on the next sync", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "locks-release";
      yield* createTask(engine, prefix, "u1");
      yield* engine.takeTask(prefix, 30_000);
      yield* engine.removeLock(prefix, "u1");

      // the task is still on the active list without a lock, so the next
      // sync treats it like a crashed worker's task and requeues it
      const lists = yield* getLists(prefix);
      expect(lists.wait).toEqual(["u1"]);
      expect(lists.active).toEqual([]);
    }).pipe(TestRuntime.runPromise));
});
