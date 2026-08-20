import { Effect, Fiber, Schedule, Schema, Stream } from "effect";
import { Packr } from "msgpackr";
import { describe, expect, test } from "vitest";
import { RedisPool, Task, TaskEngine, TaskQueue } from "./index.js";
import { TestRuntime } from "./testing/redisLayer.js";

// A typed queue with a deterministic id (idempotencyKey) so we can address a
// specific task's events by id.
const makeQueue = (name: string, retention?: Partial<Task.RetentionPolicy>) => {
  const def = Task.make({
    name,
    payload: { userId: Schema.String, amount: Schema.Number },
    success: Schema.String,
    error: Schema.Struct({ reason: Schema.String }),
    retention,
    idempotencyKey: (p) => p.userId,
  });
  return TaskQueue.make(name, def);
};

// Read events from the very start of the queue's stream until an event with one
// of `stopTags` is seen, returning everything collected up to and including it.
const collectUntil = (queue: ReturnType<typeof makeQueue>, stopTag: string) =>
  TaskQueue.stream(queue, { cursor: "0" }).pipe(
    Stream.takeUntil((e) => e._tag === stopTag),
    Stream.runCollect,
  );

describe("Task events", () => {
  test("a duplicate offer returns the existing generation without an update event", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-create-update");

      const collector = yield* collectUntil(queue, "task.moved").pipe(
        Effect.forkChild,
      );

      const createdOutcome = yield* TaskQueue.offer(queue, {
        userId: "u1",
        amount: 1,
      });
      const existingOutcome = yield* TaskQueue.offer(queue, {
        userId: "u1",
        amount: 2,
      });

      const events = yield* Fiber.join(collector);
      const tags = events.map((e) => e._tag);
      expect(tags).toContain("task.created");
      expect(tags).not.toContain("task.updated");
      expect(createdOutcome._tag).toBe("TaskCreated");
      expect(existingOutcome._tag).toBe("TaskExisting");
      expect(existingOutcome.task.payload).toEqual({
        userId: "u1",
        amount: 1,
      });
      expect(existingOutcome.handle.generation).toBe(
        createdOutcome.handle.generation,
      );

      const created = events.find((e) => e._tag === "task.created");
      // Payload is decoded to a typed task, not a raw string.
      if (created?._tag === "task.created") {
        expect(created.payload.newTask.payload).toEqual({
          userId: "u1",
          amount: 1,
        });
      }
    }).pipe(TestRuntime.runPromise));

  test("success emits task.completed, failure emits task.failed with willRetry", () =>
    Effect.gen(function* () {
      const okQueue = makeQueue("ev-completed");
      const okCollector = yield* collectUntil(okQueue, "task.completed").pipe(
        Effect.forkChild,
      );

      yield* TaskQueue.offer(okQueue, { userId: "ok", amount: 1 });
      yield* TaskQueue.complete(okQueue, () => Effect.succeed("done"));

      const okEvents = yield* Fiber.join(okCollector);
      const completed = okEvents.find((e) => e._tag === "task.completed");
      expect(completed?._tag).toBe("task.completed");
      if (completed?._tag === "task.completed") {
        expect(completed.payload.success).toBe("done");
      }

      const failQueue = makeQueue("ev-failed");
      const failCollector = yield* collectUntil(failQueue, "task.failed").pipe(
        Effect.forkChild,
      );

      yield* TaskQueue.offer(
        failQueue,
        { userId: "bad", amount: 1 },
        { onFailurePolicy: "mark-as-failure" },
      );
      yield* TaskQueue.complete(failQueue, () =>
        Effect.fail({ reason: "nope" }),
      );

      const failEvents = yield* Fiber.join(failCollector);
      const failed = failEvents.find((e) => e._tag === "task.failed");
      expect(failed?._tag).toBe("task.failed");
      if (failed?._tag === "task.failed") {
        // No retry schedule configured → no retryAt, and the error is decoded typed.
        expect(failed.payload.retryAt).toBeUndefined();
        expect(failed.payload.error).toEqual({ reason: "nope" });
      }
    }).pipe(TestRuntime.runPromise));

  test("wait resolves with the typed success value", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-ok");
      const task = yield* TaskQueue.offer(queue, { userId: "w1", amount: 5 });

      // Drive the task to completion in the background.
      yield* TaskQueue.complete(queue, () => Effect.succeed("welcome")).pipe(
        Effect.forkChild,
      );

      const result = yield* TaskQueue.wait(queue, task.handle);
      expect(result).toBe("welcome");
    }).pipe(TestRuntime.runPromise));

  test("wait observes completion after its subscription has started", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-subscription-race");
      const offered = yield* TaskQueue.offer(queue, {
        userId: "subscribed",
        amount: 5,
      });
      const waiter = yield* TaskQueue.wait(queue, offered.handle).pipe(
        Effect.forkChild,
      );

      yield* Effect.sleep("20 millis");
      yield* TaskQueue.complete(queue, () => Effect.succeed("after-subscribe"));

      expect(yield* Fiber.join(waiter)).toBe("after-subscribe");
    }).pipe(TestRuntime.runPromise));

  test("wait fails with the typed error on terminal failure", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-fail");
      const task = yield* TaskQueue.offer(
        queue,
        { userId: "w2", amount: 5 },
        { onFailurePolicy: "mark-as-failure" },
      );

      yield* TaskQueue.complete(queue, () =>
        Effect.fail({ reason: "rejected" }),
      ).pipe(Effect.forkChild);

      const outcome = yield* TaskQueue.wait(queue, task.handle).pipe(
        Effect.flip,
      );
      expect(outcome).toMatchObject({
        _tag: "TaskFailed",
        failure: { reason: "rejected" },
      });
    }).pipe(TestRuntime.runPromise));

  test("wait resolves from durable state when a retained task already completed", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-already-complete");
      const offered = yield* TaskQueue.offer(
        queue,
        { userId: "done", amount: 1 },
        { onSuccessPolicy: "keep" },
      );
      yield* TaskQueue.complete(queue, () => Effect.succeed("stored"));

      expect(yield* TaskQueue.wait(queue, offered.handle)).toBe("stored");
    }).pipe(TestRuntime.runPromise));

  test("wait reads a delete-policy result until its independent retention expires", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-expired", { resultMs: 100 });
      const engine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(10_000_000);
      const offered = yield* TaskQueue.offer(queue, {
        userId: "expired",
        amount: 1,
      });
      yield* TaskQueue.complete(queue, () => Effect.succeed("discarded"));

      expect(yield* TaskQueue.wait(queue, offered.handle)).toBe("discarded");

      yield* TaskEngine.stepMockTime(101);
      yield* engine.maintain(queue.name);

      const expired = yield* TaskQueue.wait(queue, offered.handle).pipe(
        Effect.flip,
      );
      expect(expired).toMatchObject({
        _tag: "ResultExpired",
        latestGeneration: 1,
      });

      const missing = yield* TaskQueue.wait(queue, {
        ...offered.handle,
        taskId: "never-created",
      }).pipe(Effect.flip);
      expect(missing).toMatchObject({ _tag: "TaskNotFound" });
    }).pipe(TestRuntime.runPromise));

  test("wait has a typed caller timeout", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-wait-timeout");
      const offered = yield* TaskQueue.offer(
        queue,
        { userId: "pending", amount: 1 },
        { onSuccessPolicy: "keep" },
      );
      const timeout = yield* TaskQueue.wait(queue, offered.handle, {
        timeout: "10 millis",
      }).pipe(Effect.flip);
      expect(timeout).toMatchObject({ _tag: "CallerTimeout" });
    }).pipe(TestRuntime.runPromise));

  test("stream reports the earliest cursor when retained events were trimmed", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-cursor-expired");
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      yield* TaskQueue.offer(queue, { userId: "trimmed", amount: 1 });
      const before = yield* engine.eventCursors(queue.name);

      yield* redis.send(
        "XTRIM",
        `~effectmq:v1:${queue.name}:events`,
        "MAXLEN",
        "1",
      );
      const after = yield* engine.eventCursors(queue.name);
      expect(after.earliest).not.toBe(before.first);

      const error = yield* TaskQueue.stream(queue, {
        cursor: before.first,
      }).pipe(Stream.runHead, Effect.flip);
      expect(error).toMatchObject({
        _tag: "CursorExpired",
        requested: before.first,
        earliest: after.earliest,
      });
    }).pipe(TestRuntime.runPromise));

  test("configured event retention trims the stream approximately", () =>
    Effect.gen(function* () {
      const definition = Task.make({
        name: "ev-configured-trim",
        payload: { userId: Schema.String, amount: Schema.Number },
        success: Schema.String,
        error: Schema.Struct({ reason: Schema.String }),
        storageLimits: { maxEventEntries: 10 },
        idempotencyKey: (payload) => payload.userId,
      });
      const queue = TaskQueue.make("ev-configured-trim", definition);
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;

      yield* TaskQueue.offer(queue, { userId: "event-0", amount: 0 });
      const before = yield* engine.eventCursors(queue.name);
      for (let index = 1; index < 120; index++) {
        yield* TaskQueue.offer(queue, {
          userId: `event-${index}`,
          amount: index,
        });
      }

      const after = yield* engine.eventCursors(queue.name);
      const retained = yield* redis.send<number>(
        "XLEN",
        `~effectmq:v1:${queue.name}:events`,
      );
      expect(after.earliest).not.toBe(before.first);
      expect(retained).toBeLessThan(240);
      expect(retained).toBeGreaterThanOrEqual(10);
    }).pipe(TestRuntime.runPromise));

  test("a corrupt event value fails the stream with a typed storage error", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-corrupt-value");
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      yield* TaskQueue.offer(queue, { userId: "corrupt", amount: 1 });
      const cursor = (yield* engine.eventCursors(queue.name)).latest;
      const packr = new Packr({ useRecords: false });

      yield* redis.send(
        "XADD",
        `~effectmq:v1:${queue.name}:events`,
        "*",
        "taskId",
        "corrupt",
        "generation",
        "1",
        "protocolVersion",
        "1",
        "schemaId",
        queue.task.schemaId,
        "_tag",
        "task.failed",
        "policy",
        "keep",
        "error",
        packr.pack("not-an-effectmq-envelope"),
        "failureKind",
        "handler",
        "attempt",
        "1",
        "terminal",
        "1",
      );

      const error = yield* TaskQueue.stream(queue, { cursor }).pipe(
        Stream.runHead,
        Effect.flip,
      );
      expect(error).toMatchObject({ _tag: "CorruptStorageValue" });
    }).pipe(TestRuntime.runPromise));

  test("execute offers and resolves with the handler's success value", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-execute");

      // A worker that keeps pulling — including a fast handler that completes
      // near-instantly, which execute must not miss.
      yield* TaskQueue.complete(queue, () => Effect.succeed("sent")).pipe(
        Effect.repeat(Schedule.forever),
        Effect.forkChild,
      );

      const result = yield* TaskQueue.execute(queue, {
        userId: "e1",
        amount: 9,
      });
      expect(result).toBe("sent");
    }).pipe(TestRuntime.runPromise));
});
