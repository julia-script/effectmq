import { Effect, Schedule, Schema } from "effect";
import { describe, expect, test } from "vitest";
import { Task, TaskQueue } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

// A typed queue with a deterministic id so wait-list assertions are exact.
const makeQueue = (name: string) => {
  const def = Task.make({
    name,
    payload: { userId: Schema.String, amount: Schema.Number },
    success: Schema.String,
    error: Schema.Struct({ reason: Schema.String }),
    idempotencyKey: (p) => p.userId,
  });
  return TaskQueue.make(name, def);
};

// A queue whose task defines a retry schedule; `maxRetries` caps the attempts.
const makeRetryQueue = (
  name: string,
  opts?: { maxRetries?: number | null },
) => {
  const def = Task.make({
    name,
    payload: { userId: Schema.String },
    success: Schema.String,
    error: Schema.Struct({ reason: Schema.String }),
    idempotencyKey: (p) => p.userId,
    retry: Schedule.spaced("10 seconds"),
    ...(opts?.maxRetries !== undefined ? { maxRetries: opts.maxRetries } : {}),
  });
  return TaskQueue.make(name, def);
};

describe("TaskQueue", () => {
  test("offer places a task with the deterministic id on the wait list", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-offer");
      const task = yield* TaskQueue.offer(queue, { userId: "u1", amount: 10 });

      expect(task.id).toBe("u1");
      const lists = yield* getLists(queue.name);
      expect(lists.wait).toEqual(["u1"]);
    }).pipe(TestRuntime.runPromise));

  test("complete runs the handler, delivers the typed payload, and reports success", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-complete-ok");
      yield* TaskQueue.offer(queue, { userId: "u2", amount: 42 });

      let seenPayload: { userId: string; amount: number } | undefined;
      const done = yield* TaskQueue.complete(queue, (task) => {
        seenPayload = task.payload;
        return Effect.succeed("ok");
      });

      expect(done).toBe("u2");
      // handler received the decoded typed payload, not the raw JSON string
      expect(seenPayload).toEqual({ userId: "u2", amount: 42 });

      const lists = yield* getLists(queue.name);
      expect(lists.wait).toEqual([]);
      expect(lists.active).toEqual([]);
    }).pipe(TestRuntime.runPromise));

  test("complete routes a handler failure to the engine and reports false", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-complete-fail");
      yield* TaskQueue.offer(
        queue,
        { userId: "u3", amount: 7 },
        {
          onFailurePolicy: "mark-as-failure",
        },
      );

      const done = yield* TaskQueue.complete(queue, () =>
        Effect.fail({ reason: "nope" }),
      );

      expect(done).toBe("u3");
      const lists = yield* getLists(queue.name);
      expect(lists.failed).toEqual(["u3"]);
    }).pipe(TestRuntime.runPromise));

  test("a failing task with a retry schedule lands on the scheduled list", () =>
    Effect.gen(function* () {
      const queue = makeRetryQueue("tq-retry-schedule");
      yield* TaskQueue.offer(queue, { userId: "s1" });

      yield* TaskQueue.complete(queue, () => Effect.fail({ reason: "boom" }));

      const lists = yield* getLists(queue.name);
      // Schedule.spaced("10 seconds") → first retry ~10s out, so it is scheduled.
      expect(lists.scheduled).toEqual(["s1"]);
      expect(lists.failed).toEqual([]);
    }).pipe(TestRuntime.runPromise));

  test("maxRetries cap of 0 skips retries even with a schedule", () =>
    Effect.gen(function* () {
      // cap at 0 → the first failure can't retry (0 errors is not < 0)
      const queue = makeRetryQueue("tq-retry-cap", { maxRetries: 0 });
      yield* TaskQueue.offer(
        queue,
        { userId: "c1" },
        { onFailurePolicy: "mark-as-failure" },
      );

      yield* TaskQueue.complete(queue, () => Effect.fail({ reason: "boom" }));

      const lists = yield* getLists(queue.name);
      expect(lists.scheduled).toEqual([]);
      expect(lists.failed).toEqual(["c1"]);
    }).pipe(TestRuntime.runPromise));

  test("per-offer maxRetries overrides the definition cap", () =>
    Effect.gen(function* () {
      // definition would allow 5 retries, but the offer caps at 0 → no retry
      const queue = makeRetryQueue("tq-retry-override", { maxRetries: 5 });
      yield* TaskQueue.offer(
        queue,
        { userId: "o1" },
        { maxRetries: 0, onFailurePolicy: "mark-as-failure" },
      );

      yield* TaskQueue.complete(queue, () => Effect.fail({ reason: "boom" }));

      const lists = yield* getLists(queue.name);
      expect(lists.scheduled).toEqual([]);
      expect(lists.failed).toEqual(["o1"]);
    }).pipe(TestRuntime.runPromise));
});
