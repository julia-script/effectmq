import { Effect, Schema } from "effect";
import { describe, expect, test } from "vitest";
import { Task, TaskQueue } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

// A typed queue with a deterministic id so wait-list assertions are exact.
const makeQueue = (name: string) => {
  const def = Task.make({
    name,
    payload: { userId: Schema.String, amount: Schema.Number },
    successSchema: Schema.String,
    errorSchema: Schema.Struct({ reason: Schema.String }),
    idempotencyKey: (p) => p.userId,
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

      expect(done).toBe(true);
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

      expect(done).toBe(false);
      const lists = yield* getLists(queue.name);
      expect(lists.failed).toEqual(["u3"]);
    }).pipe(TestRuntime.runPromise));
});
