import { Effect, Fiber, Schedule, Schema, Stream } from "effect";
import { describe, expect, test } from "vitest";
import { Task, TaskQueue } from "./index.js";
import { TestRuntime } from "./testing/redisLayer.js";

// A typed queue with a deterministic id (idempotencyKey) so we can address a
// specific task's events by id.
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

// Read events from the very start of the queue's stream until an event with one
// of `stopTags` is seen, returning everything collected up to and including it.
const collectUntil = (queue: ReturnType<typeof makeQueue>, stopTag: string) =>
  TaskQueue.stream(queue, { cursor: "0" }).pipe(
    Stream.takeUntil((e) => e._tag === stopTag),
    Stream.runCollect,
  );

describe("Task events", () => {
  test("a created task emits task.created, re-creating the id emits task.updated", () =>
    Effect.gen(function* () {
      const queue = makeQueue("ev-create-update");

      // Collect events in the background so we don't miss the fast ones.
      const collector = yield* collectUntil(queue, "task.updated").pipe(
        Effect.forkChild,
      );

      yield* TaskQueue.offer(queue, { userId: "u1", amount: 1 });
      // Same idempotency key → re-create → update.
      yield* TaskQueue.offer(queue, { userId: "u1", amount: 2 });

      const events = yield* Fiber.join(collector);
      const tags = events.map((e) => e._tag);
      expect(tags).toContain("task.created");
      expect(tags).toContain("task.updated");

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
        // No retries configured → won't retry, and the error is decoded typed.
        expect(failed.payload.willRetry).toBe(false);
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

      const result = yield* TaskQueue.wait(queue, task.id);
      expect(result).toBe("welcome");
    }).pipe(TestRuntime.runPromise));

  test(
    "wait fails with the typed error on terminal failure",
    () =>
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

        const outcome = yield* TaskQueue.wait(queue, task.id).pipe(Effect.flip);
        expect(outcome).toEqual({ reason: "rejected" });
      }).pipe(TestRuntime.runPromise),
    15_000,
  );

  test(
    "execute offers and resolves with the handler's success value",
    () =>
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
      }).pipe(TestRuntime.runPromise),
    15_000,
  );
});
