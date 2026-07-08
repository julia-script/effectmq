import { Cron, Effect, Fiber } from "effect";
import { describe, expect, test } from "vitest";
import { Scheduler, TaskEngine } from "./index.js";
import { TestRuntime } from "./testing/redisLayer.js";

describe("schedule primitives", () => {
  test("setSchedule is first-writer-wins", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const first = new Date(1000000000000);
      const second = new Date(1000000060000);

      const initial = yield* engine.setSchedule("sched-first-wins", first);
      expect(initial.getTime()).toBe(first.getTime());

      // a second worker proposing a different next run keeps the stored one
      const kept = yield* engine.setSchedule("sched-first-wins", second);
      expect(kept.getTime()).toBe(first.getTime());
    }).pipe(TestRuntime.runPromise));

  test("consumeSchedule on an unset schedule consumes nothing", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const result = yield* engine.consumeSchedule(
        "sched-unset",
        new Date(1000000000000),
        new Date(1000000060000),
      );
      expect(result.consumed).toBe(false);
      expect(result.next).toBeUndefined();
    }).pipe(TestRuntime.runPromise));

  test("consumeSchedule with a stale expected tick returns the stored schedule", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const now = 1000000000000;
      yield* TaskEngine.setMockTime(now);
      const stored = new Date(now + 60000);
      yield* engine.setSchedule("sched-mismatch", stored);

      // the worker computed its tick from a stale value; it must not consume,
      // and it gets the actual stored schedule back to retry with
      const result = yield* engine.consumeSchedule(
        "sched-mismatch",
        new Date(now + 999),
        new Date(now + 120000),
      );
      expect(result.consumed).toBe(false);
      expect(result.next?.getTime()).toBe(stored.getTime());
    }).pipe(TestRuntime.runPromise));

  test("consumeSchedule leaves a future tick unconsumed until the clock reaches it", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const now = 1000000000000;
      yield* TaskEngine.setMockTime(now);
      const tick = new Date(now + 60000);
      const following = new Date(now + 120000);
      yield* engine.setSchedule("sched-future", tick);

      const early = yield* engine.consumeSchedule(
        "sched-future",
        tick,
        following,
      );
      expect(early.consumed).toBe(false);
      expect(early.next?.getTime()).toBe(tick.getTime());

      yield* TaskEngine.stepMockTime(60000);
      const due = yield* engine.consumeSchedule(
        "sched-future",
        tick,
        following,
      );
      expect(due.consumed).toBe(true);
      expect(due.next?.getTime()).toBe(following.getTime());
    }).pipe(TestRuntime.runPromise));

  test("a due tick is consumed by exactly one of two workers", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const now = 1000000000000;
      yield* TaskEngine.setMockTime(now);
      const tick = new Date(now - 1000);
      const following = new Date(now + 59000);
      yield* engine.setSchedule("sched-race", tick);

      // both workers derived the same (tick, following) pair; only the first
      // consume wins, the loser is handed the advanced schedule
      const winner = yield* engine.consumeSchedule(
        "sched-race",
        tick,
        following,
      );
      const loser = yield* engine.consumeSchedule(
        "sched-race",
        tick,
        following,
      );

      expect(winner.consumed).toBe(true);
      expect(winner.next?.getTime()).toBe(following.getTime());
      expect(loser.consumed).toBe(false);
      expect(loser.next?.getTime()).toBe(following.getTime());
    }).pipe(TestRuntime.runPromise));
});

describe("Scheduler", () => {
  test("two workers running the same named scheduler fire the handler once per tick", () =>
    Effect.gen(function* () {
      // Move the engine clock two minutes ahead of the wall clock, so the first
      // cron tick (computed from the wall clock) is already due, while the
      // following tick stays out of reach for the duration of the test.
      yield* TaskEngine.setMockTime(Date.now() + 120_000);

      let fires = 0;
      const makeWorker = () =>
        Scheduler.make({
          cron: Cron.parseUnsafe("* * * * *"),
          name: "sched-collective",
          handler: Effect.sync(() => {
            fires++;
          }),
        });

      const worker1 = yield* makeWorker().pipe(Effect.forkChild);
      const worker2 = yield* makeWorker().pipe(Effect.forkChild);

      yield* Effect.sleep("2 seconds");
      yield* Fiber.interrupt(worker1);
      yield* Fiber.interrupt(worker2);

      expect(fires).toBe(1);
    }).pipe(TestRuntime.runPromise));
});
