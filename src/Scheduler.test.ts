import { Cron, Effect, Schedule, Schema } from "effect";
import { describe, expect, test } from "vitest";
import {
  Scheduler,
  StorageProtocol,
  Task,
  TaskEngine,
  TaskQueue,
} from "./index.js";
import { TestRuntime } from "./testing/redisLayer.js";

const makeQueue = (name: string, retry = false) => {
  const task = Task.make({
    name,
    payload: {
      scheduledAt: Schema.String,
      missedFrom: Schema.String,
      missedTo: Schema.String,
    },
    success: Schema.String,
    error: Schema.Struct({ reason: Schema.String }),
    idempotencyKey: (payload) => payload.scheduledAt,
    ...(retry ? { retry: Schedule.spaced("10 millis"), maxRetries: 1 } : {}),
  });
  return TaskQueue.make(name, task);
};

const config = (
  name: string,
  queue: ReturnType<typeof makeQueue>,
  missed: Scheduler.MissedTickPolicy = { _tag: "coalesce" },
) => ({
  name,
  cron: Cron.parseUnsafe("* * * * *", "UTC"),
  queue,
  missed,
  payload: (tick: Scheduler.Tick) => ({
    scheduledAt: tick.scheduledAt.toISOString(),
    missedFrom: tick.missedFrom.toISOString(),
    missedTo: tick.missedTo.toISOString(),
  }),
});

describe("durable Scheduler", () => {
  test("competing schedulers materialize one deterministic tick task", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("scheduled-race-queue");
      const definition = config("scheduled-race", queue);
      const now = new Date("2026-01-01T00:01:30.000Z");
      const tick = new Date("2026-01-01T00:01:00.000Z");
      yield* TaskEngine.setMockTime(now.getTime());
      yield* engine.setSchedule(definition.name, tick);

      yield* Effect.all(
        [
          Scheduler.materializeDue(definition, now),
          Scheduler.materializeDue(definition, now),
        ],
        { concurrency: "unbounded" },
      );

      const waiting = yield* engine.listTasks(queue.name, "wait");
      expect(waiting.items).toEqual([`scheduled-race/${tick.toISOString()}`]);
      expect(
        (yield* engine.getTask(queue.name, waiting.items[0]))?.generation,
      ).toBe(1);
    }).pipe(TestRuntime.runPromise));

  test("an offer committed before a scheduler crash is replay-safe", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("scheduled-crash-queue");
      const definition = config("scheduled-crash", queue);
      const tick = new Date("2026-01-01T00:02:00.000Z");
      const now = new Date("2026-01-01T00:02:30.000Z");
      yield* TaskEngine.setMockTime(now.getTime());
      yield* engine.setSchedule(definition.name, tick);

      // This is the state left by a process that offered and died before it
      // advanced the schedule cursor.
      yield* TaskQueue.offer(
        queue,
        {
          scheduledAt: tick.toISOString(),
          missedFrom: tick.toISOString(),
          missedTo: tick.toISOString(),
        },
        { taskId: `scheduled-crash/${tick.toISOString()}` },
      );
      yield* Scheduler.materializeDue(definition, now);

      const waiting = yield* engine.listTasks(queue.name, "wait");
      expect(waiting.items).toEqual([`scheduled-crash/${tick.toISOString()}`]);
      expect(
        (yield* engine.getTask(queue.name, waiting.items[0]))?.generation,
      ).toBe(1);
    }).pipe(TestRuntime.runPromise));

  test("a crash before offer leaves the due cursor available", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("scheduled-before-offer-queue");
      const definition = config("scheduled-before-offer", queue);
      const tick = new Date("2026-01-01T00:03:00.000Z");
      const now = new Date("2026-01-01T00:03:30.000Z");
      yield* TaskEngine.setMockTime(now.getTime());
      yield* engine.setSchedule(definition.name, tick);

      // No operation occurs before the simulated crash. A replacement
      // scheduler sees the same cursor and materializes the task normally.
      yield* Scheduler.materializeDue(definition, now);
      expect((yield* engine.listTasks(queue.name, "wait")).items).toEqual([
        `scheduled-before-offer/${tick.toISOString()}`,
      ]);
    }).pipe(TestRuntime.runPromise));

  test("skip, coalesce, and bounded backfill have explicit downtime behavior", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const now = new Date("2026-01-01T00:05:00.000Z");
      const firstMissed = new Date("2026-01-01T00:00:00.000Z");
      yield* TaskEngine.setMockTime(now.getTime());

      const skipQueue = makeQueue("scheduled-skip-queue");
      const skip = config("scheduled-skip", skipQueue, { _tag: "skip" });
      yield* engine.setSchedule(skip.name, firstMissed);
      yield* Scheduler.materializeDue(skip, now);
      expect((yield* engine.listTasks(skipQueue.name, "wait")).items).toEqual(
        [],
      );

      const coalesceQueue = makeQueue("scheduled-coalesce-queue");
      const coalesce = config("scheduled-coalesce", coalesceQueue);
      yield* engine.setSchedule(coalesce.name, firstMissed);
      yield* Scheduler.materializeDue(coalesce, now);
      const coalesced = (yield* engine.listTasks(coalesceQueue.name, "wait"))
        .items;
      expect(coalesced).toEqual([`scheduled-coalesce/${now.toISOString()}`]);
      const coalescedTask = yield* engine.getTask(
        coalesceQueue.name,
        coalesced[0],
      );
      const coalescedPayload = yield* StorageProtocol.decodeValue(
        coalescedTask?.payload,
        coalesceQueue.task.schemaId,
        "payload",
      );
      expect(coalescedPayload).toMatchObject({
        missedFrom: firstMissed.toISOString(),
      });

      const backfillQueue = makeQueue("scheduled-backfill-queue");
      const backfill = config("scheduled-backfill", backfillQueue, {
        _tag: "backfill",
        maxBackfill: 2,
      });
      yield* engine.setSchedule(backfill.name, firstMissed);
      yield* Scheduler.materializeDue(backfill, now);
      expect(
        (yield* engine.listTasks(backfillQueue.name, "wait")).items,
      ).toEqual([
        `scheduled-backfill/2026-01-01T00:04:00.000Z`,
        `scheduled-backfill/2026-01-01T00:05:00.000Z`,
      ]);
    }).pipe(TestRuntime.runPromise));

  test("scheduled work executes and retries through normal queue semantics", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("scheduled-retry-queue", true);
      const definition = config("scheduled-retry", queue);
      const now = new Date(Math.floor(Date.now() / 60_000) * 60_000 + 30_000);
      const tick = new Date(now.getTime() - 30_000);
      yield* TaskEngine.setMockTime(now.getTime());
      yield* engine.setSchedule(definition.name, tick);
      yield* Scheduler.materializeDue(definition, now);

      let attempts = 0;
      yield* TaskQueue.complete(queue, () => {
        attempts++;
        return Effect.fail({ reason: "retry" });
      });
      const retryTask = yield* engine.getTask(
        queue.name,
        `scheduled-retry/${tick.toISOString()}`,
      );
      const retryAt = retryTask?.errors.at(-1)?.retryAt;
      expect(retryAt).toBeDefined();
      if (retryAt === undefined) return yield* Effect.die("Expected retryAt");
      // Retry schedules are evaluated from the recorded handler-failure time.
      // Advance the mocked Redis clock to that durable deadline instead of
      // assuming it is within 20 ms of this process's wall clock.
      yield* TaskEngine.setMockTime(retryAt + 1);
      yield* engine.maintain(queue.name);
      yield* TaskQueue.complete(queue, () => {
        attempts++;
        return Effect.succeed("done");
      });

      expect(attempts).toBe(2);
    }).pipe(TestRuntime.runPromise));

  test("a lost scheduled-task lease can execute the handler again", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("scheduled-at-least-once-queue");
      const definition = config("scheduled-at-least-once", queue);
      const now = new Date("2026-01-01T00:08:30.000Z");
      const tick = new Date("2026-01-01T00:08:00.000Z");
      yield* TaskEngine.setMockTime(now.getTime());
      yield* engine.setSchedule(definition.name, tick);
      yield* Scheduler.materializeDue(definition, now);

      let executions = 0;
      const abandoned = yield* engine.takeTask(queue.name, 100);
      if (abandoned === null)
        return yield* Effect.die("Expected scheduled task");
      executions++;
      yield* TaskEngine.stepMockTime(101);
      yield* engine.maintain(queue.name);

      yield* TaskQueue.complete(queue, () => {
        executions++;
        return Effect.succeed("done");
      });
      expect(executions).toBe(2);
    }).pipe(TestRuntime.runPromise));

  test("the configured cron timezone determines the nominal tick", () => {
    const cron = Cron.parseUnsafe("0 9 * * *", "America/New_York");
    expect(Cron.next(cron, new Date("2026-03-08T12:00:00.000Z"))).toEqual(
      new Date("2026-03-08T13:00:00.000Z"),
    );
  });
});
