import { Deferred, Effect, Schedule, Schema } from "effect";
import * as PersistenceRedis from "effect/unstable/persistence/Redis";
import { describe, expect, test } from "vitest";
import { RedisPool, Task, TaskEngine, TaskQueue } from "./index.js";
import type { EngineTask } from "./Schemas.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";
import { takeTask } from "./testing/TaskAttemptHarness.js";

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
  test("task storage limits reject invalid configuration", () => {
    expect(() =>
      Task.make({
        name: "invalid-storage-limit",
        payload: { value: Schema.String },
        success: Schema.Void,
        error: Schema.Never,
        storageLimits: { maxErrorEntries: -1 },
      }),
    ).toThrow("maxErrorEntries must be a non-negative safe integer");
  });

  test("a task's configured byte limit is enforced while offering", () =>
    Effect.gen(function* () {
      const definition = Task.make({
        name: "tq-payload-limit",
        payload: { value: Schema.String },
        success: Schema.Void,
        error: Schema.Never,
        storageLimits: { maxValueBytes: 8 },
        idempotencyKey: () => "limited",
      });
      const queue = TaskQueue.make(definition.name, definition);
      const error = yield* TaskQueue.offer(queue, { value: "too large" }).pipe(
        Effect.flip,
      );
      expect(error).toMatchObject({
        _tag: "StorageLimitExceeded",
        kind: "payload",
        maxBytes: 8,
      });
    }).pipe(TestRuntime.runPromise));

  test("outcome byte limits fail before writing terminal state", () =>
    Effect.gen(function* () {
      const successDefinition = Task.make({
        name: "tq-success-limit",
        payload: {},
        success: Schema.String,
        error: Schema.Never,
        storageLimits: { maxValueBytes: 100 },
        idempotencyKey: () => "limited-success",
      });
      const successQueue = TaskQueue.make(
        successDefinition.name,
        successDefinition,
      );
      yield* TaskQueue.offer(successQueue, {});
      const successError = yield* TaskQueue.complete(successQueue, () =>
        Effect.succeed("x".repeat(200)),
      ).pipe(Effect.flip);
      expect(successError).toMatchObject({
        _tag: "StorageLimitExceeded",
        kind: "success",
        maxBytes: 100,
      });
      const engine = yield* TaskEngine.TaskEngine;
      expect(
        (yield* engine.getTask(successQueue.name, "limited-success"))?.outcome,
      ).toBeUndefined();

      const failureDefinition = Task.make({
        name: "tq-failure-limit",
        payload: {},
        success: Schema.Never,
        error: Schema.Struct({ reason: Schema.String }),
        storageLimits: { maxValueBytes: 100 },
        idempotencyKey: () => "limited-failure",
      });
      const failureQueue = TaskQueue.make(
        failureDefinition.name,
        failureDefinition,
      );
      yield* TaskQueue.offer(failureQueue, {});
      const failureError = yield* TaskQueue.complete(failureQueue, () =>
        Effect.fail({ reason: "x".repeat(200) }),
      ).pipe(Effect.flip);
      expect(failureError).toMatchObject({
        _tag: "StorageLimitExceeded",
        kind: "failure",
        maxBytes: 100,
      });
      expect(
        (yield* engine.getTask(failureQueue.name, "limited-failure"))?.outcome,
      ).toBeUndefined();
    }).pipe(TestRuntime.runPromise));

  test("connection loss during offer reports an indeterminate write", async () => {
    const queue = makeQueue("tq-indeterminate");
    const send: RedisPool.RedisSend = () =>
      Effect.fail(
        new PersistenceRedis.RedisError({
          cause: new Error("read ECONNRESET"),
        }),
      );

    const outcome = await Effect.gen(function* () {
      const redisPool = yield* RedisPool.make(send, send);
      const engine = yield* TaskEngine.make().pipe(
        Effect.provideService(RedisPool.RedisPool, redisPool),
      );
      return yield* TaskQueue.offer(queue, {
        userId: "retry-me",
        amount: 1,
      }).pipe(
        Effect.provideService(TaskEngine.TaskEngine, engine),
        Effect.flip,
      );
    }).pipe(Effect.runPromise);

    expect(outcome).toBeInstanceOf(TaskQueue.IndeterminateWriteError);
    expect(outcome).toMatchObject({
      _tag: "IndeterminateWriteError",
      queue: queue.name,
      taskId: "retry-me",
    });
  });

  test("offer places a task with the deterministic id on the wait list", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-offer");
      const offered = yield* TaskQueue.offer(queue, {
        userId: "u1",
        amount: 10,
      });

      expect(offered._tag).toBe("TaskCreated");
      expect(offered.task.id).toBe("u1");
      expect(offered.task.generation).toBe(1);
      expect(offered.handle).toMatchObject({
        _tag: "TaskHandle",
        generation: 1,
        queue: queue.name,
        taskId: "u1",
      });
      const lists = yield* getLists(queue.name);
      expect(lists.wait).toEqual(["u1"]);
    }).pipe(TestRuntime.runPromise));

  test("v1 envelopes preserve opaque payloads, successes, and failures", () =>
    Effect.gen(function* () {
      const definition = Task.make({
        name: "tq-storage-v1",
        schemaId: "example/storage-v1",
        payload: { id: Schema.String, value: Schema.Unknown },
        success: Schema.Unknown,
        error: Schema.Unknown,
        idempotencyKey: (payload) => payload.id,
      });
      const queue = TaskQueue.make("tq-storage-v1", definition);
      const opaque = {
        binary: new Uint8Array([0, 255, 1]),
        emptyArray: [],
        emptyObject: {},
        nested: { none: null },
        text: "✓🎉",
      };
      const offered = yield* TaskQueue.offer(
        queue,
        { id: "success", value: opaque },
        { onSuccessPolicy: "keep" },
      );
      expect(offered.task.payload.value).toEqual(opaque);
      yield* TaskQueue.complete(queue, () => Effect.succeed(opaque));
      const succeeded = yield* TaskQueue.offer(queue, {
        id: "success",
        value: "ignored duplicate",
      });
      expect(succeeded.task.success).toEqual(opaque);

      const failure = { reason: { nested: null }, values: [] };
      yield* TaskQueue.offer(
        queue,
        { id: "failure", value: null },
        { onFailurePolicy: "keep" },
      );
      yield* TaskQueue.complete(queue, () => Effect.fail(failure));
      const failed = yield* TaskQueue.offer(queue, {
        id: "failure",
        value: "ignored duplicate",
      });
      expect(failed.task.errors.at(-1)?.error).toEqual(failure);
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

  test("complete interrupts its handler when the heartbeat loses ownership", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("tq-heartbeat-lease-lost");
      yield* TaskQueue.offer(queue, { userId: "owned", amount: 1 });

      const handlerStarted = yield* Deferred.make<void>();
      let handlerInterrupted = false;
      const leaseLost = new TaskEngine.LeaseLost({
        prefix: queue.name,
        taskId: "owned",
        cause: new TaskEngine.TaskEngineError({ cause: "test lease theft" }),
      });
      const losingEngine = TaskEngine.TaskEngine.of({
        ...engine,
        extendLock: () =>
          Deferred.await(handlerStarted).pipe(
            Effect.andThen(Effect.fail(leaseLost)),
          ),
      });

      const error = yield* TaskQueue.complete(queue, () =>
        Deferred.succeed(handlerStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              handlerInterrupted = true;
            }),
          ),
        ),
      ).pipe(
        Effect.provideService(TaskEngine.TaskEngine, losingEngine),
        Effect.flip,
      );

      expect(error._tag).toBe("LeaseLost");
      expect(handlerInterrupted).toBe(true);
      expect(yield* getLists(queue.name)).toMatchObject({
        active: ["owned"],
        success: [],
        failed: [],
      });
    }).pipe(TestRuntime.runPromise));

  test("heartbeat transport failures retry only within the configured bound", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("tq-heartbeat-retry");
      yield* TaskQueue.offer(queue, { userId: "retry", amount: 1 });
      let renewals = 0;
      const recoveringEngine = TaskEngine.TaskEngine.of({
        ...engine,
        extendLock: (prefix, id, token, timeout) =>
          Effect.sync(() => ++renewals).pipe(
            Effect.flatMap((attempt) =>
              attempt < 3
                ? Effect.fail(
                    new TaskEngine.TaskEngineError({
                      cause: new Error("temporary Redis transport failure"),
                    }),
                  )
                : engine.extendLock(prefix, id, token, timeout),
            ),
          ),
      });

      const processed = yield* TaskQueue.completeOne(
        queue,
        () => Effect.sleep("20 millis").pipe(Effect.as("ok")),
        {
          lockTimeout: "1 second",
          lockRefresh: "100 millis",
          heartbeatRetryDelay: "1 millis",
          heartbeatRetryCount: 2,
        },
      ).pipe(Effect.provideService(TaskEngine.TaskEngine, recoveringEngine));

      expect(processed).toBe(true);
      expect(renewals).toBe(3);
      expect(yield* engine.getTask(queue.name, "retry")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("a duplicate offer does not mutate a leased generation", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("tq-duplicate-leased");
      const created = yield* TaskQueue.offer(queue, {
        userId: "leased",
        amount: 1,
      });
      yield* takeTask(engine, queue.name, 30_000);

      const existing = yield* TaskQueue.offer(queue, {
        userId: "leased",
        amount: 999,
      });

      expect(existing._tag).toBe("TaskExisting");
      expect(existing.handle.generation).toBe(created.handle.generation);
      expect(existing.task.payload.amount).toBe(1);
      expect(yield* getLists(queue.name)).toMatchObject({
        active: ["leased"],
        wait: [],
      });
    }).pipe(TestRuntime.runPromise));

  test("duplicate offers preserve delayed and retry-scheduled generations", () =>
    Effect.gen(function* () {
      const delayedQueue = makeQueue("tq-duplicate-delayed");
      yield* TaskQueue.offer(
        delayedQueue,
        { userId: "delayed", amount: 1 },
        { delay: 60_000 },
      );
      const delayed = yield* TaskQueue.offer(
        delayedQueue,
        { userId: "delayed", amount: 999 },
        { delay: 0 },
      );
      expect(delayed).toMatchObject({
        _tag: "TaskExisting",
        task: { delay: 60_000, payload: { amount: 1 } },
      });
      expect(yield* getLists(delayedQueue.name)).toMatchObject({
        scheduled: ["delayed"],
        wait: [],
      });

      const retryQueue = makeRetryQueue("tq-duplicate-retry");
      yield* TaskQueue.offer(retryQueue, { userId: "retry" });
      yield* TaskQueue.complete(retryQueue, () =>
        Effect.fail({ reason: "first" }),
      );
      const retry = yield* TaskQueue.offer(retryQueue, { userId: "retry" });
      expect(retry).toMatchObject({
        _tag: "TaskExisting",
        task: { errors: [{ error: { reason: "first" } }] },
      });
      expect(yield* getLists(retryQueue.name)).toMatchObject({
        scheduled: ["retry"],
        wait: [],
      });
    }).pipe(TestRuntime.runPromise));

  test("an explicit new generation starts clean after terminal settlement", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-new-generation");
      const created = yield* TaskQueue.offer(
        queue,
        { userId: "repeat", amount: 1 },
        { onSuccessPolicy: "keep" },
      );
      yield* TaskQueue.complete(queue, () => Effect.succeed("old-result"));

      const existing = yield* TaskQueue.offer(queue, {
        userId: "repeat",
        amount: 2,
      });
      expect(existing._tag).toBe("TaskExisting");
      expect(existing.handle.generation).toBe(created.handle.generation);
      expect(existing.task.payload.amount).toBe(1);
      expect(existing.task.success).toBe("old-result");

      const next = yield* TaskQueue.offer(
        queue,
        { userId: "repeat", amount: 2 },
        { onDuplicate: "new-generation", onSuccessPolicy: "keep" },
      );
      expect(next._tag).toBe("TaskCreated");
      expect(next.handle.generation).toBe(created.handle.generation + 1);
      expect(next.task).toMatchObject({
        errors: [],
        generation: 2,
        payload: { userId: "repeat", amount: 2 },
      });
      expect(next.task.success).toBeUndefined();
      expect(yield* TaskQueue.wait(queue, created.handle)).toBe("old-result");
      expect(yield* getLists(queue.name)).toMatchObject({
        active: [],
        success: [],
        wait: ["repeat"],
      });
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

describe("TaskQueue managed task context", () => {
  test("a nested offer records creator provenance without implicit retention", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const parent = makeQueue("tq-ctx-parent");
      const child = makeQueue("tq-ctx-child");
      yield* TaskQueue.offer(parent, { userId: "p1", amount: 1 });

      let during:
        | { child: EngineTask | null; parent: EngineTask | null }
        | undefined;
      yield* TaskQueue.complete(parent, () =>
        Effect.gen(function* () {
          yield* TaskQueue.offer(child, { userId: "c1", amount: 2 });
          during = {
            child: yield* engine.getTask(child.name, "c1"),
            parent: yield* engine.getTask(parent.name, "p1"),
          };
          return "ok";
        }).pipe(Effect.orDie),
      );

      expect(during?.child?.creator).toEqual({
        queue: "~effectmq:v1:tq-ctx-parent",
        id: "p1",
        generation: 1,
      });

      // Parent settlement neither cancels, joins, nor otherwise affects the
      // independently runnable spawned task.
      expect((yield* getLists(child.name)).wait).toContain("c1");

      yield* TaskQueue.complete(child, () => Effect.succeed("ok"));
      expect(yield* engine.getTask(child.name, "c1")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("a creator failure does not fail, cancel, or join its spawned task", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const creator = makeQueue("tq-ctx-failure-creator");
      const spawned = makeQueue("tq-ctx-failure-spawned");
      yield* TaskQueue.offer(creator, { userId: "creator", amount: 1 });

      yield* TaskQueue.complete(creator, () =>
        TaskQueue.offer(spawned, { userId: "spawned", amount: 2 }).pipe(
          Effect.andThen(Effect.fail({ reason: "creator failed" })),
        ),
      );

      const independent = yield* engine.getTask(spawned.name, "spawned");
      expect(independent?.creator).toMatchObject({
        queue: `~effectmq:v1:${creator.name}`,
        id: "creator",
      });
      expect((yield* getLists(spawned.name)).wait).toEqual(["spawned"]);
      expect(independent?.errors).toEqual([]);
    }).pipe(TestRuntime.runPromise));

  test("explicit retention keeps the spawned result until the current task settles", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      const parent = makeQueue("tq-ctx-det-parent");
      const child = makeQueue("tq-ctx-det-child");
      yield* TaskQueue.offer(parent, { userId: "p1", amount: 1 });

      let during:
        | { child: EngineTask | null; parent: EngineTask | null }
        | undefined;
      let holdCountDuring = 0;
      yield* TaskQueue.complete(parent, () =>
        Effect.gen(function* () {
          yield* TaskQueue.offer(
            child,
            { userId: "c1", amount: 2 },
            { retainResultUntil: "current-task-settles" },
          );
          during = {
            child: yield* engine.getTask(child.name, "c1"),
            parent: yield* engine.getTask(parent.name, "p1"),
          };
          holdCountDuring = yield* redis.send(
            "SCARD",
            "~effectmq:v1:tq-ctx-det-child:task:c1:1:retained-by",
          );
          return "ok";
        }).pipe(Effect.orDie),
      );

      expect(holdCountDuring).toBe(1);
      expect(during?.child?.creator).toEqual({
        queue: "~effectmq:v1:tq-ctx-det-parent",
        id: "p1",
        generation: 1,
      });
      expect(
        yield* redis.send(
          "SCARD",
          "~effectmq:v1:tq-ctx-det-child:task:c1:1:retained-by",
        ),
      ).toBe(0);
    }).pipe(TestRuntime.runPromise));

  test("offering outside a handler records no holder or creator", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const queue = makeQueue("tq-ctx-none");
      yield* TaskQueue.offer(queue, { userId: "u1", amount: 1 });

      const task = yield* engine.getTask(queue.name, "u1");
      expect(task?.creator).toBeUndefined();
    }).pipe(TestRuntime.runPromise));

  test("explicit current-task retention is rejected outside a handler", () =>
    Effect.gen(function* () {
      const queue = makeQueue("tq-ctx-retention-required");
      const error = yield* TaskQueue.offer(
        queue,
        { userId: "u1", amount: 1 },
        { retainResultUntil: "current-task-settles" },
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(TaskQueue.RetentionContextRequired);
    }).pipe(TestRuntime.runPromise));

  test("retention relationships stop at the holder's configured cap", () =>
    Effect.gen(function* () {
      const parentDefinition = Task.make({
        name: "tq-ctx-limit-parent",
        payload: { userId: Schema.String, amount: Schema.Number },
        success: Schema.String,
        error: Schema.Struct({ reason: Schema.String }),
        storageLimits: { maxRelationships: 1 },
        idempotencyKey: (payload) => payload.userId,
      });
      const parent = TaskQueue.make(parentDefinition.name, parentDefinition);
      const first = makeQueue("tq-ctx-limit-first");
      const second = makeQueue("tq-ctx-limit-second");
      yield* TaskQueue.offer(parent, { userId: "parent", amount: 1 });

      let limitError: unknown;
      yield* TaskQueue.complete(parent, () =>
        Effect.gen(function* () {
          yield* TaskQueue.offer(
            first,
            { userId: "first", amount: 1 },
            { retainResultUntil: "current-task-settles" },
          );
          limitError = yield* TaskQueue.offer(
            second,
            { userId: "second", amount: 1 },
            { retainResultUntil: "current-task-settles" },
          ).pipe(Effect.flip);
          return "ok";
        }).pipe(Effect.orDie),
      );

      expect(limitError).toMatchObject({
        _tag: "StorageCountLimitExceeded",
        resource: "relationships",
        scope: "holder",
        actualCount: 1,
        maxCount: 1,
      });
    }).pipe(TestRuntime.runPromise));
});
