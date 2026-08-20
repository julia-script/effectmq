import { Effect } from "effect";
import { describe, expect, test } from "vitest";
import { RedisPool, TaskEngine } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

const baseTask = (
  prefix: string,
  id: string,
  options?: {
    onSuccessPolicy?: "delete" | "keep" | "mark-as-success";
    onFailurePolicy?: "delete" | "keep" | "mark-as-failure";
  },
) =>
  ({
    prefix,
    id,
    name: `task-${id}`,
    payload: null,
    delay: 0,
    maxRetries: 0,
    onSuccessPolicy: options?.onSuccessPolicy ?? "delete",
    onFailurePolicy: options?.onFailurePolicy ?? "delete",
  }) as const;

const identity = (queue: string, id: string, generation = 1) => ({
  queue,
  id,
  generation,
});

const requireAttempt = (attempt: TaskEngine.TaskAttempt | null) => {
  expect(attempt).not.toBeNull();
  if (attempt === null) throw new Error("Expected an acquired task attempt");
  return attempt;
};

const succeedNext = (
  engine: TaskEngine.TaskEngineService,
  prefix: string,
  result = "ok",
) =>
  Effect.gen(function* () {
    const attempt = requireAttempt(yield* engine.takeTask(prefix, 30_000));
    yield* engine.writeSuccess(
      prefix,
      attempt.task.id,
      attempt.leaseToken,
      result,
    );
    return attempt.task;
  });

const relationKeys = (
  holderQueue: string,
  holderId: string,
  retainedQueue: string,
  retainedId: string,
) => ({
  holders: `~effectmq:v1:${retainedQueue}:task:${retainedId}:1:retained-by`,
  retains: `~effectmq:v1:${holderQueue}:task:${holderId}:1:retains`,
});

describe("TaskEngine result-retention relationships", () => {
  test("replaying the same holder/retained generation creates one hold", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      const holderQueue = "retention-replay-holder";
      const retainedQueue = "retention-replay-task";
      yield* engine.createTask(baseTask(holderQueue, "holder"));
      const insert = {
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity(holderQueue, "holder"),
      };

      yield* engine.offerTask(insert);
      const replay = yield* engine.offerTask(insert);
      expect(replay.status).toBe("existing");

      const keys = relationKeys(
        holderQueue,
        "holder",
        retainedQueue,
        "retained",
      );
      expect(yield* redis.send("SCARD", keys.holders)).toBe(1);
      expect(yield* redis.send("SCARD", keys.retains)).toBe(1);
    }).pipe(TestRuntime.runPromise));

  test("independent live holders can retain the same existing generation", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      const retainedQueue = "retention-multiple-task";
      yield* engine.createTask(baseTask("retention-holder-a", "a"));
      yield* engine.createTask(baseTask("retention-holder-b", "b"));
      yield* engine.offerTask({
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity("retention-holder-a", "a"),
      });
      const existing = yield* engine.offerTask({
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity("retention-holder-b", "b"),
      });

      expect(existing.status).toBe("existing");
      const holdersKey = `~effectmq:v1:${retainedQueue}:task:retained:1:retained-by`;
      expect(yield* redis.send("SCARD", holdersKey)).toBe(2);

      yield* succeedNext(engine, retainedQueue, "result");
      expect((yield* engine.getTask(retainedQueue, "retained"))?.outcome).toBe(
        "success",
      );

      yield* succeedNext(engine, "retention-holder-a");
      expect(yield* engine.getTask(retainedQueue, "retained")).not.toBeNull();
      expect(yield* redis.send("SCARD", holdersKey)).toBe(1);

      yield* succeedNext(engine, "retention-holder-b");
      expect(yield* engine.getTask(retainedQueue, "retained")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("a settled holder is rejected without creating the retained task", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const holderQueue = "retention-settled-holder";
      const retainedQueue = "retention-settled-task";
      yield* engine.createTask(
        baseTask(holderQueue, "holder", { onSuccessPolicy: "keep" }),
      );
      yield* succeedNext(engine, holderQueue);

      const error = yield* engine
        .offerTask({
          ...baseTask(retainedQueue, "retained"),
          retentionHolder: identity(holderQueue, "holder"),
        })
        .pipe(Effect.flip);

      expect(String((error.cause as { cause: unknown }).cause)).toContain(
        "retention holder is settled",
      );
      expect(yield* engine.getTask(retainedQueue, "retained")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("holder removal releases its hold without affecting runnable work", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const redis = yield* RedisPool.RedisPool;
      const holderQueue = "retention-remove-holder";
      const retainedQueue = "retention-remove-task";
      yield* engine.createTask(baseTask(holderQueue, "holder"));
      yield* engine.createTask({
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity(holderQueue, "holder"),
      });

      yield* engine.removeTask(holderQueue, "holder");
      const holdersKey = `~effectmq:v1:${retainedQueue}:task:retained:1:retained-by`;
      expect(yield* redis.send("SCARD", holdersKey)).toBe(0);
      expect((yield* getLists(retainedQueue)).wait).toEqual(["retained"]);
    }).pipe(TestRuntime.runPromise));

  test("terminal settlement is visible immediately while delete waits for holds", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const holderQueue = "retention-visible-holder";
      const retainedQueue = "retention-visible-task";
      yield* engine.createTask(baseTask(holderQueue, "holder"));
      yield* engine.createTask({
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity(holderQueue, "holder"),
      });

      yield* succeedNext(engine, retainedQueue, "visible-result");
      const retained = yield* engine.getTask(retainedQueue, "retained");
      expect(retained).toMatchObject({
        outcome: "success",
        success: "visible-result",
      });
      expect(yield* getLists(retainedQueue)).toMatchObject({
        active: [],
        wait: [],
      });

      yield* succeedNext(engine, holderQueue);
      expect(yield* engine.getTask(retainedQueue, "retained")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("mark policy indexes a held terminal task immediately", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const holderQueue = "retention-mark-holder";
      const retainedQueue = "retention-mark-task";
      yield* engine.createTask(baseTask(holderQueue, "holder"));
      yield* engine.createTask({
        ...baseTask(retainedQueue, "retained", {
          onSuccessPolicy: "mark-as-success",
        }),
        retentionHolder: identity(holderQueue, "holder"),
      });

      yield* succeedNext(engine, retainedQueue);
      expect((yield* getLists(retainedQueue)).success).toEqual(["retained"]);
      expect(yield* engine.getTask(retainedQueue, "retained")).not.toBeNull();

      yield* succeedNext(engine, holderQueue);
      expect((yield* getLists(retainedQueue)).success).toEqual(["retained"]);
      expect(yield* engine.getTask(retainedQueue, "retained")).not.toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("ordinary removal rejects holds while force removal is explicit", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const holderQueue = "retention-force-holder";
      const retainedQueue = "retention-force-task";
      yield* engine.createTask(baseTask(holderQueue, "holder"));
      yield* engine.createTask({
        ...baseTask(retainedQueue, "retained"),
        retentionHolder: identity(holderQueue, "holder"),
      });

      const error = yield* engine
        .removeTask(retainedQueue, "retained")
        .pipe(Effect.flip);
      expect(String((error.cause as { cause: unknown }).cause)).toContain(
        "active retention holds",
      );

      yield* engine.forceRemoveTask(retainedQueue, "retained");
      expect(yield* engine.getTask(retainedQueue, "retained")).toBeNull();
      yield* engine.removeTask(holderQueue, "holder");
    }).pipe(TestRuntime.runPromise));

  test("large holder release leaves and drains a durable bounded continuation", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.make({
        debugMode: true,
        maintenanceBatchSize: 64,
      });
      const redis = yield* RedisPool.RedisPool;
      const prefix = "retention-bounded";
      yield* engine.createTask(baseTask(prefix, "holder"));
      for (let index = 0; index < 65; index++) {
        yield* engine.createTask({
          ...baseTask(prefix, `retained-${index}`),
          retentionHolder: identity(prefix, "holder"),
        });
      }

      yield* engine.removeTask(prefix, "holder");
      const continuationKey = `~effectmq:v1:${prefix}:retention-release-continuations`;
      expect(yield* redis.send("ZCARD", continuationKey)).toBe(1);

      // Any subsequent maintenance-bearing operation drains another bounded
      // batch, even though the holder record itself has already been removed.
      yield* engine.listTasks(prefix, "wait");
      expect(yield* redis.send("ZCARD", continuationKey)).toBe(0);
      expect(
        yield* redis.send(
          "SCARD",
          `~effectmq:v1:${prefix}:task:retained-64:1:retained-by`,
        ),
      ).toBe(0);
    }).pipe(TestRuntime.runPromise));
});

describe("TaskEngine creator provenance", () => {
  test("creator identity is immutable metadata and survives creator removal", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "creator-provenance";
      yield* engine.createTask(baseTask(prefix, "creator"));
      const spawned = yield* engine.createTask({
        ...baseTask(prefix, "spawned"),
        creator: identity(prefix, "creator"),
      });

      expect(spawned.creator).toEqual({
        queue: `~effectmq:v1:${prefix}`,
        id: "creator",
        generation: 1,
      });
      yield* engine.removeTask(prefix, "creator");
      expect((yield* engine.getTask(prefix, "spawned"))?.creator).toEqual(
        spawned.creator,
      );
      expect((yield* getLists(prefix)).wait).toEqual(["spawned"]);
    }).pipe(TestRuntime.runPromise));
});
