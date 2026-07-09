import { Effect } from "effect";
import { describe, expect, test } from "vitest";
import { TaskEngine } from "./index.js";
import { TestRuntime } from "./testing/redisLayer.js";

const baseTask = (prefix: string, id: string) =>
  ({
    prefix,
    id,
    name: `task-${id}`,
    payload: null,
    delay: 0,
    maxRetries: 0,
    onSuccessPolicy: "delete",
    onFailurePolicy: "delete",
  }) as const;

describe("TaskEngine pinning — acquisition", () => {
  test("child created with a holder pins itself and registers on the holder", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-acquire";
      yield* engine.createTask(baseTask(prefix, "a"));
      const child = yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });

      expect(child.refCount).toBe(1);
      expect(child.refs).toEqual([]);
      const holder = yield* engine.getTask(prefix, "a");
      expect(holder?.refs).toEqual([
        { prefix: `~effectmq:${prefix}`, id: "b" },
      ]);
      expect(holder?.refCount).toBe(0);
    }).pipe(TestRuntime.runPromise));

  test("cross-queue holder works identically", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      yield* engine.createTask(baseTask("pin-xq-a", "a"));
      const child = yield* engine.createTask({
        ...baseTask("pin-xq-b", "b"),
        heldBy: [{ prefix: "pin-xq-a", id: "a" }],
      });

      expect(child.refCount).toBe(1);
      const holder = yield* engine.getTask("pin-xq-a", "a");
      expect(holder?.refs).toEqual([{ prefix: "~effectmq:pin-xq-b", id: "b" }]);
    }).pipe(TestRuntime.runPromise));

  test("multiple holders each register the child once", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-multi";
      yield* engine.createTask(baseTask(prefix, "a1"));
      yield* engine.createTask(baseTask(prefix, "a2"));
      const child = yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [
          { prefix, id: "a1" },
          { prefix, id: "a2" },
        ],
      });

      expect(child.refCount).toBe(2);
      const a1 = yield* engine.getTask(prefix, "a1");
      const a2 = yield* engine.getTask(prefix, "a2");
      expect(a1?.refs).toEqual([{ prefix: `~effectmq:${prefix}`, id: "b" }]);
      expect(a2?.refs).toEqual([{ prefix: `~effectmq:${prefix}`, id: "b" }]);
    }).pipe(TestRuntime.runPromise));

  test("missing holder rejects creation without creating the task", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-missing-holder";
      const error = yield* engine
        .createTask({
          ...baseTask(prefix, "b"),
          heldBy: [{ prefix, id: "ghost" }],
        })
        .pipe(Effect.flip);

      expect(error._tag).toBe("TaskEngineError");
      // TaskEngineError.cause is a RedisError wrapping the script's ReplyError
      expect(String((error.cause as { cause: unknown }).cause)).toContain(
        "holder not found",
      );
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("idempotent re-offer skips acquisition — no double-pin", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-replay";
      yield* engine.createTask(baseTask(prefix, "a"));
      const insert = {
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      };
      yield* engine.createTask(insert);
      const again = yield* engine.createTask(insert);

      expect(again.refCount).toBe(1);
      const holder = yield* engine.getTask(prefix, "a");
      expect(holder?.refs).toEqual([
        { prefix: `~effectmq:${prefix}`, id: "b" },
      ]);
    }).pipe(TestRuntime.runPromise));
});

describe("TaskEngine pinning — createdBy provenance", () => {
  test("createdBy is stored, returned, and unaffected by creator deletion", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-created-by";
      yield* engine.createTask(baseTask(prefix, "a"));
      const child = yield* engine.createTask({
        ...baseTask(prefix, "b"),
        createdBy: { prefix, id: "a" },
      });
      expect(child.createdBy).toEqual({
        prefix: `~effectmq:${prefix}`,
        id: "a",
      });
      // provenance is pure metadata: no pin was acquired
      expect(child.refCount).toBe(0);

      yield* engine.removeTask(prefix, "a");
      const orphan = yield* engine.getTask(prefix, "b");
      expect(orphan?.createdBy).toEqual({
        prefix: `~effectmq:${prefix}`,
        id: "a",
      });
    }).pipe(TestRuntime.runPromise));

  test("createdBy is not validated against existing tasks", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-created-by-ghost";
      const task = yield* engine.createTask({
        ...baseTask(prefix, "b"),
        createdBy: { prefix, id: "never-existed" },
      });
      expect(task.createdBy).toEqual({
        prefix: `~effectmq:${prefix}`,
        id: "never-existed",
      });
    }).pipe(TestRuntime.runPromise));
});
