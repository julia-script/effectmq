import { Effect } from "effect";
import { describe, expect, test } from "vitest";
import { TaskEngine } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

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

const boom = { _tag: "boom" };

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

describe("TaskEngine pinning — lifecycle (death, deferral, cascade)", () => {
  test("pinned delete-policy task defers deletion until its holder dies", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-defer-delete";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);

      // B completes while pinned: record survives in limbo, outcome recorded
      yield* engine.writeSuccess(prefix, "b", "b-result");
      const limbo = yield* engine.getTask(prefix, "b");
      expect(limbo?.outcome).toBe("success");
      expect(limbo?.dead).toBeUndefined();
      const lists = yield* getLists(prefix);
      expect(Object.values(lists).flat()).not.toContain("b");

      // A dies -> releases B -> B dies -> both records deleted
      yield* engine.writeSuccess(prefix, "a", "a-result");
      expect(yield* engine.getTask(prefix, "a")).toBeNull();
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("pinned mark-as-success enters the success list only at death", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-defer-mark";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        onSuccessPolicy: "mark-as-success",
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);

      yield* engine.writeSuccess(prefix, "b", "b-result");
      expect((yield* getLists(prefix)).success).not.toContain("b");

      yield* engine.writeSuccess(prefix, "a", "a-result");
      expect((yield* getLists(prefix)).success).toContain("b");
      const deadB = yield* engine.getTask(prefix, "b");
      expect(deadB?.dead).toBe(true);
      expect(deadB?.refs).toEqual([]);
    }).pipe(TestRuntime.runPromise));

  test("pinned keep-policy task is retained after death, marked dead", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-defer-keep";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        onSuccessPolicy: "keep",
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);
      yield* engine.writeSuccess(prefix, "b", "b-result");
      yield* engine.writeSuccess(prefix, "a", "a-result");

      const deadB = yield* engine.getTask(prefix, "b");
      expect(deadB?.dead).toBe(true);
      expect(deadB?.outcome).toBe("success");
      expect(Object.values(yield* getLists(prefix)).flat()).not.toContain("b");
    }).pipe(TestRuntime.runPromise));

  test("pinned terminal failure defers the failure policy until release", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-defer-fail";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        onFailurePolicy: "mark-as-failure",
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);

      // no retryAt -> terminal failure; pinned -> limbo with errors intact
      yield* engine.writeError(prefix, "b", boom);
      const limbo = yield* engine.getTask(prefix, "b");
      expect(limbo?.outcome).toBe("failure");
      expect(limbo?.errors).toHaveLength(1);
      expect((yield* getLists(prefix)).failed).not.toContain("b");

      yield* engine.writeSuccess(prefix, "a", "a-result");
      expect((yield* getLists(prefix)).failed).toContain("b");
      expect((yield* engine.getTask(prefix, "b"))?.dead).toBe(true);
    }).pipe(TestRuntime.runPromise));

  test("death cascades through a pipeline (A holds B holds C)", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-cascade";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.createTask({
        ...baseTask(prefix, "c"),
        heldBy: [{ prefix, id: "b" }],
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);
      yield* engine.takeTask(prefix, 30000);

      yield* engine.writeSuccess(prefix, "c", "c-result");
      yield* engine.writeSuccess(prefix, "b", "b-result");
      // all three parked or waiting on A; one completion tears it all down
      expect(yield* engine.getTask(prefix, "c")).not.toBeNull();
      yield* engine.writeSuccess(prefix, "a", "a-result");
      expect(yield* engine.getTask(prefix, "a")).toBeNull();
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
      expect(yield* engine.getTask(prefix, "c")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("cross-queue release applies the child's policy in its own queue", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const qa = "pin-xq-die-a";
      const qb = "pin-xq-die-b";
      yield* engine.createTask(baseTask(qa, "a"));
      yield* engine.createTask({
        ...baseTask(qb, "b"),
        onSuccessPolicy: "mark-as-success",
        heldBy: [{ prefix: qa, id: "a" }],
      });
      yield* engine.takeTask(qa, 30000);
      yield* engine.takeTask(qb, 30000);
      yield* engine.writeSuccess(qb, "b", "b-result");
      expect((yield* getLists(qb)).success).not.toContain("b");

      // A's death in qa releases and kills B in qb
      yield* engine.writeSuccess(qa, "a", "a-result");
      expect((yield* getLists(qb)).success).toContain("b");
      expect((yield* engine.getTask(qb, "b"))?.dead).toBe(true);
    }).pipe(TestRuntime.runPromise));

  test("removeTask on an alive holder forces death and releases its children", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-force";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });
      yield* engine.takeTask(prefix, 30000); // a
      yield* engine.takeTask(prefix, 30000); // b
      yield* engine.writeSuccess(prefix, "b", "b-result"); // b done, pinned

      yield* engine.removeTask(prefix, "a");
      expect(yield* engine.getTask(prefix, "a")).toBeNull();
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("releasing a child that is not yet done leaves it running", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-release-alive";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });

      yield* engine.removeTask(prefix, "a");
      // B lost its holder but is not done: it stays queued...
      const b = yield* engine.getTask(prefix, "b");
      expect(b?.refCount).toBe(0);
      expect((yield* getLists(prefix)).wait).toContain("b");

      // ...and dies immediately on completion, now unpinned
      yield* engine.takeTask(prefix, 30000);
      yield* engine.writeSuccess(prefix, "b", "b-result");
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("done-but-pinned holder is still a valid holder", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-limbo-holder";
      yield* engine.createTask(baseTask(prefix, "root"));
      yield* engine.createTask({
        ...baseTask(prefix, "a"),
        heldBy: [{ prefix, id: "root" }],
      });
      yield* engine.takeTask(prefix, 30000); // root
      yield* engine.takeTask(prefix, 30000); // a
      yield* engine.writeSuccess(prefix, "a", "a-result");
      // a is done but pinned by root: alive, so it can still hold
      const c = yield* engine.createTask({
        ...baseTask(prefix, "c"),
        heldBy: [{ prefix, id: "a" }],
      });
      expect(c.refCount).toBe(1);

      // root dies -> a dies -> c releases and (not done) keeps running
      yield* engine.writeSuccess(prefix, "root", "root-result");
      expect(yield* engine.getTask(prefix, "a")).toBeNull();
      expect((yield* engine.getTask(prefix, "c"))?.refCount).toBe(0);
    }).pipe(TestRuntime.runPromise));

  test("fan-out: holder death releases all children at once", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-fanout";
      yield* engine.createTask(baseTask(prefix, "a"));
      for (const id of ["b", "c", "d"]) {
        yield* engine.createTask({
          ...baseTask(prefix, id),
          heldBy: [{ prefix, id: "a" }],
        });
      }
      for (let i = 0; i < 4; i++) yield* engine.takeTask(prefix, 30000);
      yield* engine.writeSuccess(prefix, "b", "r");
      yield* engine.writeSuccess(prefix, "c", "r");
      yield* engine.writeSuccess(prefix, "d", "r");
      yield* engine.writeSuccess(prefix, "a", "r");

      for (const id of ["a", "b", "c", "d"]) {
        expect(yield* engine.getTask(prefix, id)).toBeNull();
      }
    }).pipe(TestRuntime.runPromise));

  test("removeTask on a pinned task is illegal until its holders are gone", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-remove-pinned";
      yield* engine.createTask(baseTask(prefix, "a"));
      yield* engine.createTask({
        ...baseTask(prefix, "b"),
        heldBy: [{ prefix, id: "a" }],
      });

      const error = yield* engine.removeTask(prefix, "b").pipe(Effect.flip);
      expect(String((error.cause as { cause: unknown }).cause)).toContain(
        "task is pinned",
      );
      expect(yield* engine.getTask(prefix, "b")).not.toBeNull();

      // removing the holder releases B; B (alive, unpinned) is then removable
      yield* engine.removeTask(prefix, "a");
      yield* engine.removeTask(prefix, "b");
      expect(yield* engine.getTask(prefix, "b")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("dead retained holder is rejected and its removal releases nothing", () =>
    Effect.gen(function* () {
      const engine = yield* TaskEngine.TaskEngine;
      const prefix = "pin-dead-holder";
      yield* engine.createTask({
        ...baseTask(prefix, "a"),
        onSuccessPolicy: "keep",
      });
      yield* engine.takeTask(prefix, 30000);
      yield* engine.writeSuccess(prefix, "a", "a-result");
      // a is dead but retained (keep policy)
      expect((yield* engine.getTask(prefix, "a"))?.dead).toBe(true);

      const error = yield* engine
        .createTask({
          ...baseTask(prefix, "b"),
          heldBy: [{ prefix, id: "a" }],
        })
        .pipe(Effect.flip);
      expect(String((error.cause as { cause: unknown }).cause)).toContain(
        "holder is dead",
      );
      expect(yield* engine.getTask(prefix, "b")).toBeNull();

      // removing the inert record is a plain deletion
      yield* engine.removeTask(prefix, "a");
      expect(yield* engine.getTask(prefix, "a")).toBeNull();
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
