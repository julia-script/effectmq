import { Effect } from "effect";
import { describe, expect, test } from "vitest";
import { TaskEngine } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

// writeError serializes its argument once (JSON.stringify), so pass a tagged
// error object; the engine stores the decoded object in the task's errors list.
const stalled = (timestamp: number) =>
  ({ _tag: "~effectmq/Error/Stalled", timestamp }) as unknown as string;
const canceled = (timestamp: number) =>
  ({ _tag: "~effectmq/Error/Canceled", timestamp }) as unknown as string;

describe("TaskEngine", () => {
  test("success happy path with delete on success policy", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "task-engine-test";
      const task = yield* taskEngine.createTask({
        id: "123",
        name: "task name",
        payload: "task payload",
        delay: 0,
        maxRetries: 0,
        onSuccessPolicy: "delete",
        onFailurePolicy: "delete",
        prefix,
      });

      expect(task).toMatchInlineSnapshot(`
        {
          "createdAt": 2001-09-09T01:46:40.000Z,
          "delay": 0,
          "errors": [],
          "id": "123",
          "maxRetries": 0,
          "name": "task name",
          "onFailurePolicy": "delete",
          "onSuccessPolicy": "delete",
          "payload": "task payload",
          "updatedAt": 2001-09-09T01:46:40.000Z,
        }
      `);

      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [],
          "success": [],
          "wait": [
            "123",
          ],
        }
      `);

      yield* taskEngine.createTask({
        id: "456",
        name: "task name 2",
        payload: "task payload 2",
        delay: 1000,
        maxRetries: 0,
        onSuccessPolicy: "delete",
        onFailurePolicy: "delete",
        prefix,
      });

      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [
            "456",
          ],
          "success": [],
          "wait": [
            "123",
          ],
        }
      `);

      yield* TaskEngine.stepMockTime(999);
      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [
            "456",
          ],
          "success": [],
          "wait": [
            "123",
          ],
        }
      `);
      yield* TaskEngine.stepMockTime(1);
      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [],
          "success": [],
          "wait": [
            "123",
            "456",
          ],
        }
      `);

      const taken = yield* taskEngine.takeTask(prefix, 1000);
      expect(taken).toMatchInlineSnapshot(`
        {
          "createdAt": 2001-09-09T01:46:40.000Z,
          "delay": 0,
          "errors": [],
          "id": "123",
          "maxRetries": 0,
          "name": "task name",
          "onFailurePolicy": "delete",
          "onSuccessPolicy": "delete",
          "payload": "task payload",
          "updatedAt": 2001-09-09T01:46:40.000Z,
        }
      `);

      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [
            "123",
          ],
          "failed": [],
          "scheduled": [],
          "success": [],
          "wait": [
            "456",
          ],
        }
      `);

      yield* taskEngine.writeSuccess(prefix, taken?.id ?? "", "success");
      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [],
          "success": [],
          "wait": [
            "456",
          ],
        }
      `);
    }).pipe(TestRuntime.runPromise));

  test("failure retries until maxRetries then applies onFailurePolicy", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "fail-retry";
      yield* taskEngine.createTask({
        id: "r1",
        name: "retry task",
        payload: "p",
        delay: 0,
        maxRetries: 2,
        onSuccessPolicy: "delete",
        onFailurePolicy: "mark-as-failure",
        prefix,
      });

      // first failure: one error < maxRetries(2) => back to wait
      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "r1", stalled(1));
      let lists = yield* getLists(prefix);
      expect(lists.wait).toEqual(["r1"]);
      expect(lists.failed).toEqual([]);
      let task = yield* taskEngine.getTask(prefix, "r1");
      expect(task?.errors).toHaveLength(1);

      // second failure: errors(2) not < maxRetries(2) => terminal, failure policy
      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "r1", stalled(2));
      lists = yield* getLists(prefix);
      expect(lists.wait).toEqual([]);
      expect(lists.active).toEqual([]);
      expect(lists.failed).toEqual(["r1"]);
      task = yield* taskEngine.getTask(prefix, "r1");
      expect(task?.errors).toHaveLength(2);
    }).pipe(TestRuntime.runPromise));

  test("Canceled error skips retries and applies onFailurePolicy immediately", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "fail-canceled";
      yield* taskEngine.createTask({
        id: "c1",
        name: "cancel task",
        payload: "p",
        delay: 0,
        maxRetries: 5,
        onSuccessPolicy: "delete",
        onFailurePolicy: "mark-as-failure",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "c1", canceled(1000000000000));

      const lists = yield* getLists(prefix);
      expect(lists.wait).toEqual([]);
      expect(lists.failed).toEqual(["c1"]);
    }).pipe(TestRuntime.runPromise));

  test("onFailurePolicy: delete removes task entirely", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "fail-delete";
      yield* taskEngine.createTask({
        id: "d1",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 0,
        onSuccessPolicy: "delete",
        onFailurePolicy: "delete",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "d1", stalled(1));

      expect(yield* getLists(prefix)).toMatchInlineSnapshot(`
        {
          "active": [],
          "failed": [],
          "scheduled": [],
          "success": [],
          "wait": [],
        }
      `);
      expect(yield* taskEngine.getTask(prefix, "d1")).toBeNull();
    }).pipe(TestRuntime.runPromise));

  test("onFailurePolicy: keep removes from lists but keeps task", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "fail-keep";
      yield* taskEngine.createTask({
        id: "k1",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 0,
        onSuccessPolicy: "delete",
        onFailurePolicy: "keep",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "k1", stalled(1));

      const lists = yield* getLists(prefix);
      expect(lists.wait).toEqual([]);
      expect(lists.active).toEqual([]);
      expect(lists.failed).toEqual([]);
      const task = yield* taskEngine.getTask(prefix, "k1");
      expect(task?.id).toBe("k1");
      expect(task?.errors).toHaveLength(1);
    }).pipe(TestRuntime.runPromise));

  test("onSuccessPolicy: mark-as-success adds to success list and keeps task", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "success-mark";
      yield* taskEngine.createTask({
        id: "s1",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 0,
        onSuccessPolicy: "mark-as-success",
        onFailurePolicy: "delete",
        prefix,
      });

      const taken = yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeSuccess(prefix, taken?.id ?? "", "ok");

      const lists = yield* getLists(prefix);
      expect(lists.success).toEqual(["s1"]);
      expect(lists.active).toEqual([]);
      const task = yield* taskEngine.getTask(prefix, "s1");
      expect(task?.id).toBe("s1");
    }).pipe(TestRuntime.runPromise));

  test("onSuccessPolicy: keep removes from lists but keeps task", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "success-keep";
      yield* taskEngine.createTask({
        id: "sk1",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 0,
        onSuccessPolicy: "keep",
        onFailurePolicy: "delete",
        prefix,
      });

      const taken = yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeSuccess(prefix, taken?.id ?? "", "ok");

      const lists = yield* getLists(prefix);
      expect(lists.success).toEqual([]);
      expect(lists.active).toEqual([]);
      expect(lists.wait).toEqual([]);
      const task = yield* taskEngine.getTask(prefix, "sk1");
      expect(task?.id).toBe("sk1");
    }).pipe(TestRuntime.runPromise));
});
