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

  // The engine routes purely on the `retryAt` it is handed: the retry/cap
  // decision lives in TaskQueue.fail. These cover the routing contract.
  test("writeError with a future retryAt schedules the task", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      const now = 1000000000000;
      yield* TaskEngine.setMockTime(now);
      const prefix = "retry-scheduled";
      yield* taskEngine.createTask({
        id: "r1",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 5,
        onSuccessPolicy: "delete",
        onFailurePolicy: "mark-as-failure",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "r1", stalled(1), now + 5000);

      const lists = yield* getLists(prefix);
      expect(lists.scheduled).toEqual(["r1"]);
      expect(lists.wait).toEqual([]);
      expect(lists.failed).toEqual([]);
      const task = yield* taskEngine.getTask(prefix, "r1");
      expect(task?.errors).toHaveLength(1);
      expect(task?.errors[0].retryAt).toBe(now + 5000);
    }).pipe(TestRuntime.runPromise));

  test("writeError with a past retryAt returns the task to wait", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      const now = 1000000000000;
      yield* TaskEngine.setMockTime(now);
      const prefix = "retry-wait";
      yield* taskEngine.createTask({
        id: "r2",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 5,
        onSuccessPolicy: "delete",
        onFailurePolicy: "mark-as-failure",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "r2", stalled(1), now - 1);

      const lists = yield* getLists(prefix);
      expect(lists.wait).toEqual(["r2"]);
      expect(lists.scheduled).toEqual([]);
      expect(lists.failed).toEqual([]);
    }).pipe(TestRuntime.runPromise));

  test("writeError without a retryAt applies the failure policy immediately", () =>
    Effect.gen(function* () {
      const taskEngine = yield* TaskEngine.TaskEngine;
      yield* TaskEngine.setMockTime(1000000000000);
      const prefix = "retry-none";
      yield* taskEngine.createTask({
        id: "r3",
        name: "t",
        payload: "p",
        delay: 0,
        maxRetries: 5,
        onSuccessPolicy: "delete",
        onFailurePolicy: "mark-as-failure",
        prefix,
      });

      yield* taskEngine.takeTask(prefix, 1000);
      yield* taskEngine.writeError(prefix, "r3", stalled(1));

      const lists = yield* getLists(prefix);
      expect(lists.failed).toEqual(["r3"]);
      expect(lists.wait).toEqual([]);
      expect(lists.scheduled).toEqual([]);
      const task = yield* taskEngine.getTask(prefix, "r3");
      expect(task?.errors[0].retryAt).toBeUndefined();
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
