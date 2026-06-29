import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { TaskEngine } from "./index.js";
import { getLists, TestRuntime } from "./testing/redisLayer.js";

describe("TaskEngine", () => {
  it("should create a task engine", () =>
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
});
