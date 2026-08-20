import { expect, layer } from "@effect/vitest";
import { Duration, Effect, Metric } from "effect";
import { Packr } from "msgpackr";
import { Observability, RedisPool, TaskEngine } from "./index.js";
import { getLists, TestLayer } from "./testing/redisLayer.js";
import {
  extendLock,
  takeTask,
  writeError,
  writeSuccess,
} from "./testing/TaskAttemptHarness.js";

// The harness writes protocol-level errors directly so low-level engine tests
// can exercise built-in transitions independently of typed queue schemas.
const stalled = (timestamp: number) => ({
  _tag: "~effectmq/Error/Stalled",
  timestamp,
});
const canceled = (timestamp: number) => ({
  _tag: "~effectmq/Error/Canceled",
  timestamp,
});

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "TaskEngine (real Redis time)",
  (it) => {
    it.effect(
      "rejects maintenance batches above the supported atomic bound",
      () =>
        Effect.gen(function* () {
          const error = yield* TaskEngine.make({
            maintenanceBatchSize: 1_001,
          }).pipe(Effect.flip);
          expect(error).toMatchObject({
            _tag: "TaskEngineConfigurationError",
            field: "maintenanceBatchSize",
            actual: 1_001,
          });
        }),
    );

    it.effect(
      "rejects unsafe low-level numeric inputs before Redis mutation",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const prefix = "invalid-low-level-numbers";
          const error = yield* engine
            .createTask({
              id: "invalid",
              name: "invalid",
              payload: null,
              delay: Number.NaN,
              maxRetries: 0,
              onSuccessPolicy: "keep",
              onFailurePolicy: "keep",
              prefix,
            })
            .pipe(Effect.flip);
          expect(error.reason).toMatchObject({
            _tag: "InvalidInput",
            field: "delay",
          });
          expect(yield* engine.getGeneration(prefix, "invalid")).toBe(0);

          const lockError = yield* engine
            .takeTask(prefix, 1.5)
            .pipe(Effect.flip);
          expect(lockError.reason).toMatchObject({
            _tag: "InvalidInput",
            field: "lockTimeout",
          });

          yield* engine.createTask({
            id: "retry-at",
            name: "retry-at",
            payload: null,
            delay: 0,
            maxRetries: 1,
            onSuccessPolicy: "keep",
            onFailurePolicy: "keep",
            prefix,
          });
          yield* takeTask(engine, prefix, 1_000);
          const retryError = yield* writeError(
            engine,
            prefix,
            "retry-at",
            "failure",
            Duration.infinity,
          ).pipe(Effect.flip);
          expect(retryError).toMatchObject({
            _tag: "TaskEngineError",
            reason: { _tag: "InvalidInput", field: "retryAt" },
          });
          expect((yield* getLists(prefix)).active).toEqual(["retry-at"]);
        }),
    );

    it.effect("task inspection is capped, cursor-paginated, and ordered", () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const prefix = "paginated-inspection";
        yield* TaskEngine.setMockTime(500_000);
        for (const id of ["first", "second", "third", "fourth", "fifth"]) {
          yield* engine.createTask({
            prefix,
            id,
            name: "inspection",
            payload: null,
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "keep",
            onFailurePolicy: "keep",
          });
        }

        const first = yield* engine.listTasks(prefix, "wait", { limit: 2 });
        const second = yield* engine.listTasks(prefix, "wait", {
          cursor: first.nextCursor,
          limit: 2,
        });
        const third = yield* engine.listTasks(prefix, "wait", {
          cursor: second.nextCursor,
          limit: 2,
        });
        expect(first).toEqual({ items: ["first", "second"], nextCursor: "2" });
        expect(second).toEqual({ items: ["third", "fourth"], nextCursor: "4" });
        expect(third).toEqual({ items: ["fifth"], nextCursor: undefined });

        for (const [id, delay] of [
          ["late", 200],
          ["zeta", 100],
          ["alpha", 100],
        ] as const) {
          yield* engine.createTask({
            prefix,
            id,
            name: "scheduled inspection",
            payload: null,
            delay,
            maxRetries: 0,
            onSuccessPolicy: "keep",
            onFailurePolicy: "keep",
          });
        }
        expect(
          (yield* engine.listTasks(prefix, "scheduled", { limit: 10 })).items,
        ).toEqual(["alpha", "zeta", "late"]);

        const invalid = yield* engine
          .listTasks(prefix, "wait", { limit: 1_001 })
          .pipe(Effect.flip);
        expect(invalid).toMatchObject({
          _tag: "TaskEngineError",
          reason: {
            _tag: "InvalidReply",
            operation: "listTasks",
          },
        });
      }),
    );

    it.effect(
      "one maintenance pass promotes at most the configured due batch",
      () =>
        Effect.gen(function* () {
          const redis = yield* RedisPool.RedisPool;
          const engine = yield* TaskEngine.make({
            debugMode: true,
            maintenanceBatchSize: 2,
          });
          const prefix = "bounded-delayed";
          yield* TaskEngine.setMockTime(1_000_000);
          for (const id of ["one", "two", "three"]) {
            yield* engine.createTask({
              prefix,
              id,
              name: "bounded delayed",
              payload: null,
              delay: 1_000,
              maxRetries: 0,
              onSuccessPolicy: "keep",
              onFailurePolicy: "keep",
            });
          }
          yield* TaskEngine.stepMockTime(1_050);
          const health = yield* engine.maintain(prefix);

          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:scheduled`),
          ).toBe(1);
          expect(yield* redis.send("LLEN", `~effectmq:v1:${prefix}:wait`)).toBe(
            2,
          );
          expect(health).toMatchObject({
            depth: 3,
            dueBacklog: 1,
            oldestTaskAgeMs: 1_050,
            sweepLagMs: 50,
          });
          expect(
            yield* Metric.value(
              Metric.withAttributes(Observability.dueBacklog, {
                queue: prefix,
              }),
            ),
          ).toMatchObject({ value: 1 });
        }),
    );

    it.effect(
      "terminal records, results, indexes, and dead letters expire independently",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const prefix = "retention-windows";
          yield* TaskEngine.setMockTime(5_000_000);
          yield* engine.createTask({
            prefix,
            id: "success",
            name: "retained success",
            payload: null,
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "mark-as-success",
            onFailurePolicy: "delete",
            taskRecordRetentionMs: 300,
            resultRetentionMs: 100,
            terminalIndexRetentionMs: 200,
            deadLetterRetentionMs: 100,
          });
          const successAttempt = yield* takeTask(engine, prefix, 30_000);
          yield* writeSuccess(engine, prefix, successAttempt?.id ?? "", "ok");

          expect(yield* engine.getResult(prefix, "success", 1)).toMatchObject({
            generation: 1,
            outcome: "success",
            success: "ok",
          });
          expect((yield* getLists(prefix)).success).toEqual(["success"]);

          yield* TaskEngine.stepMockTime(101);
          yield* engine.maintain(prefix);
          expect(yield* engine.getResult(prefix, "success", 1)).toBeNull();
          expect(yield* engine.getTask(prefix, "success")).not.toBeNull();
          expect((yield* getLists(prefix)).success).toEqual(["success"]);

          yield* TaskEngine.stepMockTime(100);
          yield* engine.maintain(prefix);
          expect((yield* getLists(prefix)).success).toEqual([]);
          expect(yield* engine.getTask(prefix, "success")).not.toBeNull();

          yield* TaskEngine.stepMockTime(100);
          yield* engine.maintain(prefix);
          expect(yield* engine.getTask(prefix, "success")).toBeNull();

          yield* engine.createTask({
            prefix,
            id: "failure",
            name: "retained failure",
            payload: null,
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "delete",
            onFailurePolicy: "keep",
            taskRecordRetentionMs: 1_000,
            resultRetentionMs: 1_000,
            terminalIndexRetentionMs: 1_000,
            deadLetterRetentionMs: 100,
          });
          yield* takeTask(engine, prefix, 30_000);
          yield* writeError(engine, prefix, "failure", stalled(5_000_301));
          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:dead-letter`),
          ).toBe(1);
          expect(yield* engine.getResult(prefix, "failure", 1)).toMatchObject({
            outcome: "failure",
            failure: stalled(5_000_301),
          });

          yield* TaskEngine.stepMockTime(101);
          yield* engine.maintain(prefix);
          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:dead-letter`),
          ).toBe(0);
          expect(yield* engine.getResult(prefix, "failure", 1)).not.toBeNull();
        }),
    );

    it.effect(
      "retention cleanup leaves bounded due work for later sweeps",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.make({
            debugMode: true,
            maintenanceBatchSize: 2,
          });
          const redis = yield* RedisPool.RedisPool;
          const prefix = "bounded-retention-cleanup";
          yield* TaskEngine.setMockTime(6_000_000);
          for (const id of ["one", "two", "three"]) {
            yield* engine.createTask({
              prefix,
              id,
              name: "bounded retention",
              payload: null,
              delay: 0,
              maxRetries: 0,
              onSuccessPolicy: "mark-as-success",
              onFailurePolicy: "delete",
              taskRecordRetentionMs: 100,
              resultRetentionMs: 100,
              terminalIndexRetentionMs: 100,
              deadLetterRetentionMs: 100,
            });
            yield* takeTask(engine, prefix, 30_000);
            yield* writeSuccess(engine, prefix, id, id);
          }

          yield* TaskEngine.stepMockTime(101);
          let sawRetentionContinuation = false;
          for (let sweep = 0; sweep < 24; sweep++) {
            const health = yield* engine.maintain(prefix);
            expect(health.processed).toBeLessThanOrEqual(2);
            if (health.retentionBacklog > 0) sawRetentionContinuation = true;
          }
          expect(sawRetentionContinuation).toBe(true);
          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:expiry:tasks`),
          ).toBe(0);
          expect(
            yield* redis.send(
              "ZCARD",
              `~effectmq:v1:${prefix}:expiry:terminal-indexes`,
            ),
          ).toBe(0);
          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:success`),
          ).toBe(0);
          expect(
            yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:expiry:results`),
          ).toBe(0);

          const deadPrefix = "bounded-dead-letter-cleanup";
          for (const id of ["one", "two", "three"]) {
            yield* engine.createTask({
              prefix: deadPrefix,
              id,
              name: "bounded dead letter",
              payload: null,
              delay: 0,
              maxRetries: 0,
              onSuccessPolicy: "delete",
              onFailurePolicy: "keep",
              taskRecordRetentionMs: 1_000,
              resultRetentionMs: 1_000,
              terminalIndexRetentionMs: 1_000,
              deadLetterRetentionMs: 100,
            });
            yield* takeTask(engine, deadPrefix, 30_000);
            yield* writeError(engine, deadPrefix, id, stalled(6_000_301));
          }
          yield* TaskEngine.stepMockTime(101);
          let sawDeadLetterContinuation = false;
          for (let sweep = 0; sweep < 15; sweep++) {
            const health = yield* engine.maintain(deadPrefix);
            expect(health.processed).toBeLessThanOrEqual(2);
            if (health.retentionBacklog > 0) sawDeadLetterContinuation = true;
          }
          expect(sawDeadLetterContinuation).toBe(true);
          expect(
            yield* redis.send(
              "ZCARD",
              `~effectmq:v1:${deadPrefix}:dead-letter`,
            ),
          ).toBe(0);
          expect(
            yield* redis.send(
              "ZCARD",
              `~effectmq:v1:${deadPrefix}:expiry:dead-letter`,
            ),
          ).toBe(0);
        }),
    );

    it.effect(
      "corrupt non-list error history fails instead of normalizing to empty",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const prefix = "corrupt-error-history";
          yield* engine.createTask({
            prefix,
            id: "corrupt",
            name: "corrupt task",
            payload: null,
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "keep",
            onFailurePolicy: "keep",
          });
          const packr = new Packr({ useRecords: false });
          yield* redis.send(
            "HSET",
            `~effectmq:v1:${prefix}:task:corrupt`,
            "errors",
            packr.pack({ not: "a list" }),
          );

          const error = yield* engine
            .getTask(prefix, "corrupt")
            .pipe(Effect.flip);
          expect(error._tag).toBe("TaskEngineError");
          expect(error.reason).toMatchObject({
            _tag: "InvalidReply",
            operation: "decodeTask",
          });
        }),
    );
    it.effect("cached scripts recover independently after SCRIPT FLUSH", () =>
      Effect.gen(function* () {
        const redis = yield* RedisPool.RedisPool;
        const reloadsBefore = (yield* Metric.value(Observability.scriptReloads))
          .count;
        const versionA = 'return "engine-a:" .. ARGV[1]';
        const versionB = 'return "engine-b:" .. ARGV[1]';

        expect(yield* redis.evalScript(versionA, {}, "first")).toBe(
          "engine-a:first",
        );
        expect(yield* redis.evalScript(versionB, {}, "first")).toBe(
          "engine-b:first",
        );

        yield* redis.send("SCRIPT", "FLUSH");

        // Both calls begin with a locally cached digest. Each source must catch
        // its own NOSCRIPT, reload its own exact content, and retry once.
        expect(yield* redis.evalScript(versionB, {}, "after-flush")).toBe(
          "engine-b:after-flush",
        );
        expect(yield* redis.evalScript(versionA, {}, "after-flush")).toBe(
          "engine-a:after-flush",
        );
        expect((yield* Metric.value(Observability.scriptReloads)).count).toBe(
          reloadsBefore + 2,
        );
      }),
    );

    it.effect(
      "re-offering a waiting task preserves exactly one wait membership",
      () =>
        Effect.gen(function* () {
          const taskEngine = yield* TaskEngine.TaskEngine;
          const prefix = "same-state-wait";
          const task = {
            id: "waiting-1",
            name: "waiting task",
            payload: "first",
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "keep" as const,
            onFailurePolicy: "keep" as const,
            prefix,
          };

          yield* taskEngine.createTask(task);
          yield* taskEngine.createTask({ ...task, payload: "replayed" });

          expect((yield* getLists(prefix)).wait).toEqual([task.id]);
        }),
    );

    it.effect("renewing a lease preserves exactly one active membership", () =>
      Effect.gen(function* () {
        const taskEngine = yield* TaskEngine.TaskEngine;
        const redis = yield* RedisPool.RedisPool;
        const productionEngine = yield* TaskEngine.makeWithRedis(redis);
        const prefix = "same-state-active";
        yield* taskEngine.createTask({
          id: "active-1",
          name: "active task",
          payload: null,
          delay: 0,
          maxRetries: 0,
          onSuccessPolicy: "keep",
          onFailurePolicy: "keep",
          prefix,
        });
        yield* takeTask(productionEngine, prefix, 30_000);

        // Simulate a partially corrupted pre-fix record. Any transition must
        // repair cross-state membership rather than preserving extra indexes.
        const keyPrefix = `~effectmq:v1:${prefix}`;
        yield* redis.send("HDEL", `${keyPrefix}:task:active-1`, "currentList");
        yield* redis.send("RPUSH", `${keyPrefix}:wait`, "active-1");
        yield* redis.send(
          "ZADD",
          `${keyPrefix}:scheduled`,
          String(Number.MAX_SAFE_INTEGER),
          "active-1",
        );
        yield* redis.send("ZADD", `${keyPrefix}:failed`, "1", "active-1");
        yield* redis.send("ZADD", `${keyPrefix}:success`, "1", "active-1");

        yield* extendLock(productionEngine, prefix, "active-1", 30_000);

        expect(yield* getLists(prefix)).toEqual({
          active: ["active-1"],
          failed: [],
          scheduled: [],
          success: [],
          wait: [],
        });
      }),
    );

    it.effect("stale rolling-deployment list markers fall back to repair", () =>
      Effect.gen(function* () {
        const redis = yield* RedisPool.RedisPool;
        const engine = yield* TaskEngine.makeWithRedis(redis);
        const prefix = "stale-current-list";
        const keyPrefix = `~effectmq:v1:${prefix}`;
        yield* engine.createTask({
          id: "stale",
          name: "stale",
          payload: null,
          delay: 0,
          maxRetries: 0,
          onSuccessPolicy: "mark-as-success",
          onFailurePolicy: "mark-as-failure",
          prefix,
        });
        yield* takeTask(engine, prefix, 30_000);
        yield* writeSuccess(engine, prefix, "stale", "ok");
        yield* redis.send(
          "HSET",
          `${keyPrefix}:task:stale`,
          "currentList",
          "wait",
        );

        yield* engine.removeTask(prefix, "stale");

        expect(yield* getLists(prefix)).toEqual({
          active: [],
          failed: [],
          scheduled: [],
          success: [],
          wait: [],
        });
      }),
    );

    it.effect("success happy path with delete on success policy", () =>
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
          "attempt": 0,
          "createdAt": 2001-09-09T01:46:40.000Z,
          "deadLetterRetentionMs": 2592000000,
          "delay": 0,
          "errors": [],
          "eventRetentionMs": 604800000,
          "generation": 1,
          "handlerFailureCount": 0,
          "id": "123",
          "maxErrorEntries": 100,
          "maxEventEntries": 10000,
          "maxRelationships": 1000,
          "maxRetries": 0,
          "maxStalledCount": 1,
          "name": "task name",
          "onFailurePolicy": "delete",
          "onSuccessPolicy": "delete",
          "payload": "task payload",
          "protocolVersion": 1,
          "resultRetentionMs": 86400000,
          "schemaId": "task name",
          "stalledAttemptCount": 0,
          "taskRecordRetentionMs": 604800000,
          "terminalIndexRetentionMs": 604800000,
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

        const taken = yield* takeTask(taskEngine, prefix, 30000);
        expect(taken).toMatchInlineSnapshot(`
        {
          "attempt": 1,
          "createdAt": 2001-09-09T01:46:40.000Z,
          "deadLetterRetentionMs": 2592000000,
          "delay": 0,
          "errors": [],
          "eventRetentionMs": 604800000,
          "generation": 1,
          "handlerFailureCount": 0,
          "id": "123",
          "maxErrorEntries": 100,
          "maxEventEntries": 10000,
          "maxRelationships": 1000,
          "maxRetries": 0,
          "maxStalledCount": 1,
          "name": "task name",
          "onFailurePolicy": "delete",
          "onSuccessPolicy": "delete",
          "payload": "task payload",
          "protocolVersion": 1,
          "resultRetentionMs": 86400000,
          "schemaId": "task name",
          "stalledAttemptCount": 0,
          "taskRecordRetentionMs": 604800000,
          "terminalIndexRetentionMs": 604800000,
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

        yield* writeSuccess(taskEngine, prefix, taken?.id ?? "", "success");
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
      }),
    );

    // The engine routes purely on the `retryAt` it is handed: the retry/cap
    // decision lives in TaskQueue.fail. These cover the routing contract.
    it.effect("writeError with a future retryAt schedules the task", () =>
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

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "r1", stalled(1), now + 5000);

        const lists = yield* getLists(prefix);
        expect(lists.scheduled).toEqual(["r1"]);
        expect(lists.wait).toEqual([]);
        expect(lists.failed).toEqual([]);
        const task = yield* taskEngine.getTask(prefix, "r1");
        expect(task?.errors).toHaveLength(1);
        expect(task?.errors[0].retryAt).toBe(now + 5000);
      }),
    );

    it.effect("writeError with a past retryAt returns the task to wait", () =>
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

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "r2", stalled(1), now - 1);

        const lists = yield* getLists(prefix);
        expect(lists.wait).toEqual(["r2"]);
        expect(lists.scheduled).toEqual([]);
        expect(lists.failed).toEqual([]);
      }),
    );

    it.effect("writeError treats zero as an immediate retry time", () =>
      Effect.gen(function* () {
        const taskEngine = yield* TaskEngine.TaskEngine;
        yield* TaskEngine.setMockTime(1000000000000);
        const prefix = "retry-zero";
        yield* taskEngine.createTask({
          id: "r0",
          name: "t",
          payload: "p",
          delay: 0,
          maxRetries: 5,
          onSuccessPolicy: "delete",
          onFailurePolicy: "mark-as-failure",
          prefix,
        });

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "r0", stalled(1), 0);

        const lists = yield* getLists(prefix);
        expect(lists.wait).toEqual(["r0"]);
        expect(lists.failed).toEqual([]);
      }),
    );

    it.effect("retains a false terminal failure with zero error history", () =>
      Effect.gen(function* () {
        const taskEngine = yield* TaskEngine.TaskEngine;
        const prefix = "false-terminal-failure";
        yield* taskEngine.createTask({
          id: "false",
          name: "t",
          payload: "p",
          delay: 0,
          maxRetries: 0,
          maxErrorEntries: 0,
          onSuccessPolicy: "delete",
          onFailurePolicy: "keep",
          prefix,
        });

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "false", false);

        expect(yield* taskEngine.getResult(prefix, "false", 1)).toMatchObject({
          outcome: "failure",
          failure: false,
        });
      }),
    );

    it.effect(
      "writeError without a retryAt applies the failure policy immediately",
      () =>
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

          yield* takeTask(taskEngine, prefix, 30000);
          yield* writeError(taskEngine, prefix, "r3", stalled(1));

          const lists = yield* getLists(prefix);
          expect(lists.failed).toEqual(["r3"]);
          expect(lists.wait).toEqual([]);
          expect(lists.scheduled).toEqual([]);
          const task = yield* taskEngine.getTask(prefix, "r3");
          expect(task?.errors[0].retryAt).toBeUndefined();
        }),
    );

    it.effect(
      "Canceled error skips retries and applies onFailurePolicy immediately",
      () =>
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

          yield* takeTask(taskEngine, prefix, 30000);
          yield* writeError(taskEngine, prefix, "c1", canceled(1000000000000));

          const lists = yield* getLists(prefix);
          expect(lists.wait).toEqual([]);
          expect(lists.failed).toEqual(["c1"]);
        }),
    );

    it.effect(
      "Canceled error with a scheduled retry still skips retrying",
      () =>
        Effect.gen(function* () {
          const taskEngine = yield* TaskEngine.TaskEngine;
          const now = 1000000000000;
          yield* TaskEngine.setMockTime(now);
          const prefix = "fail-canceled-retry";
          yield* taskEngine.createTask({
            id: "c2",
            name: "cancel task",
            payload: "p",
            delay: 0,
            maxRetries: 5,
            onSuccessPolicy: "delete",
            onFailurePolicy: "mark-as-failure",
            prefix,
          });

          yield* takeTask(taskEngine, prefix, 30000);
          // a retryAt is provided (as TaskQueue.fail would when a retry schedule
          // exists), but Canceled must short-circuit it
          yield* writeError(
            taskEngine,
            prefix,
            "c2",
            canceled(now),
            now + 5000,
          );

          const lists = yield* getLists(prefix);
          expect(lists.scheduled).toEqual([]);
          expect(lists.wait).toEqual([]);
          expect(lists.failed).toEqual(["c2"]);
        }),
    );

    it.effect("onFailurePolicy: delete removes task entirely", () =>
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

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "d1", stalled(1));

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
      }),
    );

    it.effect("onFailurePolicy: keep removes from lists but keeps task", () =>
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

        yield* takeTask(taskEngine, prefix, 30000);
        yield* writeError(taskEngine, prefix, "k1", stalled(1));

        const lists = yield* getLists(prefix);
        expect(lists.wait).toEqual([]);
        expect(lists.active).toEqual([]);
        expect(lists.failed).toEqual([]);
        const task = yield* taskEngine.getTask(prefix, "k1");
        expect(task?.id).toBe("k1");
        expect(task?.errors).toHaveLength(1);
      }),
    );

    it.effect(
      "onSuccessPolicy: mark-as-success adds to success list and keeps task",
      () =>
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

          const taken = yield* takeTask(taskEngine, prefix, 30000);
          yield* writeSuccess(taskEngine, prefix, taken?.id ?? "", "ok");

          const lists = yield* getLists(prefix);
          expect(lists.success).toEqual(["s1"]);
          expect(lists.active).toEqual([]);
          const task = yield* taskEngine.getTask(prefix, "s1");
          expect(task?.id).toBe("s1");
        }),
    );

    it.effect("onSuccessPolicy: keep removes from lists but keeps task", () =>
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

        const taken = yield* takeTask(taskEngine, prefix, 30000);
        yield* writeSuccess(taskEngine, prefix, taken?.id ?? "", "ok");

        const lists = yield* getLists(prefix);
        expect(lists.success).toEqual([]);
        expect(lists.active).toEqual([]);
        expect(lists.wait).toEqual([]);
        const task = yield* taskEngine.getTask(prefix, "sk1");
        expect(task?.id).toBe("sk1");
      }),
    );

    it.effect("structured payloads round-trip through msgpack unchanged", () =>
      Effect.gen(function* () {
        const taskEngine = yield* TaskEngine.TaskEngine;
        const prefix = "task-engine-msgpack-roundtrip";
        // nested objects/arrays, floats, unicode, empty collections
        const payload = {
          user: { id: "u1", tags: ["a", "b"], scores: [1.5, -2, 3e10] },
          note: "unicode ✓ émoji 🎉",
          empty: [],
          nested: { deep: { flag: true, none: null } },
        };
        const created = yield* taskEngine.createTask({
          id: "mp1",
          name: "t",
          payload,
          delay: 0,
          maxRetries: 0,
          onSuccessPolicy: "keep",
          onFailurePolicy: "keep",
          prefix,
        });
        expect(created.payload).toEqual(payload);
        expect(created.errors).toEqual([]);

        const fetched = yield* taskEngine.getTask(prefix, "mp1");
        expect(fetched?.payload).toEqual(payload);

        const taken = yield* takeTask(taskEngine, prefix, 30000);
        expect(taken?.payload).toEqual(payload);
      }),
    );
  },
);
