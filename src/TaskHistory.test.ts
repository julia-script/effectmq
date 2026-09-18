import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Fiber, Schedule, Schema } from "effect";
import * as PersistenceRedis from "effect/unstable/persistence/Redis";
import {
  RedisPool,
  StorageProtocol,
  Task,
  TaskEngine,
  TaskHistory,
  TaskQueue,
  Worker,
} from "./index.js";
import { TestLayer } from "./testing/redisLayer.js";

const Progress = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Message"), text: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("Percent"), value: Schema.Number }),
]);
const queue = (name: string, maxHistoryEntries?: number | null) =>
  TaskQueue.make(
    name,
    Task.make({
      name,
      schemaId: "progress/v1",
      payload: { id: Schema.String },
      success: Schema.String,
      error: Schema.String,
      progress: Progress,
      idempotencyKey: ({ id }) => id,
      retry: Schedule.spaced("1 millis"),
      maxRetries: 1,
      storageLimits: { maxHistoryEntries, maxEventEntries: 1 },
    }),
  );
const insert = (
  prefix: string,
  overrides: Partial<
    Parameters<TaskEngine.TaskEngineService["createTask"]>[0]
  > = {},
) => ({
  prefix,
  id: "task",
  name: "history",
  schemaId: "history/v1",
  payload: null,
  delay: 0,
  maxRetries: 0,
  onSuccessPolicy: "keep" as const,
  onFailurePolicy: "keep" as const,
  historyEnabled: true,
  ...overrides,
});
const identity = (queue: string, generation = 1): TaskHistory.Identity => ({
  queue,
  taskId: "task",
  generation,
});
const key = (queue: string, generation = 1) =>
  `~effectmq:v1:${queue}:task:task:${generation}:history`;
const hash = (queue: string) => `~effectmq:v1:${queue}:task:task`;
const acquire = (
  engine: TaskEngine.TaskEngineService,
  queue: string,
  timeout = 30_000,
) =>
  engine.takeTask(queue, timeout).pipe(
    Effect.map((attempt) => {
      if (!attempt) throw new Error("Expected an acquired attempt");
      return attempt;
    }),
  );
const events = <A>(page: TaskHistory.Page<A>) =>
  page.entries.filter((e) => e.event._tag === "Progress");

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "Task history",
  (it) => {
    it.effect(
      "reads live worker progress, preserves retries and pages retained completion",
      () =>
        Effect.gen(function* () {
          const q = queue("history-live");
          const engine = yield* TaskEngine.TaskEngine;
          const offered = yield* TaskQueue.offer(
            q,
            { id: "task" },
            { onSuccessPolicy: "keep", onFailurePolicy: "keep" },
          );
          const emitted = yield* Deferred.make<void>();
          const finish = yield* Deferred.make<void>();
          const worker = Worker.make(
            q,
            (task, ctx) =>
              Effect.gen(function* () {
                yield* ctx.progress({
                  _tag: "Message",
                  text: `attempt ${task.attempt}`,
                });
                yield* Deferred.succeed(emitted, undefined);
                yield* Deferred.await(finish);
                if (task.attempt === 1) return yield* Effect.fail("retry");
                yield* ctx.progress({ _tag: "Percent", value: 100 });
                return "done";
              }),
            { pollInterval: "5 millis", maintenanceInterval: "5 millis" },
          );
          const running = yield* Effect.forkChild(Worker.run(worker));
          yield* Deferred.await(emitted);
          const live = yield* TaskQueue.readEvents(q, offered.handle);
          expect(events(live)).toHaveLength(1);
          expect(events(live)[0]?.attempt).toBe(1);
          expect(
            (yield* engine.getTask(q.name, "task"))?.outcome,
          ).toBeUndefined();
          yield* Deferred.succeed(finish, undefined);
          yield* engine.getTask(q.name, "task").pipe(
            Effect.repeat({
              schedule: Schedule.spaced("5 millis"),
              until: (task) => task?.outcome === "success",
            }),
            Effect.timeout("5 seconds"),
          );
          yield* Fiber.interrupt(running);
          const all = yield* TaskQueue.readEvents(q, offered.handle);
          expect(events(all).map((e) => e.attempt)).toEqual([1, 2, 2]);
          expect(
            all.entries.some(
              (e) =>
                e.event._tag === "Lifecycle" &&
                e.event.data._tag === "task.failed",
            ),
          ).toBe(true);
          const collected: string[] = [];
          let cursor: string | undefined;
          while (true) {
            const page = yield* TaskQueue.readEvents(q, offered.handle, {
              after: cursor,
              limit: 2,
            });
            collected.push(...page.entries.map((e) => e.id));
            cursor = page.cursor;
            if (!page.hasMore) break;
          }
          expect(collected).toEqual(all.entries.map((e) => e.id));
          const empty = yield* TaskQueue.readEvents(q, offered.handle, {
            after: cursor,
          });
          expect(empty.entries).toEqual([]);
          expect(empty.cursor).toBe(cursor);
          expect(yield* TaskQueue.readEvents(q, offered.handle)).toEqual(all);
        }),
    );

    it.effect(
      "has no default cap, supports null and trims only when configured",
      () =>
        Effect.gen(function* () {
          for (const cap of [undefined, null, 1, 3]) {
            const q = queue(`history-cap-${cap}`, cap);
            const offered = yield* TaskQueue.offer(
              q,
              { id: "task" },
              { onSuccessPolicy: "keep", onFailurePolicy: "keep" },
            );
            yield* TaskQueue.completeOne(q, (_, ctx) =>
              Effect.gen(function* () {
                for (let value = 0; value < 110; value++)
                  yield* ctx.progress({ _tag: "Percent", value });
                return "done";
              }),
            );
            const page = yield* TaskQueue.readEvents(q, offered.handle, {
              limit: 1000,
            });
            expect(page.entries.length).toBe(cap ?? 115);
            expect(page.truncated).toBe(cap !== undefined && cap !== null);
            expect(page.entries.at(-1)?.event).toMatchObject({
              _tag: "Lifecycle",
              data: { _tag: "task.moved" },
            });
          }
        }),
    );

    it.effect(
      "signals gaps, preserves the trimmed cursor boundary and binds identity",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const q = "history-gap";
          yield* engine.createTask(insert(q, { maxHistoryEntries: 3 }));
          const first = yield* engine.readHistory(identity(q), "history/v1", {
            limit: 1,
          });
          const attempt = yield* acquire(engine, q);
          const before = yield* engine.readHistory(identity(q), "history/v1");
          for (let n = 0; n < 3; n++)
            yield* engine.appendProgress(
              identity(q),
              "history/v1",
              attempt.leaseToken,
              "opaque",
            );
          // The last entry of 'before' was removed, but every later entry remains.
          const boundary = yield* engine.readHistory(
            identity(q),
            "history/v1",
            { after: before.cursor },
          );
          expect(boundary.entries).toHaveLength(3);
          const expired = yield* engine
            .readHistory(identity(q), "history/v1", { after: first.cursor })
            .pipe(Effect.flip);
          expect(expired._tag).toBe("HistoryCursorExpired");
          if (expired._tag !== "HistoryCursorExpired")
            return yield* Effect.die(expired);
          expect(
            (yield* engine.readHistory(identity(q), "history/v1", {
              after: expired.earliestCursor,
            })).entries,
          ).toEqual(boundary.entries);
          const page = yield* engine.readHistory(identity(q), "history/v1", {
            limit: 1,
          });
          for (let n = 0; n < 4; n++)
            yield* engine.appendProgress(
              identity(q),
              "history/v1",
              attempt.leaseToken,
              "opaque",
            );
          expect(
            (yield* engine
              .readHistory(identity(q), "history/v1", { after: page.cursor })
              .pipe(Effect.flip))._tag,
          ).toBe("HistoryCursorExpired");
          for (const cursor of [
            "junk",
            TaskHistory.cursor(identity(q), {
              sequence: 10,
              id: "18446744073709551616-0",
            }),
            `${first.cursor}!`,
            TaskHistory.cursor(identity("other"), { sequence: 0, id: "0-0" }),
            TaskHistory.cursor(identity(q), { sequence: 999, id: "0-0" }),
            TaskHistory.cursor(identity(q), { sequence: 10, id: "0-0" }),
          ]) {
            expect(
              (yield* engine
                .readHistory(identity(q), "history/v1", { after: cursor })
                .pipe(Effect.flip))._tag,
            ).toBe("InvalidHistoryCursor");
          }
          for (const limit of [0, -1, 1.5, 1001, Number.NaN]) {
            expect(
              (yield* engine
                .readHistory(identity(q), "history/v1", { limit })
                .pipe(Effect.flip))._tag,
            ).toBe("InvalidHistoryCursor");
          }
        }),
    );

    it.effect(
      "fences stale, expired, settled, removed and replaced attempts",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const q = "history-fenced";
          yield* TaskEngine.setMockTime(1_000_000);
          yield* engine.createTask(insert(q));
          const attempt = yield* acquire(engine, q, 100);
          const append = (token: string) =>
            engine.appendProgress(identity(q), "history/v1", token, "opaque");
          expect(yield* append(attempt.leaseToken)).toMatch(/^\d+-\d+$/);
          expect((yield* append("stale").pipe(Effect.flip))._tag).toBe(
            "LeaseLost",
          );
          yield* TaskEngine.stepMockTime(101);
          expect(
            (yield* append(attempt.leaseToken).pipe(Effect.flip))._tag,
          ).toBe("LeaseLost");
          yield* redis.send("DEL", `~effectmq:v1:${q}:lock:task`);
          const retry = yield* acquire(engine, q);
          expect(retry.task.attempt).toBe(2);
          expect(
            (yield* append(attempt.leaseToken).pipe(Effect.flip))._tag,
          ).toBe("LeaseLost");
          yield* engine.writeSuccess(q, "task", retry.leaseToken, "ok");
          expect((yield* append(retry.leaseToken).pipe(Effect.flip))._tag).toBe(
            "LeaseLost",
          );
          yield* engine.offerTask(
            insert(q, { onDuplicate: "new-generation", maxHistoryEntries: 1 }),
          );
          expect((yield* append(retry.leaseToken).pipe(Effect.flip))._tag).toBe(
            "HistoryUnavailable",
          );
          expect(yield* redis.send("EXISTS", key(q))).toBe(0);
          const replaced = yield* engine.readHistory(
            identity(q, 2),
            "history/v1",
          );
          expect(replaced.entries).toHaveLength(1);
          yield* engine.forceRemoveTask(q, "task");
          expect(yield* redis.send("EXISTS", key(q, 2))).toBe(0);
        }),
    );

    it.effect(
      "keeps configuration immutable and leaves legacy and disabled tasks alone",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const q = "history-disabled";
          yield* engine.createTask(insert(q, { historyEnabled: false }));
          expect(yield* redis.send("HEXISTS", hash(q), "historyEnabled")).toBe(
            0,
          );
          yield* engine.offerTask(insert(q));
          expect(
            (yield* engine
              .readHistory(identity(q), "history/v1")
              .pipe(Effect.flip))._tag,
          ).toBe("HistoryDisabled");
          const attempt = yield* acquire(engine, q);
          yield* engine.writeSuccess(q, "task", attempt.leaseToken, "ok");
          expect(yield* redis.send("EXISTS", key(q))).toBe(0);
          yield* engine.offerTask(
            insert(q, { onDuplicate: "new-generation", maxHistoryEntries: 3 }),
          );
          const before = yield* engine.readHistory(
            identity(q, 2),
            "history/v1",
          );
          yield* engine.offerTask(insert(q, { maxHistoryEntries: 1 }));
          expect(
            yield* engine.readHistory(identity(q, 2), "history/v1"),
          ).toEqual(before);
          expect((yield* engine.getTask(q, "task"))?.maxHistoryEntries).toBe(3);
          // SCRIPT FLUSH rehearses NOSCRIPT reload on an existing generation.
          yield* redis.send("SCRIPT", "FLUSH");
          expect(
            yield* engine.readHistory(identity(q, 2), "history/v1"),
          ).toEqual(before);
          yield* engine.forceRemoveTask(q, "task");
          expect(yield* redis.send("EXISTS", key(q, 2))).toBe(0);
        }),
    );

    it.effect(
      "disposes history with records for every completion policy and expiry",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          for (const success of [true, false])
            for (const policy of ["delete", "keep", "mark"] as const) {
              const q = `history-disposal-${success}-${policy}`;
              yield* TaskEngine.setMockTime(2_000_000);
              yield* engine.createTask(
                insert(q, {
                  onSuccessPolicy:
                    policy === "mark" ? "mark-as-success" : policy,
                  onFailurePolicy:
                    policy === "mark" ? "mark-as-failure" : policy,
                  taskRecordRetentionMs: 100,
                  resultRetentionMs: 1000,
                }),
              );
              const a = yield* acquire(engine, q);
              if (success)
                yield* engine.writeSuccess(q, "task", a.leaseToken, "ok");
              else yield* engine.writeError(q, "task", a.leaseToken, "failed");
              expect(yield* redis.send("EXISTS", key(q))).toBe(
                policy === "delete" ? 0 : 1,
              );
              expect(yield* engine.getResult(q, "task", 1)).not.toBeNull();
              yield* TaskEngine.stepMockTime(101);
              yield* engine.maintain(q);
              expect(yield* redis.send("EXISTS", key(q))).toBe(0);
              expect(yield* engine.getResult(q, "task", 1)).not.toBeNull();
              expect(
                (yield* engine
                  .readHistory(identity(q), "history/v1")
                  .pipe(Effect.flip))._tag,
              ).toBe("HistoryUnavailable");
            }
        }),
    );

    it.effect(
      "holds preserve history until final release and failed offers leave no history",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const q = "history-held";
          yield* engine.createTask(
            insert("history-holder", { historyEnabled: false }),
          );
          yield* engine.createTask(
            insert(q, {
              onSuccessPolicy: "delete",
              retentionHolder: {
                queue: "history-holder",
                id: "task",
                generation: 1,
              },
            }),
          );
          const a = yield* acquire(engine, q);
          yield* engine.writeSuccess(q, "task", a.leaseToken, "ok");
          expect(
            (yield* engine.readHistory(identity(q), "history/v1")).entries
              .length,
          ).toBeGreaterThan(0);
          yield* engine.removeTask(q, "task").pipe(Effect.flip);
          expect(yield* redis.send("EXISTS", key(q))).toBe(1);
          yield* engine.removeTask("history-holder", "task");
          expect(yield* redis.send("EXISTS", key(q))).toBe(0);
          yield* engine
            .offerTask(
              insert(q, {
                retentionHolder: {
                  queue: "missing",
                  id: "missing",
                  generation: 1,
                },
              }),
            )
            .pipe(Effect.flip);
          expect(yield* redis.send("EXISTS", key(q, 2))).toBe(0);
          yield* engine.createTask(insert("history-remove"));
          yield* engine.removeTask("history-remove", "task");
          expect(yield* redis.send("EXISTS", key("history-remove"))).toBe(0);
        }),
    );

    it.effect(
      "does not replay an append after an acknowledgement is lost",
      () =>
        Effect.gen(function* () {
          const redis = yield* RedisPool.RedisPool;
          let appends = 0;
          const send: RedisPool.RedisSend = <A>(
            command: string,
            ...args: ReadonlyArray<RedisPool.RedisArgument>
          ) =>
            Effect.gen(function* () {
              const result = yield* redis.send<A>(command, ...args);
              if (
                command === "EVALSHA" &&
                args.includes("effectmq_appendProgress")
              ) {
                appends++;
                return yield* new PersistenceRedis.RedisError({
                  cause: new Error("read ECONNRESET"),
                });
              }
              return result;
            });
          const pool = yield* RedisPool.make(send, redis.sendBinary);
          const engine = yield* TaskEngine.make({ debugMode: true }).pipe(
            Effect.provideService(RedisPool.RedisPool, pool),
          );
          const q = TaskQueue.make(
            "history-lost-ack",
            Task.make({
              name: "never-error",
              payload: {},
              success: Schema.String,
              error: Schema.Never,
              progress: Schema.String,
              idempotencyKey: () => "task",
            }),
          );
          const offered = yield* TaskQueue.offer(
            q,
            {},
            { onSuccessPolicy: "keep" },
          ).pipe(Effect.provideService(TaskEngine.TaskEngine, engine));
          const failure = yield* TaskQueue.completeOne(q, (_, ctx) =>
            ctx.progress("working").pipe(Effect.as("done")),
          ).pipe(
            Effect.provideService(TaskEngine.TaskEngine, engine),
            Effect.flip,
          );
          expect(failure).toMatchObject({
            _tag: "ProgressWriteError",
            reason: "IndeterminateWrite",
          });
          expect(appends).toBe(1);
          const page = yield* TaskQueue.readEvents(q, offered.handle);
          expect(events(page)).toHaveLength(1);
          const task = yield* engine.getTask(q.name, "task");
          expect(task?.outcome).toBeUndefined();
          expect(task?.errors).toHaveLength(0);
        }),
    );

    it.effect(
      "validates progress schema and size; callers can catch operational failures",
      () =>
        Effect.gen(function* () {
          const q = TaskQueue.make(
            "history-progress-codec",
            Task.make({
              name: "codec",
              payload: {},
              success: Schema.String,
              error: Schema.Never,
              progress: Schema.String,
              storageLimits: { maxValueBytes: 100 },
              idempotencyKey: () => "task",
            }),
          );
          const offered = yield* TaskQueue.offer(
            q,
            {},
            { onSuccessPolicy: "keep" },
          );
          yield* TaskQueue.complete(q, (_, ctx) =>
            Effect.gen(function* () {
              const failure = yield* ctx
                .progress("x".repeat(200))
                .pipe(Effect.flip, Effect.orDie);
              expect(failure.cause).toMatchObject({
                _tag: "StorageLimitExceeded",
                kind: "progress",
              });
              yield* ctx.progress("ok");
              return "done";
            }),
          );
          expect(
            events(yield* TaskQueue.readEvents(q, offered.handle)),
          ).toHaveLength(1);
        }),
    );

    it.effect(
      "round-trips bytes and collections in progress envelopes and rejects corruption",
      () =>
        Effect.gen(function* () {
          const value = {
            bytes: new Uint8Array([0, 128, 255]),
            empty: null,
            list: [null, { text: "😀" }],
          };
          const encoded = yield* StorageProtocol.encodeValue(
            "history/v1",
            "progress",
            value,
          );
          expect(
            yield* StorageProtocol.decodeValue(
              encoded,
              "history/v1",
              "progress",
            ),
          ).toEqual(value);
          expect(
            (yield* StorageProtocol.decodeValue(
              encoded,
              "other",
              "progress",
            ).pipe(Effect.flip))._tag,
          ).toBe("SchemaIdentityMismatch");
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const q = "history-corrupt";
          yield* engine.createTask(insert(q));
          for (const [field, value, tag] of [
            ["schemaId", "other", "SchemaIdentityMismatch"],
            ["protocolVersion", "2", "UnsupportedProtocolVersion"],
          ]) {
            const original = yield* redis.send<string>("HGET", hash(q), field);
            yield* redis.send("HSET", hash(q), field, value);
            expect(
              (yield* engine
                .readHistory(identity(q), "history/v1")
                .pipe(Effect.flip))._tag,
            ).toBe(tag);
            yield* redis.send("HSET", hash(q), field, original);
          }
          yield* redis.send("DEL", key(q));
          yield* engine
            .readHistory(identity(q), "history/v1")
            .pipe(Effect.flip);
        }),
    );

    it.effect(
      "rejects invalid count limits on first use without mutating Redis",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          for (const cap of [
            0,
            -1,
            1.5,
            Infinity,
            NaN,
            Number.MAX_SAFE_INTEGER + 1,
          ]) {
            const q = queue(`history-invalid-${cap}`, cap);
            const cause = yield* TaskQueue.offer(
              q,
              { id: "task" },
              { onSuccessPolicy: "keep", onFailurePolicy: "keep" },
            ).pipe(Effect.sandbox, Effect.flip);
            expect(cause.reasons.some((r) => r._tag === "Die")).toBe(true);
            expect(yield* engine.getGeneration(q.name, "task")).toBe(0);
          }
        }),
    );
    it.effect(
      "orders racing appends before settlement and never resurrects removed streams",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          for (const remove of [false, true]) {
            const q = `history-race-${remove}`;
            yield* engine.createTask(insert(q));
            const a = yield* acquire(engine, q);
            const results = yield* Effect.all(
              [
                engine
                  .appendProgress(
                    identity(q),
                    "history/v1",
                    a.leaseToken,
                    "opaque",
                  )
                  .pipe(Effect.result),
                (remove
                  ? engine.forceRemoveTask(q, "task")
                  : engine.writeSuccess(q, "task", a.leaseToken, "ok")
                ).pipe(Effect.result),
              ],
              { concurrency: "unbounded" },
            );
            expect(results[1]._tag).toBe("Success");
            if (remove) expect(yield* redis.send("EXISTS", key(q))).toBe(0);
            else {
              const page = yield* engine.readHistory(identity(q), "history/v1");
              const completed = page.entries.findIndex(
                (e) =>
                  e.event._tag === "Lifecycle" &&
                  e.event.data._tag === "task.completed",
              );
              expect(completed).toBeGreaterThan(0);
              for (const e of events(page))
                expect(page.entries.indexOf(e)).toBeLessThan(completed);
            }
            yield* engine
              .appendProgress(identity(q), "history/v1", a.leaseToken, "late")
              .pipe(Effect.flip);
            if (remove) expect(yield* redis.send("EXISTS", key(q))).toBe(0);
          }
        }),
    );

    it.effect(
      "records cancellation and stalled recovery but no unchanged renewals",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          for (const kind of ["canceled", "stall"]) {
            const q = `history-${kind}`;
            yield* TaskEngine.setMockTime(3_000_000);
            yield* engine.createTask(insert(q, { maxStalledCount: 0 }));
            const a = yield* acquire(engine, q, 100);
            const before = yield* engine.readHistory(identity(q), "history/v1");
            yield* engine.extendLock(q, "task", a.leaseToken, 100);
            expect(
              yield* engine.readHistory(identity(q), "history/v1"),
            ).toEqual(before);
            expect(
              before.entries.find(
                (e) =>
                  e.event._tag === "Lifecycle" && e.event.data.to === "active",
              )?.attempt,
            ).toBe(1);
            expect(before.entries[0]?.attempt).toBe(0);
            if (kind === "canceled")
              yield* engine.writeError(q, "task", a.leaseToken, {
                _tag: "~effectmq/Error/Canceled",
              });
            else {
              yield* redis.send("DEL", `~effectmq:v1:${q}:lock:task`);
              yield* TaskEngine.stepMockTime(101);
              yield* engine.maintain(q);
            }
            const page = yield* engine.readHistory(identity(q), "history/v1");
            expect(
              page.entries.find(
                (e) =>
                  e.event._tag === "Lifecycle" &&
                  e.event.data._tag === "task.failed",
              )?.event,
            ).toMatchObject({
              _tag: "Lifecycle",
              data: { failureKind: kind, terminal: true },
            });
          }
        }),
    );

    it.effect(
      "captured contexts reject later writes and interruption leaves recovery to the lease",
      () =>
        Effect.gen(function* () {
          const q = queue("history-context");
          const engine = yield* TaskEngine.TaskEngine;
          const handle = (yield* TaskQueue.offer(
            q,
            { id: "task" },
            { onSuccessPolicy: "keep" },
          )).handle;
          let saved: TaskHistory.Context<typeof Progress> | undefined;
          yield* TaskQueue.completeOne(q, (_, ctx) => {
            saved = ctx;
            return Effect.succeed("ok");
          });
          if (!saved) return yield* Effect.die("Missing context");
          const failure = yield* saved
            .progress({ _tag: "Message", text: "late" })
            .pipe(Effect.flip);
          expect(failure.cause).toMatchObject({ _tag: "LeaseLost" });
          expect(events(yield* TaskQueue.readEvents(q, handle))).toHaveLength(
            0,
          );
          const q2 = queue("history-interrupted");
          yield* TaskQueue.offer(q2, { id: "task" });
          const started = yield* Deferred.make<void>();
          const running = yield* Effect.forkChild(
            TaskQueue.completeOne(q2, (_, ctx) =>
              Effect.gen(function* () {
                yield* ctx.progress({ _tag: "Message", text: "start" });
                yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }),
            ),
          );
          yield* Deferred.await(started);
          yield* Fiber.interrupt(running);
          expect(
            (yield* engine.getTask(q2.name, "task"))?.outcome,
          ).toBeUndefined();
          expect((yield* engine.getTask(q2.name, "task"))?.errors).toHaveLength(
            0,
          );
        }),
    );

    it.effect(
      "rejects corrupt page entries and malformed schema values without partial pages",
      () =>
        Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const redis = yield* RedisPool.RedisPool;
          const q = queue("history-invalid-entry");
          const offered = yield* TaskQueue.offer(q, { id: "task" });
          const a = yield* acquire(engine, q.name);
          yield* engine.appendProgress(
            identity(q.name),
            q.task.schemaId,
            a.leaseToken,
            "not an envelope",
          );
          expect(
            (yield* TaskQueue.readEvents(q, offered.handle).pipe(Effect.flip))
              ._tag,
          ).toBe("CorruptStorageValue");
          const q2 = queue("history-invalid-schema");
          const handle = (yield* TaskQueue.offer(q2, { id: "task" })).handle;
          yield* TaskQueue.completeOne(q2, (_, ctx) =>
            Effect.gen(function* () {
              const error = yield* ctx
                // @ts-expect-error Exercise runtime validation at an untyped application boundary.
                .progress({ _tag: "Percent", value: "bad" })
                .pipe(Effect.flip, Effect.orDie);
              expect(error.cause).toMatchObject({ _tag: "SchemaError" });
              return "ok";
            }),
          );
          expect(
            (yield* TaskQueue.readEvents(q2, handle).pipe(Effect.flip))._tag,
          ).toBe("HistoryUnavailable");
          // A malformed entry with a valid stream count fails decoding, too.
          yield* redis.send("DEL", key(q.name));
          yield* redis.send(
            "HSET",
            hash(q.name),
            "historySequence",
            "1",
            "historyTrimmed",
            "0",
          );
          yield* redis.send("XADD", key(q.name), "*", "data", "broken");
          expect(
            (yield* engine
              .readHistory(identity(q.name), q.task.schemaId)
              .pipe(Effect.flip))._tag,
          ).toBe("SchemaError");
        }),
    );
  },
);
