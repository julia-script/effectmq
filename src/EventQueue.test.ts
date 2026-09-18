import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Schema } from "effect";
import * as EventEngine from "./EventEngine.js";
import * as EventQueue from "./EventQueue.js";
import type * as EventRecord from "./EventRecord.js";
import * as RedisPool from "./RedisPool.js";
import { redisContainerLayer } from "./testing/redisLayer.js";

const TestLayer = EventEngine.layerNoDeps({ maintenanceBatchSize: 2 }).pipe(
  Layer.provideMerge(redisContainerLayer()),
  Layer.provideMerge(NodeCrypto.layer),
);
const payload = Schema.Struct({ value: Schema.String });
const queue = (name: string, options: Omit<EventEngine.Queue, "name"> = {}) =>
  EventQueue.make(name, payload, { onCompletion: "archive", ...options });
const root = (name: string) =>
  `~effectmq:events:v1:{${Buffer.from(name).toString("base64url")}}`;
const take = Effect.fnUntraced(function* (
  q: ReturnType<typeof queue>,
  sub: EventRecord.Subscription,
  leaseMs = 30_000,
) {
  const delivery = yield* EventQueue.take(q, sub, { leaseMs });
  if (delivery === null) return yield* Effect.die("Expected a delivery");
  return delivery;
});

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "durable event queues (real Redis)",
  (it) => {
    it.effect(
      "registers names idempotently under concurrent callers and scopes them to queues",
      () =>
        Effect.gen(function* () {
          const q = queue("event-registration");
          const registrations = yield* Effect.all(
            Array.from({ length: 12 }, () =>
              EventQueue.subscribe(q, "billing"),
            ),
            { concurrency: "unbounded" },
          );
          expect(new Set(registrations.map((s) => s.generation)).size).toBe(1);
          const other = yield* EventQueue.subscribe(
            queue("event-registration-other"),
            "billing",
          );
          expect(other.generation).not.toBe(registrations[0].generation);
          const emitted = yield* EventQueue.emit(q, { value: "one" });
          expect(Object.values(emitted.recipients)).toHaveLength(1);
          const engine = yield* EventEngine.make();
          expect(yield* engine.subscribe(q, "billing")).toEqual(
            registrations[0],
          );
          expect((yield* engine.take(q, registrations[0]))?.event.id).toBe(
            emitted.id,
          );
        }),
    );

    it.effect(
      "captures recipients at emission and acknowledges each independently",
      () =>
        Effect.gen(function* () {
          const q = queue("event-fanout");
          const a = yield* EventQueue.subscribe(q, "a");
          const b = yield* EventQueue.subscribe(q, "b");
          const emitted = yield* EventQueue.emit(q, { value: "hello" });
          const late = yield* EventQueue.subscribe(q, "late");
          expect(yield* EventQueue.take(q, late)).toBeNull();
          const first = yield* take(q, a);
          expect(first.event.payload).toEqual({ value: "hello" });
          expect(yield* EventQueue.acknowledge(q, first)).toBe("acknowledged");
          expect(yield* EventQueue.acknowledge(q, first)).toBe(
            "already-acknowledged",
          );
          expect((yield* EventQueue.get(q, emitted.id))?.status).toBe("active");
          expect(yield* EventQueue.take(q, a)).toBeNull();
          const second = yield* take(q, b);
          yield* EventQueue.acknowledge(q, second);
          const archived = yield* EventQueue.get(q, emitted.id);
          expect(archived?.status).toBe("completed");
          expect(
            Object.values(archived?.recipients ?? {}).map((r) => r.status),
          ).toEqual(["acknowledged", "acknowledged"]);
          expect(yield* EventQueue.listArchived(q)).toEqual([emitted.id]);
        }),
    );

    it.effect(
      "shares one leased delivery between workers and fences expired attempts",
      () =>
        Effect.gen(function* () {
          const q = queue("event-leases");
          const sub = yield* EventQueue.subscribe(q, "shared");
          yield* EventQueue.emit(q, { value: "one" });
          const attempts = yield* Effect.all(
            [
              EventQueue.take(q, sub, { leaseMs: 100 }),
              EventQueue.take(q, sub, { leaseMs: 100 }),
            ],
            { concurrency: "unbounded" },
          );
          expect(attempts.filter(Boolean)).toHaveLength(1);
          const old = attempts.find((d) => d !== null);
          if (!old) return yield* Effect.die("Missing delivery");
          yield* Effect.sleep(150);
          const current = yield* take(q, sub);
          expect(current.leaseToken).not.toBe(old.leaseToken);
          for (const operation of [
            EventQueue.acknowledge(q, old),
            EventQueue.renew(q, old),
            EventQueue.release(q, old),
          ]) {
            expect(yield* operation.pipe(Effect.flip)).toMatchObject({
              code: "LeaseLost",
            });
          }
          yield* EventQueue.acknowledge(q, current);
        }),
    );

    it.effect(
      "releases an obligation for retry and renewal extends ownership",
      () =>
        Effect.gen(function* () {
          const q = queue("event-release");
          const sub = yield* EventQueue.subscribe(q, "worker");
          yield* EventQueue.emit(q, { value: "retry" });
          const first = yield* take(q, sub, 100);
          yield* EventQueue.renew(q, first, 500);
          yield* Effect.sleep(150);
          expect(yield* EventQueue.take(q, sub)).toBeNull();
          yield* EventQueue.release(q, first, 100);
          expect(yield* EventQueue.take(q, sub)).toBeNull();
          expect(
            yield* EventQueue.acknowledge(q, first).pipe(Effect.flip),
          ).toMatchObject({ code: "LeaseLost" });
          yield* Effect.sleep(130);
          const second = yield* take(q, sub);
          yield* EventQueue.acknowledge(q, second);
        }),
    );

    it.effect(
      "waives only removed recipients and isolates replacement generations",
      () =>
        Effect.gen(function* () {
          const q = queue("event-removal");
          const old = yield* EventQueue.subscribe(q, "billing");
          const other = yield* EventQueue.subscribe(q, "email");
          const emitted = yield* EventQueue.emit(q, { value: "old" });
          const oldAttempt = yield* take(q, old);
          expect(yield* EventQueue.unsubscribe(q, old)).toBe(true);
          const replacement = yield* EventQueue.subscribe(q, "billing");
          expect(replacement.generation).not.toBe(old.generation);
          expect(yield* EventQueue.unsubscribe(q, old)).toBe(false);
          expect(yield* EventQueue.take(q, replacement)).toBeNull();
          expect(
            yield* EventQueue.take(q, old).pipe(Effect.flip),
          ).toMatchObject({ code: "SubscriptionMissing" });
          expect(
            yield* EventQueue.acknowledge(q, oldAttempt).pipe(Effect.flip),
          ).toMatchObject({ code: "LeaseLost" });
          yield* EventQueue.acknowledge(q, yield* take(q, other));
          const archived = yield* EventQueue.get(q, emitted.id);
          expect(archived?.status).toBe("completed");
          expect(archived?.recipients[old.generation].status).toBe("waived");
          expect(archived?.recipients[other.generation].status).toBe(
            "acknowledged",
          );
          const next = yield* EventQueue.emit(q, { value: "new" });
          expect((yield* take(q, replacement)).event.id).toBe(next.id);
        }),
    );

    it.effect(
      "settles zero-recipient emissions immediately under both retention policies",
      () =>
        Effect.gen(function* () {
          const archived = queue("event-zero-archive");
          const record = yield* EventQueue.emit(archived, { value: "empty" });
          expect(record.status).toBe("completed");
          expect(record.recipients).toEqual({});
          expect((yield* EventQueue.get(archived, record.id))?.status).toBe(
            "completed",
          );
          const deleted = queue("event-zero-delete", {
            onCompletion: "delete",
          });
          const gone = yield* EventQueue.emit(deleted, { value: "empty" });
          expect(gone.status).toBe("completed");
          expect(yield* EventQueue.get(deleted, gone.id)).toBeNull();
          expect(yield* EventQueue.listArchived(deleted)).toEqual([]);
        }),
    );

    it.effect(
      "fully deletes event records and all delivery indexes after completion",
      () =>
        Effect.gen(function* () {
          const q = queue("event-full-delete", { onCompletion: "delete" });
          const a = yield* EventQueue.subscribe(q, "a");
          const b = yield* EventQueue.subscribe(q, "b");
          const event = yield* EventQueue.emit(
            q,
            { value: "delete" },
            { ttlMs: 10000 },
          );
          const first = yield* take(q, a);
          const second = yield* take(q, b);
          yield* Effect.all(
            [
              EventQueue.acknowledge(q, first),
              EventQueue.acknowledge(q, second),
            ],
            { concurrency: "unbounded" },
          );
          expect(yield* EventQueue.get(q, event.id)).toBeNull();
          expect(yield* EventQueue.acknowledge(q, second)).toBe("gone");
          const redis = yield* RedisPool.RedisPool;
          const keys = yield* redis.send<string[]>("KEYS", `${root(q.name)}:*`);
          expect(keys.sort()).toEqual(
            [`${root(q.name)}:config`, `${root(q.name)}:subscriptions`].sort(),
          );
        }),
    );

    it.effect(
      "expires unfinished events using Redis time and prevents late acknowledgement",
      () =>
        Effect.gen(function* () {
          const q = queue("event-expiry", { ttlMs: 100 });
          const sub = yield* EventQueue.subscribe(q, "offline");
          const event = yield* EventQueue.emit(q, { value: "expires" });
          const delivery = yield* take(q, sub);
          yield* Effect.sleep(150);
          expect(
            yield* EventQueue.acknowledge(q, delivery).pipe(Effect.flip),
          ).toMatchObject({ code: "EventNotActive" });
          expect(yield* EventQueue.take(q, sub)).toBeNull();
          const expired = yield* EventQueue.get(q, event.id);
          expect(expired?.status).toBe("expired");
          expect(expired?.settledAt).toBe(event.expiresAt);
          expect(expired?.recipients[sub.generation].status).toBe("pending");
          expect(expired?.archiveUntil).toBeNull();
        }),
    );

    it.effect(
      "allows indefinite events and a null override of the queue deadline",
      () =>
        Effect.gen(function* () {
          const q = queue("event-indefinite", { ttlMs: 1 });
          const sub = yield* EventQueue.subscribe(q, "offline");
          const event = yield* EventQueue.emit(
            q,
            { value: "indefinite" },
            { ttlMs: null },
          );
          expect(event.expiresAt).toBeNull();
          yield* Effect.sleep(30);
          yield* EventQueue.maintain(q);
          expect((yield* EventQueue.get(q, event.id))?.status).toBe("active");
          expect((yield* take(q, sub)).event.id).toBe(event.id);
          const noDefault = queue("event-no-default");
          yield* EventQueue.subscribe(noDefault, "offline");
          expect(
            (yield* EventQueue.emit(noDefault, { value: "no default" }))
              .expiresAt,
          ).toBeNull();
        }),
    );

    it.effect(
      "uses independent archive retention and removes finite archives",
      () =>
        Effect.gen(function* () {
          const q = queue("event-archive-retention", {
            ttlMs: 10000,
            archiveRetentionMs: 100,
          });
          const sub = yield* EventQueue.subscribe(q, "worker");
          const event = yield* EventQueue.emit(q, { value: "retained" });
          yield* EventQueue.acknowledge(q, yield* take(q, sub));
          const archived = yield* EventQueue.get(q, event.id);
          expect(archived?.archiveUntil).toBe((archived?.settledAt ?? 0) + 100);
          yield* Effect.sleep(150);
          yield* EventQueue.maintain(q);
          expect(yield* EventQueue.get(q, event.id)).toBeNull();
          expect(yield* EventQueue.listArchived(q)).toEqual([]);
        }),
    );

    it.effect(
      "drains removals in bounded batches and preserves completion before later expiry",
      () =>
        Effect.gen(function* () {
          const q = queue("event-bounded-removal", { ttlMs: 500 });
          const sub = yield* EventQueue.subscribe(q, "removed");
          const events = yield* Effect.forEach(
            Array.from({ length: 8 }),
            (_, i) => EventQueue.emit(q, { value: String(i) }),
          );
          yield* EventQueue.unsubscribe(q, sub);
          const redis = yield* RedisPool.RedisPool;
          expect(
            yield* redis.send<number>(
              "ZCARD",
              `${root(q.name)}:delivery:${sub.generation}`,
            ),
          ).toBe(6);
          yield* Effect.sleep(550);
          // Removal preceded the deadline; delayed materialization must not turn completion into expiry.
          for (const event of events) {
            const stored = yield* EventQueue.get(q, event.id);
            expect(stored?.status).toBe("completed");
            expect(stored?.recipients[sub.generation].status).toBe("waived");
          }
          const result = yield* EventQueue.maintain(q);
          expect(result.pending).toBe(false);
          expect(
            yield* redis.send<number>(
              "EXISTS",
              `${root(q.name)}:removed`,
              `${root(q.name)}:retired`,
              `${root(q.name)}:delivery:${sub.generation}`,
            ),
          ).toBe(0);
        }),
    );

    it.effect(
      "expires and cleans idle queues through bounded maintenance",
      () =>
        Effect.gen(function* () {
          const q = queue("event-idle-maintenance", {
            onCompletion: "delete",
            ttlMs: 1,
          });
          yield* EventQueue.subscribe(q, "offline");
          const events = yield* Effect.forEach(Array.from({ length: 7 }), () =>
            EventQueue.emit(q, { value: "expire" }),
          );
          yield* Effect.sleep(10);
          let result = yield* EventQueue.maintain(q);
          expect(result.processed).toBeLessThanOrEqual(2);
          while (result.pending) result = yield* EventQueue.maintain(q);
          for (const event of events)
            expect(yield* EventQueue.get(q, event.id)).toBeNull();
        }),
    );

    it.effect("serializes subscription changes with concurrent emissions", () =>
      Effect.gen(function* () {
        for (let i = 0; i < 10; i++) {
          const q = queue(`event-race-${i}`);
          const [sub, event] = yield* Effect.all(
            [
              EventQueue.subscribe(q, "racer"),
              EventQueue.emit(q, { value: "racing" }),
            ],
            { concurrency: "unbounded" },
          );
          const recipients = Object.values(event.recipients);
          expect(recipients.length).toBeLessThanOrEqual(1);
          if (recipients.length === 0) {
            expect(event.status).toBe("completed");
            expect(yield* EventQueue.take(q, sub)).toBeNull();
          } else {
            expect(recipients[0].generation).toBe(sub.generation);
            expect((yield* take(q, sub)).event.id).toBe(event.id);
          }
          yield* EventQueue.unsubscribe(q, sub);
          expect((yield* EventQueue.get(q, event.id))?.status).toBe(
            "completed",
          );
        }
      }),
    );

    it.effect(
      "rejects conflicting policies and invalid configuration before mutation",
      () =>
        Effect.gen(function* () {
          const q = queue("event-config");
          const sub = yield* EventQueue.subscribe(q, "worker");
          expect(
            yield* EventQueue.subscribe(
              { ...q, onCompletion: "delete" },
              "new",
            ).pipe(Effect.flip),
          ).toMatchObject({ code: "ConfigurationConflict" });
          for (const ttlMs of [-1, Number.NaN, Infinity, 0.5]) {
            expect(
              yield* EventQueue.emit(q, { value: "invalid" }, { ttlMs }).pipe(
                Effect.flip,
              ),
            ).toMatchObject({ code: "InvalidInput" });
          }
          expect(
            yield* EventQueue.subscribe(q, "").pipe(Effect.flip),
          ).toMatchObject({ code: "InvalidInput" });
          expect(
            yield* EventQueue.take(queue("other"), sub).pipe(Effect.flip),
          ).toMatchObject({ code: "InvalidInput" });
          expect(
            yield* EventQueue.take(q, sub, { leaseMs: 0 }).pipe(Effect.flip),
          ).toMatchObject({ code: "InvalidInput" });
          expect(yield* EventQueue.take(q, sub)).toBeNull();
          expect(
            yield* EventEngine.make({ maintenanceBatchSize: 0 }).pipe(
              Effect.flip,
            ),
          ).toMatchObject({ code: "InvalidInput" });
          expect(
            yield* EventEngine.make({ prefix: "bad{slot}" }).pipe(Effect.flip),
          ).toMatchObject({ code: "InvalidInput" });
        }),
    );

    it.effect(
      "round-trips schema transformations and preserves typed codec failures",
      () =>
        Effect.gen(function* () {
          const q = EventQueue.make(
            "event-codecs",
            Schema.Struct({ value: Schema.NumberFromString }),
            { onCompletion: "archive" },
          );
          const sub = yield* EventQueue.subscribe(q, "worker");
          const event = yield* EventQueue.emit(q, { value: 42 });
          expect((yield* EventQueue.get(q, event.id))?.payload.value).toBe(42);
          const delivery = yield* EventQueue.take(q, sub);
          expect(delivery?.event.payload.value).toBe(42);
          expect(
            yield* EventQueue.get(
              { ...q, payload: Schema.Struct({ value: Schema.Number }) },
              event.id,
            ).pipe(Effect.flip),
          ).toMatchObject({ _tag: "SchemaError" });
          const redis = yield* RedisPool.RedisPool;
          const key = `${root(q.name)}:event:${event.id}`;
          const stored = JSON.parse(yield* redis.send<string>("GET", key));
          stored.payload = "corrupt";
          yield* redis.send("SET", key, JSON.stringify(stored));
          expect(
            yield* EventQueue.get(q, event.id).pipe(Effect.flip),
          ).toMatchObject({ _tag: "CorruptStorageValue" });
          stored.version = 999;
          yield* redis.send("SET", key, JSON.stringify(stored));
          expect(
            yield* EventQueue.get(q, event.id).pipe(Effect.flip),
          ).toMatchObject({ code: "CorruptStorage" });
        }),
    );

    it.effect(
      "renews managed work and acknowledges only after handler success",
      () =>
        Effect.gen(function* () {
          const q = queue("event-managed");
          const sub = yield* EventQueue.subscribe(q, "worker");
          const event = yield* EventQueue.emit(q, { value: "work" });
          const started = yield* Deferred.make<void>();
          const fiber = yield* EventQueue.processOne(
            q,
            sub,
            () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Effect.sleep(400)),
              ),
            { leaseMs: 200, renewEveryMs: 30 },
          ).pipe(Effect.forkChild);
          yield* Deferred.await(started);
          yield* Effect.sleep(250);
          expect(yield* EventQueue.take(q, sub)).toBeNull();
          expect(yield* Fiber.join(fiber)).toBe(true);
          expect((yield* EventQueue.get(q, event.id))?.status).toBe(
            "completed",
          );
          expect(yield* EventQueue.processOne(q, sub, () => Effect.void)).toBe(
            false,
          );
        }),
    );

    it.effect(
      "propagates handler failures and releases the obligation for retry",
      () =>
        Effect.gen(function* () {
          const q = queue("event-handler-failure");
          const sub = yield* EventQueue.subscribe(q, "worker");
          const event = yield* EventQueue.emit(q, { value: "fail" });
          expect(
            yield* EventQueue.processOne(
              q,
              sub,
              () => Effect.fail("handler failed"),
              { retryDelayMs: 0 },
            ).pipe(Effect.flip),
          ).toBe("handler failed");
          expect((yield* EventQueue.get(q, event.id))?.status).toBe("active");
          expect(yield* EventQueue.processOne(q, sub, () => Effect.void)).toBe(
            true,
          );
        }),
    );

    it.effect(
      "interrupts managed handlers when subscription removal loses ownership",
      () =>
        Effect.gen(function* () {
          const q = queue("event-managed-ownership");
          const sub = yield* EventQueue.subscribe(q, "worker");
          const event = yield* EventQueue.emit(q, { value: "remove" });
          const started = yield* Deferred.make<void>();
          const stopped = yield* Deferred.make<void>();
          const fiber = yield* EventQueue.processOne(
            q,
            sub,
            () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(stopped, undefined)),
              ),
            { leaseMs: 200, renewEveryMs: 20 },
          ).pipe(Effect.flip, Effect.forkChild);
          yield* Deferred.await(started);
          yield* EventQueue.unsubscribe(q, sub);
          expect(yield* Fiber.join(fiber)).toMatchObject({
            _tag: "EventEngineError",
          });
          yield* Deferred.await(stopped);
          expect(
            (yield* EventQueue.get(q, event.id))?.recipients[sub.generation]
              .status,
          ).toBe("waived");
        }),
    );

    it.effect(
      "runs an interruptible maintenance loop for idle event deadlines",
      () =>
        Effect.gen(function* () {
          const q = queue("event-maintenance-loop", {
            onCompletion: "delete",
            ttlMs: 20,
          });
          yield* EventQueue.subscribe(q, "offline");
          const event = yield* EventQueue.emit(q, { value: "idle" });
          const fiber = yield* EventQueue.runMaintenance(q, 10).pipe(
            Effect.forkChild,
          );
          yield* Effect.sleep(100);
          const redis = yield* RedisPool.RedisPool;
          expect(
            yield* redis.send("EXISTS", `${root(q.name)}:event:${event.id}`),
          ).toBe(0);
          yield* Fiber.interrupt(fiber);
        }),
    );
    it.effect(
      "archive listing excludes expired records even when cleanup has a backlog",
      () =>
        Effect.gen(function* () {
          const q = queue("event-archive-list-backlog", {
            archiveRetentionMs: 100,
          });
          const records = yield* Effect.forEach(Array.from({ length: 8 }), () =>
            EventQueue.emit(q, { value: "archived" }),
          );
          yield* Effect.sleep(150);
          expect(yield* EventQueue.listArchived(q)).toEqual([]);
          const redis = yield* RedisPool.RedisPool;
          // A single listing only cleans one batch; visibility must not depend on cleanup finishing.
          expect(
            yield* redis.send<number>("ZCARD", `${root(q.name)}:archives`),
          ).toBe(6);
          let remaining = yield* EventQueue.maintain(q);
          while (remaining.pending) remaining = yield* EventQueue.maintain(q);
          for (const record of records)
            expect(yield* EventQueue.get(q, record.id)).toBeNull();
        }),
    );

    it.effect(
      "rejects subscriber overflow while preserving idempotent registration at capacity",
      () =>
        Effect.gen(function* () {
          const q = queue("event-subscriber-capacity");
          const sub = yield* EventQueue.subscribe(q, "existing");
          const redis = yield* RedisPool.RedisPool;
          const fields = Array.from({ length: 999 }, (_, i) => [
            `subscriber-${i}`,
            `generation-${i}`,
          ]).flat();
          yield* redis.send("HSET", `${root(q.name)}:subscriptions`, ...fields);
          expect(yield* EventQueue.subscribe(q, "existing")).toEqual(sub);
          expect(
            yield* EventQueue.subscribe(q, "overflow").pipe(Effect.flip),
          ).toMatchObject({ code: "CapacityExceeded" });
          expect(
            yield* redis.send<number>("HLEN", `${root(q.name)}:subscriptions`),
          ).toBe(1000);
        }),
    );

    it.effect(
      "serializes final acknowledgement and removal without reviving a completed event",
      () =>
        Effect.gen(function* () {
          const q = queue("event-ack-remove-race");
          const a = yield* EventQueue.subscribe(q, "a");
          const b = yield* EventQueue.subscribe(q, "b");
          const event = yield* EventQueue.emit(q, { value: "race" });
          const delivery = yield* take(q, a);
          yield* Effect.all(
            [EventQueue.acknowledge(q, delivery), EventQueue.unsubscribe(q, b)],
            { concurrency: "unbounded" },
          );
          const archived = yield* EventQueue.get(q, event.id);
          expect(archived?.status).toBe("completed");
          expect(archived?.recipients[a.generation].status).toBe(
            "acknowledged",
          );
          expect(archived?.recipients[b.generation].status).toBe("waived");
          expect(yield* EventQueue.acknowledge(q, delivery)).toBe(
            "already-acknowledged",
          );
        }),
    );
  },
);
