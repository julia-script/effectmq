import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import { Context, Effect, Schema, SchemaGetter } from "effect";
import type * as Crypto from "effect/Crypto";
import * as Redis from "effect/unstable/persistence/Redis";
import { expectTypeOf } from "vitest";
import * as EventEngine from "./EventEngine.js";
import * as EventQueue from "./EventQueue.js";
import type * as EventRecord from "./EventRecord.js";
import * as RedisPool from "./RedisPool.js";

const replying = (raw: unknown): RedisPool.RedisPoolService =>
  RedisPool.RedisPool.of({
    send: <A>() => Effect.succeed(raw as A),
    sendBinary: <A>() => Effect.succeed(raw as A),
    evalScript: <A>() => Effect.succeed(raw as A),
  });

it.effect(
  "rejects malformed Redis replies instead of reporting an empty queue",
  () =>
    Effect.gen(function* () {
      for (const raw of [
        null,
        [],
        "not-json",
        '{"ok":true,"value":false}',
        '{"ok":false,"code":"unknown","message":"bad"}',
      ]) {
        const engine = yield* EventEngine.makeWithRedis(replying(raw));
        expect(
          yield* engine.get({ name: "test" }, "id").pipe(Effect.flip),
        ).toMatchObject({ code: "CorruptStorage" });
      }
    }),
);

it.effect(
  "preserves uncertain mutation identity and never retries an emission automatically",
  () =>
    Effect.gen(function* () {
      let invocations = 0;
      const base = replying(null);
      const redis: RedisPool.RedisPoolService = {
        ...base,
        evalScript: () => {
          invocations++;
          return Effect.fail(
            new Redis.RedisError({ cause: new Error("connection dropped") }),
          );
        },
      };
      const engine = yield* EventEngine.makeWithRedis(redis);
      const error = yield* engine
        .emit({ name: "test" }, "encoded")
        .pipe(Effect.flip, Effect.provide(NodeCrypto.layer));
      expect(error).toMatchObject({
        code: "IndeterminateWrite",
        operation: "emit",
        eventId: expect.any(String),
      });
      expect(invocations).toBe(1);
    }),
);

it.effect(
  "rejects runtime null leases and malformed names before sending commands",
  () =>
    Effect.gen(function* () {
      let invocations = 0;
      const base = replying(null);
      const redis: RedisPool.RedisPoolService = {
        ...base,
        evalScript: <A>() => {
          invocations++;
          return Effect.succeed(null as A);
        },
      };
      const engine = yield* EventEngine.makeWithRedis(redis);
      const sub = { queue: "test", name: "worker", generation: "generation" };
      const attempt = { ...sub, id: "id", token: "token" };
      const operations: ReadonlyArray<
        Effect.Effect<unknown, EventEngine.EventEngineError, Crypto.Crypto>
      > = [
        engine.take({ name: "test" }, sub, null as unknown as number),
        engine.renew({ name: "test" }, attempt, null as unknown as number),
        engine.release({ name: "test" }, attempt, null as unknown as number),
        engine.subscribe({ name: "test" }, "\uD800"),
        engine.subscribe({ name: "test" }, "x".repeat(257)),
      ];
      for (const operation of operations) {
        expect(
          yield* operation.pipe(Effect.flip, Effect.provide(NodeCrypto.layer)),
        ).toMatchObject({ code: "InvalidInput" });
      }
      expect(invocations).toBe(0);
    }),
);

it.effect(
  "routes delivery and maintenance to isolated Redis workload roles",
  () =>
    Effect.gen(function* () {
      const invoked: string[] = [];
      const connection = (role: string): RedisPool.RedisPoolService => ({
        ...replying(null),
        evalScript: <A>() =>
          Effect.sync(() => {
            invoked.push(role);
            return (
              role === "maintenance"
                ? '{"ok":true,"value":{"processed":0,"pending":false}}'
                : '{"ok":true,"value":null}'
            ) as A;
          }),
      });
      const producer = connection("producer");
      const engine = yield* EventEngine.makeWithRedis(
        producer,
        {},
        {
          producer,
          worker: connection("worker"),
          maintenance: connection("maintenance"),
        },
      );
      yield* engine.get({ name: "test" }, "id");
      yield* engine
        .take(
          { name: "test" },
          { queue: "test", name: "worker", generation: "g" },
        )
        .pipe(Effect.provide(NodeCrypto.layer));
      yield* engine.maintain({ name: "test" });
      expect(invoked).toEqual(["producer", "worker", "maintenance"]);
    }),
);

class Encoder extends Context.Service<Encoder, { readonly prefix: string }>()(
  "event-test/Encoder",
) {}
class Decoder extends Context.Service<Decoder, { readonly prefix: string }>()(
  "event-test/Decoder",
) {}
const serviceSchema = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) =>
      Decoder.pipe(Effect.map(({ prefix }) => prefix + value)),
    ),
    encode: SchemaGetter.transformEffect((value) =>
      Encoder.pipe(Effect.map(({ prefix }) => prefix + value)),
    ),
  }),
);
const serviceQueue = EventQueue.make("service-types", serviceSchema);
const subscription: EventRecord.Subscription = {
  queue: "service-types",
  name: "worker",
  generation: "g",
};

it("retains exact schema service and error channels on public queue operations", () => {
  const emitted = EventQueue.emit(serviceQueue, "value");
  const read = EventQueue.get(serviceQueue, "id");
  const taken = EventQueue.take(serviceQueue, subscription);
  const processed = EventQueue.processOne(serviceQueue, subscription, () =>
    Encoder.pipe(Effect.andThen(Effect.fail("handler" as const))),
  );
  expectTypeOf<Effect.Services<typeof emitted>>().toEqualTypeOf<
    EventEngine.EventEngine | Crypto.Crypto | Encoder
  >();
  expectTypeOf<Effect.Services<typeof read>>().toEqualTypeOf<
    EventEngine.EventEngine | Decoder
  >();
  expectTypeOf<Effect.Services<typeof taken>>().toEqualTypeOf<
    EventEngine.EventEngine | Crypto.Crypto | Decoder
  >();
  expectTypeOf<Effect.Services<typeof processed>>().toEqualTypeOf<
    EventEngine.EventEngine | Crypto.Crypto | Decoder | Encoder
  >();
  expectTypeOf<Effect.Error<typeof processed>>().toEqualTypeOf<
    EventQueue.Error | "handler"
  >();
  expectTypeOf<Effect.Success<typeof emitted>>().toEqualTypeOf<
    EventRecord.Event<string>
  >();
});
