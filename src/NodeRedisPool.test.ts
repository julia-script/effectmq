import { expect, layer } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { NodeRedisPool, RedisPool, TaskEngine } from "./index.js";
import { TestLayer, TestRedisAddress } from "./testing/redisLayer.js";

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "NodeRedisPool (real Redis time)",
  (it) => {
    it.effect(
      "provides a working RedisPool service backed by a node-redis pool",
      () =>
        Effect.gen(function* () {
          const address = yield* TestRedisAddress;
          const result = yield* Effect.gen(function* () {
            const redis = yield* RedisPool.RedisPool;
            const health = yield* NodeRedisPool.RedisConnectionHealth;
            expect(yield* health.readiness).toBe(true);
            yield* redis.send("SET", "node-redis-pool-test", "pong");
            const value = yield* redis.send<string>(
              "GET",
              "node-redis-pool-test",
            );
            const snapshot = yield* health.snapshot;
            expect(snapshot.ready).toBe(true);
            expect(snapshot.topology).toBe("standalone");
            expect(JSON.stringify(snapshot)).not.toContain("redis://");
            return value;
          }).pipe(Effect.provide(NodeRedisPool.layer(address)));
          expect(result).toBe("pong");
        }),
    );

    it.effect("fails before connecting when Redis Cluster is configured", () =>
      Effect.gen(function* () {
        const error = yield* Effect.gen(function* () {
          yield* RedisPool.RedisPool;
        }).pipe(
          Effect.provide(NodeRedisPool.layer({ topology: "cluster" })),
          Effect.flip,
        );

        expect(error).toBeInstanceOf(NodeRedisPool.UnsupportedRedisTopology);
        if (!(error instanceof NodeRedisPool.UnsupportedRedisTopology)) {
          throw new Error("Expected UnsupportedRedisTopology");
        }
        expect(error.topology).toBe("cluster");
      }),
    );

    it.effect("rejects unbounded or inconsistent pool configuration", () =>
      Effect.gen(function* () {
        const error = yield* Effect.gen(function* () {
          yield* RedisPool.RedisPool;
        }).pipe(
          Effect.provide(
            NodeRedisPool.layer({ pool: { minimum: 5, maximum: 2 } }),
          ),
          Effect.flip,
        );

        expect(error).toBeInstanceOf(NodeRedisPool.InvalidRedisConfiguration);
      }),
    );

    // the integration suite runs the engine through ioredis; this exercises the
    // node-redis binary reply path (typeMapping) for msgpack round-trips
    it.effect("TaskEngine msgpack round-trip through the node-redis pool", () =>
      Effect.gen(function* () {
        const address = yield* TestRedisAddress;
        const payload = { nested: { value: [1, null, "✓"] }, n: 1.5 };
        const result = yield* Effect.gen(function* () {
          const engine = yield* TaskEngine.TaskEngine;
          const prefix = "node-redis-msgpack";
          yield* engine.createTask({
            id: "nr1",
            name: "t",
            payload,
            delay: 0,
            maxRetries: 0,
            onSuccessPolicy: "keep",
            onFailurePolicy: "keep",
            prefix,
          });
          return (yield* engine.takeTask(prefix, 30000))?.task;
        }).pipe(
          Effect.provide(
            Layer.provideMerge(
              TaskEngine.layerNoDeps(),
              NodeRedisPool.layer(address),
            ),
          ),
        );
        expect(result?.payload).toEqual(payload);
        expect(result?.errors).toEqual([]);
      }),
    );
  },
);
