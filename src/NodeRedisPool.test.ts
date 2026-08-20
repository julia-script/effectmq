import { RedisContainer } from "@testcontainers/redis";
import { Effect, Layer } from "effect";
import { expect, test } from "vitest";
import { NodeRedisPool, RedisPool, TaskEngine } from "./index.js";

test("provides a working RedisPool service backed by a node-redis pool", async () => {
  const container = await new RedisContainer("redis:7").start();
  try {
    const result = await Effect.gen(function* () {
      const redis = yield* RedisPool.RedisPool;
      const health = yield* NodeRedisPool.RedisConnectionHealth;
      expect(yield* health.readiness).toBe(true);
      yield* redis.send("SET", "node-redis-pool-test", "pong");
      const value = yield* redis.send<string>("GET", "node-redis-pool-test");
      const snapshot = yield* health.snapshot;
      expect(snapshot.ready).toBe(true);
      expect(snapshot.topology).toBe("standalone");
      expect(JSON.stringify(snapshot)).not.toContain("redis://");
      return value;
    }).pipe(
      Effect.provide(
        NodeRedisPool.layer({ url: container.getConnectionUrl() }),
      ),
      Effect.runPromise,
    );
    expect(result).toBe("pong");
  } finally {
    await container.stop();
  }
});

test("fails before connecting when Redis Cluster is configured", async () => {
  const error = await Effect.gen(function* () {
    yield* RedisPool.RedisPool;
  }).pipe(
    Effect.provide(NodeRedisPool.layer({ topology: "cluster" })),
    Effect.flip,
    Effect.runPromise,
  );

  expect(error).toBeInstanceOf(NodeRedisPool.UnsupportedRedisTopology);
  expect(error.topology).toBe("cluster");
});

test("rejects unbounded or inconsistent pool configuration", async () => {
  const error = await Effect.gen(function* () {
    yield* RedisPool.RedisPool;
  }).pipe(
    Effect.provide(NodeRedisPool.layer({ pool: { minimum: 5, maximum: 2 } })),
    Effect.flip,
    Effect.runPromise,
  );

  expect(error).toBeInstanceOf(NodeRedisPool.InvalidRedisConfiguration);
});

// the integration suite runs the engine through ioredis; this exercises the
// node-redis binary reply path (typeMapping) for msgpack round-trips
test("TaskEngine msgpack round-trip through the node-redis pool", async () => {
  const container = await new RedisContainer("redis:7").start();
  try {
    const payload = { nested: { value: [1, null, "✓"] }, n: 1.5 };
    const result = await Effect.gen(function* () {
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
          TaskEngine.layer(),
          NodeRedisPool.layer({ url: container.getConnectionUrl() }),
        ),
      ),
      Effect.runPromise,
    );
    expect(result?.payload).toEqual(payload);
    expect(result?.errors).toEqual([]);
  } finally {
    await container.stop();
  }
});
