import { RedisContainer } from "@testcontainers/redis";
import { Effect } from "effect";
import { expect, test } from "vitest";
import { NodeRedisPool, RedisPool } from "./index.js";

test("provides a working RedisPool service backed by a node-redis pool", async () => {
  const container = await new RedisContainer("redis:7").start();
  try {
    const result = await Effect.gen(function* () {
      const redis = yield* RedisPool.RedisPool;
      yield* redis.send("SET", "node-redis-pool-test", "pong");
      return yield* redis.send<string>("GET", "node-redis-pool-test");
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
