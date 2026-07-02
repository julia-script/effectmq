import { RedisContainer } from "@testcontainers/redis";
import { Effect } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import { expect, test } from "vitest";
import { NodeRedisPool } from "./index.js";

test("provides a working Redis service backed by a node-redis pool", async () => {
  const container = await new RedisContainer("redis:7").start();
  try {
    const result = await Effect.gen(function* () {
      const redis = yield* Redis.Redis;
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
