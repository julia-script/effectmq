import { RedisContainer } from "@testcontainers/redis";
import { Effect } from "effect";
import { expect, test } from "vitest";
import { NodeRedisPool, RedisPool } from "./index.js";

const image = process.env.EFFECTMQ_COMPAT_REDIS_IMAGE ?? "redis:7.2-alpine";
const resp = Number(process.env.EFFECTMQ_COMPAT_RESP ?? "3") as 2 | 3;

test(`compatibility: ${image} over RESP${resp}`, async () => {
  const container = await new RedisContainer(image).start();
  try {
    const result = await Effect.gen(function* () {
      const redis = yield* RedisPool.RedisPool;
      yield* redis.send("SET", "effectmq:compat", `RESP${resp}`);
      const value = yield* redis.send<string>("GET", "effectmq:compat");
      const binary = new Uint8Array([0, 255, 1, 128]);
      yield* redis.sendBinary("SET", "effectmq:compat:binary", binary);
      const binaryResult = yield* redis.sendBinary<Uint8Array>(
        "GET",
        "effectmq:compat:binary",
      );
      return { binaryResult, value };
    }).pipe(
      Effect.provide(
        NodeRedisPool.layer({ RESP: resp, url: container.getConnectionUrl() }),
      ),
      Effect.runPromise,
    );
    expect(result.value).toBe(`RESP${resp}`);
    expect(Buffer.from(result.binaryResult)).toEqual(
      Buffer.from([0, 255, 1, 128]),
    );
  } finally {
    await container.stop();
  }
});
