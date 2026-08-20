import { expect, layer } from "@effect/vitest";
import { Effect } from "effect";
import { NodeRedisPool, RedisPool } from "./index.js";
import { redisContainerLayer, TestRedisAddress } from "./testing/redisLayer.js";

const image = process.env.EFFECTMQ_COMPAT_REDIS_IMAGE ?? "redis:7.2-alpine";
const resp = Number(process.env.EFFECTMQ_COMPAT_RESP ?? "3") as 2 | 3;

layer(redisContainerLayer({ image }), {
  excludeTestServices: true,
  timeout: "60 seconds",
})(`Redis compatibility (real Redis time)`, (it) => {
  it.effect(`compatibility: ${image} over RESP${resp}`, () =>
    Effect.gen(function* () {
      const address = yield* TestRedisAddress;
      const result = yield* Effect.gen(function* () {
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
      }).pipe(Effect.provide(NodeRedisPool.layer({ ...address, RESP: resp })));
      expect(result.value).toBe(`RESP${resp}`);
      expect(Buffer.from(result.binaryResult)).toEqual(
        Buffer.from([0, 255, 1, 128]),
      );
    }),
  );
});
