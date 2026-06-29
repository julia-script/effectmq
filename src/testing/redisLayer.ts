import {
  RedisContainer,
  type StartedRedisContainer,
} from "@testcontainers/redis";
import { Effect, Layer } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import { Redis as IORedis } from "ioredis";


export const startRedis = async () => {
  const container = await new RedisContainer("redis:7").start();
  const client = new IORedis({
    host: container.getHost(),
    port: container.getMappedPort(6379),
  });

  const redisService = Redis.make({
    send: <A = unknown>(command: string, ...args: ReadonlyArray<string>) =>
      Effect.tryPromise({
        try: () => client.call(command, ...args) as Promise<A>,
        catch: (cause) => new Redis.RedisError({ cause }),
      }),
  });

  const layer = Layer.effect(Redis.Redis, redisService);

  const stop = async () => {
    client.disconnect();
    await container.stop();
  };

  return { container, client, layer, stop };
};

export type StartedRedis = {
  container: StartedRedisContainer;
  client: IORedis;
  layer: Layer.Layer<Redis.Redis>;
  stop: () => Promise<void>;
};
