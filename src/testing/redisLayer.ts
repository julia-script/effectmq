import { RedisContainer } from "@testcontainers/redis";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import { Redis as IORedis } from "ioredis";
import { RedisPool, TaskEngine } from "../index.js";

const redisContainer = (image: string) => {
  const ef = Effect.tryPromise({
    try: () => new RedisContainer(image).start(),
    catch: (cause) => new Redis.RedisError({ cause }),
  });

  return Effect.acquireRelease(ef, (container) =>
    Effect.promise(() => container.stop()),
  );
};
export const redisContainerLayer = ({
  image = "redis:7",
}: {
  image?: string;
} = {}) =>
  Effect.gen(function* () {
    const container = yield* redisContainer(image);

    const client = yield* Effect.acquireRelease(
      Effect.succeed(
        new IORedis({
          host: container.getHost(),
          port: container.getMappedPort(6379),
        }),
      ),
      (client) => Effect.succeed(client.disconnect()),
    );

    const send = <A = unknown>(
      command: string,
      ...args: ReadonlyArray<string>
    ) =>
      Effect.tryPromise({
        try: () => client.call(command, ...args) as Promise<A>,
        catch: (cause) => new Redis.RedisError({ cause }),
      });

    const redis = yield* Redis.make({ send });
    return Context.make(
      RedisPool.RedisPool,
      RedisPool.RedisPool.of({ send, eval: redis.eval }),
    ).pipe(Context.add(Redis.Redis, redis));
  }).pipe(Layer.effectContext);
const taskEngineLayer = TaskEngine.layer({
  debugMode: true,
});
// const

const layers = taskEngineLayer.pipe(Layer.provideMerge(redisContainerLayer()));
export const TestRuntime = ManagedRuntime.make(layers);
// Warm the runtime (container start + layer build) at import time so the
// first test in a file doesn't pay for it inside its own timeout budget.
await TestRuntime.runPromise(Effect.void);

export const getLists = (prefix: string) =>
  Effect.gen(function* () {
    const taskEngine = yield* TaskEngine.TaskEngine;
    return {
      wait: yield* taskEngine.getList(prefix, "wait"),
      scheduled: yield* taskEngine.getList(prefix, "scheduled"),
      active: yield* taskEngine.getList(prefix, "active"),
      failed: yield* taskEngine.getList(prefix, "failed"),
      success: yield* taskEngine.getList(prefix, "success"),
    };
  });

// // );
// export const startRedis = async () => {
//   const container = await new RedisContainer("redis:7").start();
//   const client = new IORedis({
//     host: container.getHost(),
//     port: container.getMappedPort(6379),
//   });

//   const redisService = Redis.make({
//     send: <A = unknown>(command: string, ...args: ReadonlyArray<string>) =>
//       Effect.tryPromise({
//         try: () => client.call(command, ...args) as Promise<A>,
//         catch: (cause) => new Redis.RedisError({ cause }),
//       }),
//   });

//   const layer = Layer.effect(Redis.Redis, redisService);

//   const stop = async () => {
//     client.disconnect();
//     await container.stop();
//   };

//   return { container, client, layer, stop };
// };

// export type StartedRedis = {
//   container: StartedRedisContainer;
//   client: IORedis;
//   layer: Layer.Layer<Redis.Redis>;
//   stop: () => Promise<void>;
// };
