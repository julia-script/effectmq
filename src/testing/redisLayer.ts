import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RedisContainer } from "@testcontainers/redis";
import { Context, Effect, Layer, ManagedRuntime, Schedule } from "effect";
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

const containerClient = (image: string) =>
  Effect.gen(function* () {
    const container = yield* redisContainer(image);
    return yield* Effect.acquireRelease(
      Effect.succeed(
        new IORedis({
          host: container.getHost(),
          port: container.getMappedPort(6379),
        }),
      ),
      (client) => Effect.succeed(client.disconnect()),
    );
  });

// A `redis-server` child process on a per-worker unix socket, for environments
// without Docker (opt in with EFFECTMQ_TEST_REDIS=local).
const localServerClient = () =>
  Effect.gen(function* () {
    const socket = join(
      mkdtempSync(join(tmpdir(), "effectmq-redis-")),
      "redis.sock",
    );
    yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          spawn(
            "redis-server",
            ["--port", "0", "--unixsocket", socket, "--save", ""],
            { stdio: "ignore" },
          ),
        catch: (cause) => new Redis.RedisError({ cause }),
      }),
      (child) => Effect.sync(() => child.kill()),
    );
    const client = yield* Effect.acquireRelease(
      Effect.succeed(new IORedis({ path: socket, lazyConnect: true })),
      (client) => Effect.succeed(client.disconnect()),
    );
    // the server needs a moment to create the socket; retry until it answers
    yield* Effect.tryPromise({
      try: () => client.ping(),
      catch: (cause) => new Redis.RedisError({ cause }),
    }).pipe(
      Effect.retry({ schedule: Schedule.spaced("100 millis"), times: 50 }),
    );
    return client;
  });

export const redisContainerLayer = ({
  image = "redis:7",
}: {
  image?: string;
} = {}) =>
  Effect.gen(function* () {
    const client =
      process.env.EFFECTMQ_TEST_REDIS === "local"
        ? yield* localServerClient()
        : yield* containerClient(image);

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
