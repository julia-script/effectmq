import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RedisContainer } from "@testcontainers/redis";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Redis from "effect/unstable/persistence/Redis";
import { Redis as IORedis } from "ioredis";
import { RedisPool, TaskEngine } from "../index.js";

export type TestInfrastructureOperation =
  | "container-start"
  | "container-stop"
  | "client-create"
  | "client-disconnect"
  | "directory-create"
  | "directory-remove"
  | "redis-server-start"
  | "redis-server-stop"
  | "redis-ready"
  | "sentinel-setup"
  | "sentinel-teardown";

export class TestInfrastructureError extends Data.TaggedError(
  "TestInfrastructureError",
)<{
  readonly operation: TestInfrastructureOperation;
  readonly cause: unknown;
}> {}

export type TestRedisAddressValue =
  | { readonly url: string }
  | { readonly socket: { readonly path: string; readonly tls: false } };

export class TestRedisAddress extends Context.Service<
  TestRedisAddress,
  TestRedisAddressValue
>()("effectmq/testing/TestRedisAddress") {}

export interface TestResourceRegistry {
  readonly active: Set<string>;
  nextId: number;
}

export const makeTestResourceRegistry = (): TestResourceRegistry => ({
  active: new Set(),
  nextId: 0,
});

export const acquireTracked = <A, E, R, E2, R2>(
  registry: TestResourceRegistry,
  label: string,
  acquire: Effect.Effect<A, E, R>,
  release: (resource: A) => Effect.Effect<unknown, E2, R2>,
) => {
  return Effect.acquireRelease(
    acquire.pipe(
      Effect.map((resource) => {
        registry.nextId += 1;
        const resourceId = `${label}#${registry.nextId}`;
        registry.active.add(resourceId);
        return { resource, resourceId } as const;
      }),
    ),
    ({ resource, resourceId }) =>
      release(resource).pipe(
        Effect.orDie,
        Effect.ensuring(
          Effect.sync(() => {
            registry.active.delete(resourceId);
          }),
        ),
      ),
  ).pipe(Effect.map(({ resource }) => resource));
};

const infrastructureError =
  (operation: TestInfrastructureOperation) => (cause: unknown) =>
    new TestInfrastructureError({ cause, operation });

const stopChild = (child: ChildProcess) =>
  Effect.tryPromise({
    try: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    },
    catch: infrastructureError("redis-server-stop"),
  });

const redisContainer = (registry: TestResourceRegistry, image: string) =>
  acquireTracked(
    registry,
    "redis-container",
    Effect.tryPromise({
      try: () => new RedisContainer(image).start(),
      catch: infrastructureError("container-start"),
    }),
    (container) =>
      Effect.tryPromise({
        try: () => container.stop(),
        catch: infrastructureError("container-stop"),
      }),
  );

const containerClient = (registry: TestResourceRegistry, image: string) =>
  Effect.gen(function* () {
    const container = yield* redisContainer(registry, image);
    const client = yield* acquireTracked(
      registry,
      "ioredis-client",
      Effect.try({
        try: () =>
          new IORedis({
            host: container.getHost(),
            port: container.getMappedPort(6379),
          }),
        catch: infrastructureError("client-create"),
      }),
      (client) =>
        Effect.try({
          try: () => client.disconnect(),
          catch: infrastructureError("client-disconnect"),
        }),
    );
    return {
      address: { url: container.getConnectionUrl() } as const,
      client,
    };
  });

const localServerClient = (registry: TestResourceRegistry) =>
  Effect.gen(function* () {
    const directory = yield* acquireTracked(
      registry,
      "redis-directory",
      Effect.try({
        try: () => mkdtempSync(join(tmpdir(), "effectmq-redis-")),
        catch: infrastructureError("directory-create"),
      }),
      (path) =>
        Effect.try({
          try: () => rmSync(path, { force: true, recursive: true }),
          catch: infrastructureError("directory-remove"),
        }),
    );
    const socket = join(directory, "redis.sock");
    yield* acquireTracked(
      registry,
      "redis-server",
      Effect.try({
        try: () =>
          spawn(
            "redis-server",
            ["--port", "0", "--unixsocket", socket, "--save", ""],
            { stdio: "ignore" },
          ),
        catch: infrastructureError("redis-server-start"),
      }),
      stopChild,
    );
    const client = yield* acquireTracked(
      registry,
      "ioredis-client",
      Effect.try({
        try: () => new IORedis({ path: socket, lazyConnect: true }),
        catch: infrastructureError("client-create"),
      }),
      (client) =>
        Effect.try({
          try: () => client.disconnect(),
          catch: infrastructureError("client-disconnect"),
        }),
    );
    yield* Effect.tryPromise({
      try: () => client.ping(),
      catch: infrastructureError("redis-ready"),
    }).pipe(
      Effect.retry({ schedule: Schedule.spaced("100 millis"), times: 50 }),
    );
    return {
      address: { socket: { path: socket, tls: false } } as const,
      client,
    };
  });

export const redisContainerLayer = ({
  image = "redis:7",
}: {
  readonly image?: string;
} = {}) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const registry = makeTestResourceRegistry();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (registry.active.size > 0) {
            throw new Error(
              `Leaked test resources: ${Array.from(registry.active).join(", ")}`,
            );
          }
        }),
      );

      const connection =
        process.env.EFFECTMQ_TEST_REDIS === "local"
          ? yield* localServerClient(registry)
          : yield* containerClient(registry, image);
      const { client } = connection;

      const toArg = (arg: string | Uint8Array) =>
        typeof arg === "string" || Buffer.isBuffer(arg)
          ? arg
          : Buffer.from(arg);

      const send = <A = unknown>(
        command: string,
        ...args: ReadonlyArray<string | Uint8Array>
      ) =>
        Effect.tryPromise({
          try: () => client.call(command, ...args.map(toArg)) as Promise<A>,
          catch: (cause) => new Redis.RedisError({ cause }),
        });

      const sendBinary = <A = unknown>(
        command: string,
        ...args: ReadonlyArray<string | Uint8Array>
      ) =>
        Effect.tryPromise({
          try: () =>
            client.callBuffer(command, ...args.map(toArg)) as Promise<A>,
          catch: (cause) => new Redis.RedisError({ cause }),
        });

      const redisPool = yield* RedisPool.make(send, sendBinary);
      const roles = RedisPool.makeConnectionRoles(
        redisPool,
        redisPool,
        redisPool,
      );
      const redis = yield* Redis.make({ send });
      return Context.make(RedisPool.RedisPool, redisPool).pipe(
        Context.add(RedisPool.RedisConnectionRoles, roles),
        Context.add(Redis.Redis, redis),
        Context.add(TestRedisAddress, connection.address),
      );
    }),
  );

const taskEngineLayer = TaskEngine.layerNoDeps({ debugMode: true });

export const TestLayer = Layer.merge(
  taskEngineLayer.pipe(Layer.provideMerge(redisContainerLayer())),
  NodeCrypto.layer,
);

export const getLists = (prefix: string) =>
  Effect.gen(function* () {
    const taskEngine = yield* TaskEngine.TaskEngine;
    return {
      wait: (yield* taskEngine.listTasks(prefix, "wait", { limit: 1_000 }))
        .items,
      scheduled: (yield* taskEngine.listTasks(prefix, "scheduled", {
        limit: 1_000,
      })).items,
      active: (yield* taskEngine.listTasks(prefix, "active", { limit: 1_000 }))
        .items,
      failed: (yield* taskEngine.listTasks(prefix, "failed", { limit: 1_000 }))
        .items,
      success: (yield* taskEngine.listTasks(prefix, "success", {
        limit: 1_000,
      })).items,
    };
  });
