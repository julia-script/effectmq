import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, layer } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { Redis as IORedis } from "ioredis";
import { NodeRedisPool, RedisPool } from "./index.js";
import * as FaultInjection from "./testing/FaultInjection.js";
import { TestInfrastructureError } from "./testing/redisLayer.js";

const availablePort = () =>
  new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("could not allocate test port"));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });

const waitFor = async (
  description: string,
  check: () => Promise<boolean>,
  timeoutMs = 15_000,
) => {
  const deadline = Date.now() + timeoutMs;
  let attempts = 0;
  let lastError: unknown;
  while (Date.now() < deadline) {
    attempts += 1;
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(
    `timed out waiting for ${description} after ${attempts} attempts`,
    {
      cause: lastError,
    },
  );
};

const stopChild = async (child: ChildProcess) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
};

interface StartedSentinel {
  readonly directory: string;
  readonly master: ChildProcess;
  readonly masterPort: number;
  readonly replicaPort: number;
  readonly sentinelPorts: ReadonlyArray<number>;
  readonly children: ReadonlyArray<ChildProcess>;
}

const stopSentinel = async (fixture: StartedSentinel) => {
  await Promise.all(fixture.children.map(stopChild));
  rmSync(fixture.directory, { force: true, recursive: true });
};

const startSentinel = async (): Promise<StartedSentinel> => {
  const directory = mkdtempSync(join(tmpdir(), "effectmq-sentinel-"));
  const [masterPort, replicaPort, ...sentinelPorts] = await Promise.all(
    Array.from({ length: 5 }, availablePort),
  );
  const spawnRedis = (args: ReadonlyArray<string>) =>
    spawn("redis-server", [...args], { stdio: "ignore" });
  const masterDir = join(directory, "master");
  const replicaDir = join(directory, "replica");
  mkdirSync(masterDir);
  mkdirSync(replicaDir);
  const master = spawnRedis([
    "--port",
    String(masterPort),
    "--bind",
    "127.0.0.1",
    "--protected-mode",
    "no",
    "--save",
    "",
    "--appendonly",
    "no",
    "--dir",
    masterDir,
  ]);
  const replica = spawnRedis([
    "--port",
    String(replicaPort),
    "--bind",
    "127.0.0.1",
    "--protected-mode",
    "no",
    "--save",
    "",
    "--appendonly",
    "no",
    "--dir",
    replicaDir,
    "--replicaof",
    "127.0.0.1",
    String(masterPort),
  ]);
  const sentinels = sentinelPorts.map((port, index) => {
    const sentinelDir = join(directory, `sentinel-${index}`);
    mkdirSync(sentinelDir);
    const config = join(sentinelDir, "sentinel.conf");
    writeFileSync(
      config,
      [
        `port ${port}`,
        "bind 127.0.0.1",
        "protected-mode no",
        `dir ${sentinelDir}`,
        `sentinel monitor effectmq 127.0.0.1 ${masterPort} 2`,
        "sentinel down-after-milliseconds effectmq 2000",
        "sentinel failover-timeout effectmq 10000",
        "sentinel parallel-syncs effectmq 1",
      ].join("\n"),
    );
    return spawnRedis([config, "--sentinel"]);
  });
  const fixture = {
    children: [master, replica, ...sentinels],
    directory,
    master,
    masterPort,
    replicaPort,
    sentinelPorts,
  } satisfies StartedSentinel;

  const masterClient = new IORedis(masterPort, "127.0.0.1", {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
  });
  const replicaClient = new IORedis(replicaPort, "127.0.0.1", {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
  });
  const sentinelClients = sentinelPorts.map(
    (port) =>
      new IORedis(port, "127.0.0.1", {
        lazyConnect: true,
        maxRetriesPerRequest: 0,
      }),
  );
  try {
    await waitFor(
      "Redis master readiness",
      async () => (await masterClient.ping()) === "PONG",
    );
    await waitFor("Redis replica synchronization", async () => {
      const replication = await replicaClient.info("replication");
      return (
        replication.includes("role:slave") &&
        replication.includes("master_link_status:up")
      );
    });
    for (const client of sentinelClients) {
      await waitFor("Sentinel quorum", async () => {
        const address = (await client.call(
          "SENTINEL",
          "get-master-addr-by-name",
          "effectmq",
        )) as [string, string] | null;
        const peers = (await client.call(
          "SENTINEL",
          "sentinels",
          "effectmq",
        )) as ReadonlyArray<unknown>;
        const quorum = String(
          await client.call("SENTINEL", "ckquorum", "effectmq"),
        );
        return (
          address?.[1] === String(masterPort) &&
          peers.length >= 2 &&
          quorum.startsWith("OK")
        );
      });
    }
    return fixture;
  } catch (error) {
    await stopSentinel(fixture);
    throw error;
  } finally {
    masterClient.disconnect();
    replicaClient.disconnect();
    for (const client of sentinelClients) client.disconnect();
  }
};

class SentinelFixture extends Context.Service<
  SentinelFixture,
  StartedSentinel
>()("effectmq/testing/SentinelFixture") {}

const sentinelFixtureLayer = Layer.effect(
  SentinelFixture,
  Effect.acquireRelease(
    Effect.tryPromise({
      try: startSentinel,
      catch: (cause) =>
        new TestInfrastructureError({ cause, operation: "sentinel-setup" }),
    }),
    (fixture) =>
      Effect.tryPromise({
        try: () => stopSentinel(fixture),
        catch: (cause) =>
          new TestInfrastructureError({
            cause,
            operation: "sentinel-teardown",
          }),
      }).pipe(Effect.orDie),
  ),
);

if (process.env.EFFECTMQ_TEST_SENTINEL === "local") {
  layer(sentinelFixtureLayer, {
    excludeTestServices: true,
    timeout: "30 seconds",
  })("Redis Sentinel failover (real Redis time)", (it) => {
    it.effect(
      "discovers a promoted primary and reloads scripts after failover",
      () =>
        Effect.gen(function* () {
          const fixture = yield* SentinelFixture;
          const result = yield* Effect.gen(function* () {
            const redis = yield* RedisPool.RedisPool;
            const health = yield* NodeRedisPool.RedisConnectionHealth;
            const fault = yield* FaultInjection.make({
              "sentinel-failover": 1,
            });
            const script = "return ARGV[1]";
            expect(yield* redis.evalScript<string>(script, {}, "before")).toBe(
              "before",
            );

            yield* health.readiness.pipe(
              Effect.repeat({
                schedule: Schedule.spaced("100 millis"),
                until: (ready) => ready,
              }),
            );

            const failoverFault = yield* fault
              .after(
                "sentinel-failover",
                Effect.try({
                  try: () => fixture.master.kill("SIGKILL"),
                  catch: (cause) =>
                    new TestInfrastructureError({
                      cause,
                      operation: "sentinel-teardown",
                    }),
                }),
              )
              .pipe(Effect.flip);
            expect(failoverFault).toMatchObject({ point: "sentinel-failover" });

            const sentinelClient = yield* Effect.acquireRelease(
              Effect.try({
                try: () =>
                  new IORedis(fixture.sentinelPorts[0], "127.0.0.1", {
                    lazyConnect: true,
                    maxRetriesPerRequest: 0,
                  }),
                catch: (cause) =>
                  new TestInfrastructureError({
                    cause,
                    operation: "client-create",
                  }),
              }),
              (client) =>
                Effect.try({
                  try: () => client.disconnect(),
                  catch: (cause) =>
                    new TestInfrastructureError({
                      cause,
                      operation: "client-disconnect",
                    }),
                }).pipe(Effect.orDie),
            );
            yield* Effect.tryPromise({
              try: async () => {
                const address = (await sentinelClient.call(
                  "SENTINEL",
                  "get-master-addr-by-name",
                  "effectmq",
                )) as [string, string] | null;
                return address?.[1] === String(fixture.replicaPort);
              },
              catch: (cause) =>
                new TestInfrastructureError({
                  cause,
                  operation: "redis-ready",
                }),
            }).pipe(
              Effect.repeat({
                schedule: Schedule.spaced("100 millis"),
                until: (promoted) => promoted,
              }),
              Effect.timeout("30 seconds"),
            );

            const after = yield* redis
              .evalScript<string>(script, {}, "after")
              .pipe(
                Effect.retry({
                  schedule: Schedule.spaced("100 millis"),
                  times: 200,
                }),
              );
            const snapshot = yield* health.snapshot;
            return { after, snapshot };
          }).pipe(
            Effect.provide(
              NodeRedisPool.layer({
                topology: "sentinel",
                sentinel: {
                  name: "effectmq",
                  sentinelRootNodes: fixture.sentinelPorts.map((port) => ({
                    host: "127.0.0.1",
                    port,
                  })),
                  masterPoolSize: 4,
                  maxCommandRediscovers: 20,
                  passthroughClientErrorEvents: true,
                  scanInterval: 100,
                  commandOptions: { timeout: 1_000 },
                  nodeClientOptions: {
                    socket: { connectTimeout: 500 },
                  },
                  sentinelClientOptions: {
                    socket: { connectTimeout: 500 },
                  },
                },
              }),
            ),
          );

          expect(result.after).toBe("after");
          expect(result.snapshot.topology).toBe("sentinel");
          expect(result.snapshot.roles.producer.reconnects).toBeGreaterThan(0);
        }),
      60_000,
    );
  });
} else {
  it.skip("discovers a promoted primary and reloads scripts after failover", () =>
    Effect.void);
}
