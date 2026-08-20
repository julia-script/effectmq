import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Schedule } from "effect";
import { Redis as IORedis } from "ioredis";
import { afterAll, beforeAll, expect, test } from "vitest";
import { NodeRedisPool, RedisPool } from "./index.js";
import * as FaultInjection from "./testing/FaultInjection.js";

const enabled = process.env.EFFECTMQ_TEST_SENTINEL === "local";
const sentinelTest = enabled ? test : test.skip;

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

const waitFor = async (check: () => Promise<boolean>, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("timed out waiting for Redis Sentinel", { cause: lastError });
};

let directory = "";
let masterPort = 0;
let replicaPort = 0;
let sentinelPorts: ReadonlyArray<number> = [];
let master: ChildProcess | undefined;
let children: ReadonlyArray<ChildProcess> = [];

beforeAll(async () => {
  if (!enabled) return;
  directory = mkdtempSync(join(tmpdir(), "effectmq-sentinel-"));
  [masterPort, replicaPort, ...sentinelPorts] = await Promise.all(
    Array.from({ length: 5 }, availablePort),
  );
  const spawnRedis = (args: ReadonlyArray<string>) =>
    spawn("redis-server", [...args], { stdio: "ignore" });
  const masterDir = join(directory, "master");
  const replicaDir = join(directory, "replica");
  mkdirSync(masterDir);
  mkdirSync(replicaDir);
  master = spawnRedis([
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
        "sentinel down-after-milliseconds effectmq 500",
        "sentinel failover-timeout effectmq 2000",
        "sentinel parallel-syncs effectmq 1",
      ].join("\n"),
    );
    return spawnRedis([config, "--sentinel"]);
  });
  children = [master, replica, ...sentinels];

  const masterClient = new IORedis(masterPort, "127.0.0.1", {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
  });
  const sentinelClient = new IORedis(sentinelPorts[0], "127.0.0.1", {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
  });
  try {
    await waitFor(async () => (await masterClient.ping()) === "PONG");
    await waitFor(async () => {
      const address = (await sentinelClient.call(
        "SENTINEL",
        "get-master-addr-by-name",
        "effectmq",
      )) as [string, string] | null;
      return address?.[1] === String(masterPort);
    });
  } finally {
    masterClient.disconnect();
    sentinelClient.disconnect();
  }
}, 30_000);

afterAll(() => {
  for (const child of children) child.kill("SIGKILL");
  if (directory !== "") rmSync(directory, { force: true, recursive: true });
});

sentinelTest(
  "discovers a promoted primary and reloads scripts after failover",
  async () => {
    const script = "return ARGV[1]";
    const result = await Effect.gen(function* () {
      const redis = yield* RedisPool.RedisPool;
      const health = yield* NodeRedisPool.RedisConnectionHealth;
      const fault = yield* FaultInjection.make({ "sentinel-failover": 1 });
      expect(yield* redis.evalScript<string>(script, {}, "before")).toBe(
        "before",
      );

      const failoverFault = yield* fault
        .after(
          "sentinel-failover",
          Effect.sync(() => master?.kill("SIGKILL")),
        )
        .pipe(Effect.flip);
      expect(failoverFault).toMatchObject({ point: "sentinel-failover" });
      const sentinelClient = new IORedis(sentinelPorts[0], "127.0.0.1", {
        lazyConnect: true,
        maxRetriesPerRequest: 0,
      });
      yield* Effect.tryPromise({
        try: () =>
          waitFor(async () => {
            const address = (await sentinelClient.call(
              "SENTINEL",
              "get-master-addr-by-name",
              "effectmq",
            )) as [string, string] | null;
            return address?.[1] === String(replicaPort);
          }, 30_000).finally(() => sentinelClient.disconnect()),
        catch: (cause) => cause,
      });

      const after = yield* redis.evalScript<string>(script, {}, "after").pipe(
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
            sentinelRootNodes: sentinelPorts.map((port) => ({
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
      Effect.runPromise,
    );

    expect(result.after).toBe("after");
    expect(result.snapshot.topology).toBe("sentinel");
    expect(result.snapshot.roles.producer.reconnects).toBeGreaterThan(0);
  },
  60_000,
);
