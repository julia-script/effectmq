import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Layer, Schedule } from "effect";
import { Redis as IORedis } from "ioredis";
import { expect, test } from "vitest";
import { NodeRedisPool, RedisPool, TaskEngine } from "./index.js";
import * as FaultInjection from "./testing/FaultInjection.js";

const startRedis = async (socket: string, directory: string) => {
  const child = spawn(
    "redis-server",
    [
      "--port",
      "0",
      "--unixsocket",
      socket,
      "--dir",
      directory,
      "--dbfilename",
      "restart.rdb",
      "--save",
      "",
    ],
    { stdio: "ignore" },
  );
  const probe = new IORedis({ path: socket, lazyConnect: true });
  try {
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        await probe.ping();
        return child;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    throw new Error("Redis restart probe timed out");
  } finally {
    probe.disconnect();
  }
};

const stopRedis = async (child: ChildProcess | undefined) => {
  if (!child || child.exitCode !== null) return;
  child.kill("SIGTERM");
  await once(child, "exit");
};

test.skipIf(process.env.EFFECTMQ_TEST_REDIS !== "local")(
  "a current attempt survives Redis restart and reloads its script",
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "effectmq-restart-"));
    const socket = join(directory, "redis.sock");
    let server: ChildProcess | undefined;
    try {
      server = await startRedis(socket, directory);
      const program = Effect.gen(function* () {
        const engine = yield* TaskEngine.TaskEngine;
        const redis = yield* RedisPool.RedisPool;
        const fault = yield* FaultInjection.make({ reconnect: 1, restart: 1 });
        const prefix = "redis-restart";
        yield* engine.createTask({
          prefix,
          id: "restart",
          name: "restart",
          payload: null,
          delay: 0,
          maxRetries: 0,
          onSuccessPolicy: "keep",
          onFailurePolicy: "keep",
        });
        const attempt = yield* engine.takeTask(prefix, 30_000);
        if (attempt === null) return yield* Effect.die("Expected an attempt");
        yield* redis.send("SAVE");

        const restartFault = yield* fault
          .after(
            "restart",
            Effect.promise(async () => {
              await stopRedis(server);
              server = await startRedis(socket, directory);
            }),
          )
          .pipe(Effect.flip);
        expect(restartFault).toMatchObject({ point: "restart" });

        const reconnectFault = yield* fault
          .before(
            "reconnect",
            engine.writeSuccess(
              prefix,
              attempt.task.id,
              attempt.leaseToken,
              "after-restart",
            ),
          )
          .pipe(Effect.flip);
        expect(reconnectFault).toMatchObject({ point: "reconnect" });

        // The process restart clears Redis' script cache. The cached digest
        // therefore takes the NOSCRIPT reload path before acknowledging.
        yield* engine
          .writeSuccess(
            prefix,
            attempt.task.id,
            attempt.leaseToken,
            "after-restart",
          )
          .pipe(
            Effect.retry({
              times: 50,
              schedule: Schedule.spaced("20 millis"),
            }),
          );
        return yield* engine.getTask(prefix, "restart");
      }).pipe(
        Effect.provide(
          Layer.provideMerge(
            TaskEngine.layer(),
            NodeRedisPool.layer({ socket: { path: socket } }),
          ),
        ),
      );

      const task = await Effect.runPromise(program);
      expect(task).toMatchObject({
        outcome: "success",
        success: "after-restart",
      });
    } finally {
      await stopRedis(server);
      rmSync(directory, { recursive: true, force: true });
    }
  },
  20_000,
);
