import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, layer } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { Redis as IORedis } from "ioredis";
import { NodeRedisPool, RedisPool, TaskEngine } from "./index.js";
import * as FaultInjection from "./testing/FaultInjection.js";
import { TestInfrastructureError } from "./testing/redisLayer.js";

const stopRedis = (child: ChildProcess) =>
  Effect.tryPromise({
    try: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    },
    catch: (cause) =>
      new TestInfrastructureError({
        cause,
        operation: "redis-server-stop",
      }),
  });

const startRedis = Effect.fnUntraced(function* (
  socket: string,
  directory: string,
) {
  const child = yield* Effect.try({
    try: () =>
      spawn(
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
      ),
    catch: (cause) =>
      new TestInfrastructureError({
        cause,
        operation: "redis-server-start",
      }),
  });

  yield* Effect.scoped(
    Effect.gen(function* () {
      const probe = yield* Effect.acquireRelease(
        Effect.try({
          try: () => new IORedis({ path: socket, lazyConnect: true }),
          catch: (cause) =>
            new TestInfrastructureError({ cause, operation: "client-create" }),
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
        try: () => probe.ping(),
        catch: (cause) =>
          new TestInfrastructureError({ cause, operation: "redis-ready" }),
      }).pipe(
        Effect.retry({
          times: 100,
          schedule: Schedule.spaced("20 millis"),
        }),
      );
    }),
  ).pipe(Effect.tapError(() => stopRedis(child).pipe(Effect.orDie)));

  return child;
});

class RestartFixture extends Context.Service<
  RestartFixture,
  {
    readonly socket: string;
    readonly restart: Effect.Effect<void, TestInfrastructureError>;
  }
>()("effectmq/testing/RestartFixture") {}

const restartFixtureLayer = Layer.effect(
  RestartFixture,
  Effect.gen(function* () {
    const directory = yield* Effect.acquireRelease(
      Effect.try({
        try: () => mkdtempSync(join(tmpdir(), "effectmq-restart-")),
        catch: (cause) =>
          new TestInfrastructureError({
            cause,
            operation: "directory-create",
          }),
      }),
      (path) =>
        Effect.try({
          try: () => rmSync(path, { recursive: true, force: true }),
          catch: (cause) =>
            new TestInfrastructureError({
              cause,
              operation: "directory-remove",
            }),
        }).pipe(Effect.orDie),
    );
    const socket = join(directory, "redis.sock");
    let server = yield* startRedis(socket, directory);
    yield* Effect.addFinalizer(() => stopRedis(server).pipe(Effect.orDie));

    return RestartFixture.of({
      socket,
      restart: Effect.gen(function* () {
        yield* stopRedis(server);
        server = yield* startRedis(socket, directory);
      }),
    });
  }),
);

if (process.env.EFFECTMQ_TEST_REDIS === "local") {
  layer(restartFixtureLayer, {
    excludeTestServices: true,
    timeout: "20 seconds",
  })("Redis restart (real Redis time)", (it) => {
    it.effect(
      "a current attempt survives Redis restart and reloads its script",
      () =>
        Effect.gen(function* () {
          const fixture = yield* RestartFixture;
          const task = yield* Effect.gen(function* () {
            const engine = yield* TaskEngine.TaskEngine;
            const redis = yield* RedisPool.RedisPool;
            const fault = yield* FaultInjection.make({
              reconnect: 1,
              restart: 1,
            });
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
            if (attempt === null)
              return yield* Effect.die("Expected an attempt");
            yield* redis.send("SAVE");

            const restartFault = yield* fault
              .after("restart", fixture.restart)
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
              Layer.merge(
                Layer.provideMerge(
                  TaskEngine.layerNoDeps(),
                  NodeRedisPool.layer({
                    socket: { path: fixture.socket, tls: false },
                  }),
                ),
                NodeCrypto.layer,
              ),
            ),
          );

          expect(task).toMatchObject({
            outcome: "success",
            success: "after-restart",
          });
        }),
      20_000,
    );
  });
} else {
  it.skip("a current attempt survives Redis restart and reloads its script", () =>
    Effect.void);
}
