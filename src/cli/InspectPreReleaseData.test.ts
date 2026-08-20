import { expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Redis from "effect/unstable/persistence/Redis";
import { configuration, inspect } from "./InspectPreReleaseData.js";
import * as RedisPool from "../RedisPool.js";

const service = (send: RedisPool.RedisSend): RedisPool.RedisPoolService =>
  RedisPool.RedisPool.of({
    send,
    sendBinary: send,
    evalScript: () =>
      Effect.fail(new Redis.RedisError({ cause: "not used by inspection" })),
  });

it.effect("requires the Redis URL through Effect Config", () =>
  Effect.gen(function* () {
    const error = yield* configuration.pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({}),
      ),
      Effect.flip,
    );
    expect(error._tag).toBe("ConfigError");
  }),
);

it.effect("scans all pages and reports legacy keys", () =>
  Effect.gen(function* () {
    const replies: unknown[] = [
      ["7", ["~effectmq:v1:current", "~effectmq:legacy:a"]],
      ["0", ["~effectmq:legacy:b"]],
    ];
    const result = yield* inspect({
      match: "~effectmq:*",
      count: 100,
      assertDrained: false,
    }).pipe(
      Effect.provideService(
        RedisPool.RedisPool,
        service(<A>() => Effect.succeed(replies.shift() as A)),
      ),
    );
    expect(result).toMatchObject({
      status: "pre-v1-data-found",
      legacyKeyCount: 2,
      keys: ["~effectmq:legacy:a", "~effectmq:legacy:b"],
    });
  }),
);

it.effect("maps scan and malformed-reply failures semantically", () =>
  Effect.gen(function* () {
    const scanError = yield* inspect({
      match: "*",
      count: 1,
      assertDrained: false,
    }).pipe(
      Effect.provideService(
        RedisPool.RedisPool,
        service(() =>
          Effect.fail(new Redis.RedisError({ cause: "scan rejected" })),
        ),
      ),
      Effect.flip,
    );
    expect(scanError).toMatchObject({
      _tag: "InspectionError",
      operation: "scan",
    });

    const replyError = yield* inspect({
      match: "*",
      count: 1,
      assertDrained: false,
    }).pipe(
      Effect.provideService(
        RedisPool.RedisPool,
        service(<A>() => Effect.succeed({ cursor: "0" } as A)),
      ),
      Effect.flip,
    );
    expect(replyError).toMatchObject({
      _tag: "InspectionError",
      operation: "decode-reply",
    });
  }),
);

it.effect("inspection remains interruptible while Redis is blocked", () =>
  Effect.gen(function* () {
    const fiber = yield* inspect({
      match: "*",
      count: 1,
      assertDrained: false,
    }).pipe(
      Effect.provideService(
        RedisPool.RedisPool,
        service(() => Effect.never),
      ),
      Effect.forkChild,
    );
    yield* Fiber.interrupt(fiber);
    const exit = yield* Fiber.await(fiber);
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);
