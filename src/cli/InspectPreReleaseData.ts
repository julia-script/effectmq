/** Effect program for the read-only pre-v1 storage release gate. @module */
import * as Config from "effect/Config";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as NodeRedisPool from "../NodeRedisPool.js";
import * as RedisPool from "../RedisPool.js";

export interface InspectionConfig {
  readonly redisUrl: string;
  readonly match: string;
  readonly count: number;
  readonly assertDrained: boolean;
}

export const configuration: Config.Config<InspectionConfig> = Config.all({
  redisUrl: Config.string("EFFECTMQ_REDIS_URL"),
  match: Config.string("EFFECTMQ_SCAN_MATCH").pipe(
    Config.withDefault("~effectmq:*"),
  ),
  count: Config.number("EFFECTMQ_SCAN_COUNT").pipe(Config.withDefault(500)),
  assertDrained: Config.boolean("EFFECTMQ_ASSERT_DRAINED").pipe(
    Config.withDefault(false),
  ),
});

export class InspectionError extends Data.TaggedError("InspectionError")<{
  readonly operation: "scan" | "decode-reply";
  readonly cause: unknown;
}> {}

export class LegacyDataFound extends Data.TaggedError("LegacyDataFound")<{
  readonly count: number;
}> {}

const decodeScanReply = (
  value: unknown,
): Effect.Effect<readonly [string, readonly string[]], InspectionError> => {
  if (!Array.isArray(value) || value.length !== 2) {
    return Effect.fail(
      new InspectionError({
        operation: "decode-reply",
        cause: { expected: "[cursor, keys]", received: value },
      }),
    );
  }
  const [cursor, keys] = value;
  if (
    typeof cursor !== "string" ||
    !Array.isArray(keys) ||
    !keys.every((key) => typeof key === "string")
  ) {
    return Effect.fail(
      new InspectionError({
        operation: "decode-reply",
        cause: { expected: "[string, string[]]", received: value },
      }),
    );
  }
  return Effect.succeed([cursor, keys]);
};

export const inspect = Effect.fnUntraced(function* (
  config: Omit<InspectionConfig, "redisUrl">,
) {
  const redis = yield* RedisPool.RedisPool;
  const legacyKeys: string[] = [];
  let cursor = "0";
  do {
    const reply = yield* redis
      .send(
        "SCAN",
        cursor,
        "MATCH",
        config.match,
        "COUNT",
        String(config.count),
      )
      .pipe(
        Effect.mapError(
          (cause) => new InspectionError({ operation: "scan", cause }),
        ),
        Effect.flatMap(decodeScanReply),
      );
    cursor = reply[0];
    for (const key of reply[1]) {
      if (!key.startsWith("~effectmq:v1:")) legacyKeys.push(key);
    }
  } while (cursor !== "0");

  legacyKeys.sort();
  const result = {
    status: legacyKeys.length === 0 ? "drained" : "pre-v1-data-found",
    legacyKeyCount: legacyKeys.length,
    keys: legacyKeys,
  } as const;
  yield* Effect.sync(() => console.log(JSON.stringify(result, null, 2)));
  if (config.assertDrained && legacyKeys.length > 0) {
    return yield* new LegacyDataFound({ count: legacyKeys.length });
  }
  return result;
});

export const main = Effect.gen(function* () {
  const config = yield* configuration;
  return yield* inspect(config).pipe(
    Effect.provide(NodeRedisPool.layer({ url: config.redisUrl })),
  );
});
