import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as RedisPool from "./RedisPool.js";
import * as TaskEngine from "./TaskEngine.js";

const service = (options: {
  readonly scriptReply?: unknown;
  readonly binaryReply?: unknown;
}): RedisPool.RedisPoolService => ({
  send: <A>() => Effect.succeed(options.scriptReply as A),
  sendBinary: <A>() => Effect.succeed(options.binaryReply as A),
  evalScript: <A>() => Effect.succeed(options.scriptReply as A),
});

it.effect("malformed scalar and collection replies fail semantically", () =>
  Effect.gen(function* () {
    const scalarEngine = yield* TaskEngine.makeWithRedis(
      service({ scriptReply: { generation: 1 } }),
    );
    const scalarError = yield* scalarEngine
      .getGeneration("queue", "task")
      .pipe(Effect.flip);
    expect(scalarError.reason).toMatchObject({
      _tag: "InvalidReply",
      operation: "effectmq_getGeneration",
    });

    const collectionEngine = yield* TaskEngine.makeWithRedis(
      service({ scriptReply: { cursor: "not-a-tuple" } }),
    );
    const collectionError = yield* collectionEngine
      .listTasks("queue", "wait")
      .pipe(Effect.flip);
    expect(collectionError.reason).toMatchObject({
      _tag: "InvalidReply",
      operation: "effectmq_listTasks",
    });
  }),
);

const eventFields = [
  "taskId",
  "task",
  "generation",
  "1",
  "protocolVersion",
  "1",
  "schemaId",
  "schema",
  "_tag",
  "task.completed",
  "policy",
  "keep",
  "__proto__",
  "polluted",
  "constructor",
  "constructor-value",
  "prototype",
  "prototype-value",
] as const;

const streamReply = (representation: "array" | "map") => {
  const entries = [["1-0", eventFields]];
  return representation === "map"
    ? new Map([["events", entries]])
    : [["events", entries]];
};

for (const representation of ["array", "map"] as const) {
  it.effect(
    `validates ${representation} RESP stream replies without prototype mutation`,
    () =>
      Effect.gen(function* () {
        const engine = yield* TaskEngine.makeWithRedis(
          service({
            scriptReply: ["0-0", "0-0", "0-0"],
            binaryReply: streamReply(representation),
          }),
        );
        const event = yield* engine
          .stream("queue", { cursor: "0-0" })
          .pipe(Stream.runHead);
        expect(Option.isSome(event)).toBe(true);
        if (Option.isSome(event))
          expect(event.value._tag).toBe("task.completed");
        expect(
          (Object.prototype as Record<string, unknown>).polluted,
        ).toBeUndefined();
      }),
  );
}

it.effect("rejects odd stream field arrays", () =>
  Effect.gen(function* () {
    const engine = yield* TaskEngine.makeWithRedis(
      service({
        scriptReply: ["0-0", "0-0", "0-0"],
        binaryReply: [["events", [["1-0", ["taskId"]]]]],
      }),
    );
    const error = yield* engine
      .stream("queue", { cursor: "0-0" })
      .pipe(Stream.runHead, Effect.flip);
    expect(error._tag).toBe("TaskEngineError");
    if (error._tag !== "TaskEngineError") return;
    expect(error.reason).toMatchObject({
      _tag: "InvalidReply",
      operation: "xread.fields",
    });
  }),
);

it.effect("rejects malformed caller cursors as typed failures", () =>
  Effect.gen(function* () {
    const engine = yield* TaskEngine.makeWithRedis(
      service({ scriptReply: ["0-0", "0-0", "0-0"] }),
    );
    for (const cursor of [
      "not-a-stream-id",
      "18446744073709551616-0",
      "0-18446744073709551616",
      "1".repeat(1_000),
    ]) {
      const error = yield* engine
        .stream("queue", { cursor })
        .pipe(Stream.runHead, Effect.flip);
      expect(error).toMatchObject({ _tag: "InvalidCursor", cursor });
    }
  }),
);

it.effect("rejects malformed persisted latest cursors as invalid replies", () =>
  Effect.gen(function* () {
    const engine = yield* TaskEngine.makeWithRedis(
      service({ scriptReply: ["0-0", "0-0", "$"] }),
    );
    const error = yield* engine
      .stream("queue")
      .pipe(Stream.runHead, Effect.flip);
    expect(error).toMatchObject({
      _tag: "TaskEngineError",
      reason: {
        _tag: "InvalidReply",
        operation: "effectmq_eventCursors",
      },
    });
  }),
);
