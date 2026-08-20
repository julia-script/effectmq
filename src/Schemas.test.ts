import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { UnknownFromMsgpack } from "./MessagePack.js";

it.effect("MessagePack round-trips supported values", () =>
  Effect.gen(function* () {
    const value = { text: "effectmq", values: [1, true, null] };
    const bytes = yield* Schema.encodeEffect(UnknownFromMsgpack)(value);
    expect(yield* Schema.decodeEffect(UnknownFromMsgpack)(bytes)).toEqual(
      value,
    );
  }),
);

it.effect("truncated MessagePack fails as a SchemaError, not a defect", () =>
  Effect.gen(function* () {
    const error = yield* Schema.decodeEffect(UnknownFromMsgpack)(
      new Uint8Array([0xd9]),
    ).pipe(Effect.flip);
    expect(error._tag).toBe("SchemaError");
  }),
);

it.effect("non-byte MessagePack input fails as a SchemaError", () =>
  Effect.gen(function* () {
    const error = yield* Schema.decodeUnknownEffect(UnknownFromMsgpack)({
      not: "bytes",
    }).pipe(Effect.flip);
    expect(error._tag).toBe("SchemaError");
  }),
);
