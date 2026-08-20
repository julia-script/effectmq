import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  BooleanFromBytes,
  IntegerFromBytes,
  NumberFromBytes,
} from "./EngineRecord.js";
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

it.effect("Redis numeric fields reject non-finite and nonnumeric values", () =>
  Effect.gen(function* () {
    for (const value of [
      "NaN",
      "Infinity",
      "not-a-number",
      String(Number.MAX_SAFE_INTEGER + 1),
      "",
      "   ",
      "0x10",
      "1e2",
      "+1",
      "9007199254740992.5",
    ]) {
      const error = yield* Schema.decodeEffect(IntegerFromBytes)(value).pipe(
        Effect.flip,
      );
      expect(error._tag).toBe("SchemaError");
    }
  }),
);

it.effect("Redis finite numeric fields retain fractional values", () =>
  Effect.gen(function* () {
    expect(yield* Schema.decodeEffect(NumberFromBytes)("0.5")).toBe(0.5);
    expect(yield* Schema.decodeEffect(NumberFromBytes)("1e-7")).toBe(1e-7);
    expect(
      yield* Schema.decodeEffect(NumberFromBytes)(
        yield* Schema.encodeEffect(NumberFromBytes)(1e-7),
      ),
    ).toBe(1e-7);
    const error = yield* Schema.decodeEffect(NumberFromBytes)(
      "9007199254740992.5",
    ).pipe(Effect.flip);
    expect(error._tag).toBe("SchemaError");
  }),
);

it.effect("Redis boolean fields accept only zero and one", () =>
  Effect.gen(function* () {
    expect(yield* Schema.decodeEffect(BooleanFromBytes)("0")).toBe(false);
    expect(yield* Schema.decodeEffect(BooleanFromBytes)("1")).toBe(true);
    for (const value of ["", "2", "false", "bogus"]) {
      const error = yield* Schema.decodeEffect(BooleanFromBytes)(value).pipe(
        Effect.flip,
      );
      expect(error._tag).toBe("SchemaError");
    }
  }),
);
