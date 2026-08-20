import { readFileSync } from "node:fs";
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { Packr } from "msgpackr";
import * as StorageProtocol from "./StorageProtocol.js";

const fixture = JSON.parse(
  readFileSync(
    new URL("./testing/fixtures/storage-v1.json", import.meta.url),
    "utf8",
  ),
) as { schemaId: string; kind: StorageProtocol.ValueKind; encoded: string };

const value = {
  binary: new Uint8Array([0, 255, 1]),
  emptyArray: [],
  emptyObject: {},
  nested: { none: null },
  number: 1.5,
  text: "✓🎉",
};

describe("StorageProtocol v1", () => {
  it.effect("the committed golden envelope is byte-stable and lossless", () =>
    Effect.gen(function* () {
      const encoded = yield* StorageProtocol.encodeValue(
        fixture.schemaId,
        fixture.kind,
        value,
      );
      expect(encoded).toBe(fixture.encoded);
      expect(
        yield* StorageProtocol.decodeValue(
          fixture.encoded,
          fixture.schemaId,
          fixture.kind,
        ),
      ).toEqual(value);
    }),
  );

  it.effect("unsupported JavaScript values fail instead of coercing", () =>
    Effect.gen(function* () {
      const cyclic: { self?: unknown } = {};
      cyclic.self = cyclic;
      for (const unsupported of [
        undefined,
        BigInt(1),
        Number.POSITIVE_INFINITY,
        Number.MAX_SAFE_INTEGER + 1,
        -0,
        new Date(),
        cyclic,
        { visible: true, [Symbol("hidden")]: false },
        Object.defineProperty({}, "hidden", { value: true }),
        Object.defineProperty({}, "computed", { get: () => true }),
        Object.assign([1], { extra: true }),
        Object.assign([1], { [Symbol("hidden")]: true }),
      ]) {
        const error = yield* StorageProtocol.encodeValue(
          "schema",
          "payload",
          unsupported,
        ).pipe(Effect.flip);
        expect(error._tag).toBe("UnsupportedStorageValue");
      }
    }),
  );

  it.effect("supports top-level undefined only for Schema.Void successes", () =>
    Effect.gen(function* () {
      const encoded = yield* StorageProtocol.encodeValue(
        "schema",
        "success",
        undefined,
      );
      expect(
        yield* StorageProtocol.decodeValue(encoded, "schema", "success"),
      ).toBeUndefined();

      const error = yield* StorageProtocol.encodeValue(
        "schema",
        "payload",
        undefined,
      ).pipe(Effect.flip);
      expect(error._tag).toBe("UnsupportedStorageValue");
    }),
  );

  it.effect("preserves prototype-sensitive keys as ordinary data", () =>
    Effect.gen(function* () {
      const value: Record<string, unknown> = {
        constructor: "constructor-value",
        prototype: "prototype-value",
      };
      Object.defineProperty(value, "__proto__", {
        enumerable: true,
        value: "proto-value",
      });

      const encoded = yield* StorageProtocol.encodeValue(
        "schema",
        "payload",
        value,
      );
      const decoded = yield* StorageProtocol.decodeValue(
        encoded,
        "schema",
        "payload",
      );
      expect(decoded).toEqual(value);
      expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
      expect(Object.getOwnPropertyDescriptor(decoded, "__proto__")?.value).toBe(
        "proto-value",
      );
    }),
  );

  it.effect("rejects decoded values outside the lossless storage domain", () =>
    Effect.gen(function* () {
      const external = new Packr({ useRecords: false });
      for (const unsupported of [
        Number.NaN,
        Number.POSITIVE_INFINITY,
        BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        new Date("2026-01-01T00:00:00.000Z"),
      ]) {
        const encoded = `effectmq:v1:${Buffer.from(
          external.pack([1, "schema", "payload", unsupported]),
        ).toString("base64")}`;
        const error = yield* StorageProtocol.decodeValue(
          encoded,
          "schema",
          "payload",
        ).pipe(Effect.flip);
        expect(error._tag).toBe("CorruptStorageValue");
      }
    }),
  );

  it.effect("size limits are checked on the encoded representation", () =>
    Effect.gen(function* () {
      const error = yield* StorageProtocol.encodeValue(
        "schema",
        "success",
        "too large",
        {
          maxValueBytes: 4,
        },
      ).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "StorageLimitExceeded",
        kind: "success",
        maxBytes: 4,
      });
    }),
  );

  it.effect(
    "schema, version, kind, and malformed envelopes have distinct errors",
    () =>
      Effect.gen(function* () {
        const schemaError = yield* StorageProtocol.decodeValue(
          fixture.encoded,
          "another/schema",
          "payload",
        ).pipe(Effect.flip);
        expect(schemaError._tag).toBe("SchemaIdentityMismatch");

        const packr = new Packr({ useRecords: false });
        const v2 = `effectmq:v1:${Buffer.from(
          packr.pack([2, fixture.schemaId, "payload", null]),
        ).toString("base64")}`;
        const versionError = yield* StorageProtocol.decodeValue(
          v2,
          fixture.schemaId,
          "payload",
        ).pipe(Effect.flip);
        expect(versionError).toMatchObject({
          _tag: "UnsupportedProtocolVersion",
          encountered: 2,
          supported: [1],
        });

        const kindError = yield* StorageProtocol.decodeValue(
          fixture.encoded,
          fixture.schemaId,
          "failure",
        ).pipe(Effect.flip);
        expect(kindError._tag).toBe("CorruptStorageValue");

        const corrupt = yield* StorageProtocol.decodeValue(
          "effectmq:v1:not-messagepack",
          fixture.schemaId,
          "payload",
        ).pipe(Effect.flip);
        expect(corrupt).toMatchObject({
          _tag: "StorageDecodingError",
          stage: "base64",
          path: "$",
        });

        const truncated = yield* StorageProtocol.decodeValue(
          `effectmq:v1:${Buffer.from([0xd9]).toString("base64")}`,
          fixture.schemaId,
          "payload",
        ).pipe(Effect.flip);
        expect(truncated).toMatchObject({
          _tag: "StorageDecodingError",
          stage: "messagepack",
          path: "$",
        });
      }),
  );

  it.effect("serializer-adjacent exceptions remain typed", () =>
    Effect.gen(function* () {
      const hostile = new Proxy(
        {},
        {
          ownKeys() {
            throw new Error("hostile ownKeys");
          },
        },
      );
      const error = yield* StorageProtocol.encodeValue(
        "schema",
        "payload",
        hostile,
      ).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "StorageEncodingError",
        stage: "messagepack",
        path: "$",
      });
    }),
  );
});
