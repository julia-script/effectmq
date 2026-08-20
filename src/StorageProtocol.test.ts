import { readFileSync } from "node:fs";
import { Effect } from "effect";
import { Packr } from "msgpackr";
import { describe, expect, test } from "vitest";
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
  test("the committed golden envelope is byte-stable and lossless", async () => {
    const encoded = await Effect.runPromise(
      StorageProtocol.encodeValue(fixture.schemaId, fixture.kind, value),
    );
    expect(encoded).toBe(fixture.encoded);
    await expect(
      Effect.runPromise(
        StorageProtocol.decodeValue(
          fixture.encoded,
          fixture.schemaId,
          fixture.kind,
        ),
      ),
    ).resolves.toEqual(value);
  });

  test("unsupported JavaScript values fail instead of coercing", async () => {
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    for (const unsupported of [
      undefined,
      BigInt(1),
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
      new Date(),
      cyclic,
    ]) {
      const error = await Effect.runPromise(
        StorageProtocol.encodeValue("schema", "payload", unsupported).pipe(
          Effect.flip,
        ),
      );
      expect(error._tag).toBe("UnsupportedStorageValue");
    }
  });

  test("size limits are checked on the encoded representation", async () => {
    const error = await Effect.runPromise(
      StorageProtocol.encodeValue("schema", "success", "too large", {
        maxValueBytes: 4,
      }).pipe(Effect.flip),
    );
    expect(error).toMatchObject({
      _tag: "StorageLimitExceeded",
      kind: "success",
      maxBytes: 4,
    });
  });

  test("schema, version, kind, and malformed envelopes have distinct errors", async () => {
    const schemaError = await Effect.runPromise(
      StorageProtocol.decodeValue(
        fixture.encoded,
        "another/schema",
        "payload",
      ).pipe(Effect.flip),
    );
    expect(schemaError._tag).toBe("SchemaIdentityMismatch");

    const packr = new Packr({ useRecords: false });
    const v2 = `effectmq:v1:${Buffer.from(
      packr.pack([2, fixture.schemaId, "payload", null]),
    ).toString("base64")}`;
    const versionError = await Effect.runPromise(
      StorageProtocol.decodeValue(v2, fixture.schemaId, "payload").pipe(
        Effect.flip,
      ),
    );
    expect(versionError).toMatchObject({
      _tag: "UnsupportedProtocolVersion",
      encountered: 2,
      supported: [1],
    });

    const kindError = await Effect.runPromise(
      StorageProtocol.decodeValue(
        fixture.encoded,
        fixture.schemaId,
        "failure",
      ).pipe(Effect.flip),
    );
    expect(kindError._tag).toBe("CorruptStorageValue");

    const corrupt = await Effect.runPromise(
      StorageProtocol.decodeValue(
        "effectmq:v1:not-messagepack",
        fixture.schemaId,
        "payload",
      ).pipe(Effect.flip),
    );
    expect(corrupt._tag).toBe("CorruptStorageValue");
  });
});
