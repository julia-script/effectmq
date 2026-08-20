/** Versioned storage envelopes for opaque user values. @module */
import { Data, Effect } from "effect";
import { Packr } from "msgpackr";

export const protocolVersion = 1 as const;
export const readableProtocolVersions = [1] as const;
export const writableProtocolVersion = 1 as const;

export const builtInErrorTags = {
  stalled: "~effectmq/Error/Stalled",
  canceled: "~effectmq/Error/Canceled",
} as const;

export type ValueKind = "payload" | "success" | "failure";

export interface StorageLimits {
  readonly maxValueBytes: number;
  readonly maxErrorEntries: number;
  readonly maxRelationships: number;
  readonly maxEventEntries: number;
}

export const defaultStorageLimits: StorageLimits = {
  maxValueBytes: 1024 * 1024,
  maxErrorEntries: 100,
  maxRelationships: 1000,
  maxEventEntries: 10_000,
};

export class UnsupportedStorageValue extends Data.TaggedError(
  "UnsupportedStorageValue",
)<{ readonly path: string; readonly valueType: string }> {}

export class StorageLimitExceeded extends Data.TaggedError(
  "StorageLimitExceeded",
)<{
  readonly kind: ValueKind;
  readonly actualBytes: number;
  readonly maxBytes: number;
}> {}

export class StorageCountLimitExceeded extends Data.TaggedError(
  "StorageCountLimitExceeded",
)<{
  readonly resource: "relationships";
  readonly scope: "holder" | "retained";
  readonly actualCount: number;
  readonly maxCount: number;
}> {}

export class CorruptStorageValue extends Data.TaggedError(
  "CorruptStorageValue",
)<{ readonly message: string; readonly cause?: unknown }> {}

export class UnsupportedProtocolVersion extends Data.TaggedError(
  "UnsupportedProtocolVersion",
)<{ readonly encountered: number; readonly supported: readonly number[] }> {}

export class SchemaIdentityMismatch extends Data.TaggedError(
  "SchemaIdentityMismatch",
)<{ readonly expected: string; readonly encountered: string }> {}

export type StorageProtocolError =
  | UnsupportedStorageValue
  | StorageLimitExceeded
  | StorageCountLimitExceeded
  | CorruptStorageValue
  | UnsupportedProtocolVersion
  | SchemaIdentityMismatch;

const packr = new Packr({ useRecords: false, int64AsType: "number" });
const prefix = "effectmq:v1:";

const validate = (
  value: unknown,
  path: string,
  seen: Set<object>,
): UnsupportedStorageValue | undefined => {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    value instanceof Uint8Array
  ) {
    return undefined;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER
      ? undefined
      : new UnsupportedStorageValue({ path, valueType: "unsafe number" });
  }
  if (typeof value !== "object") {
    return new UnsupportedStorageValue({ path, valueType: typeof value });
  }
  if (seen.has(value)) {
    return new UnsupportedStorageValue({ path, valueType: "cyclic object" });
  }
  seen.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      const error = validate(value[index], `${path}[${index}]`, seen);
      if (error) return error;
    }
    seen.delete(value);
    return undefined;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype) {
    return new UnsupportedStorageValue({
      path,
      valueType: value.constructor?.name ?? "non-plain object",
    });
  }
  for (const [key, nested] of Object.entries(value)) {
    const error = validate(nested, `${path}.${key}`, seen);
    if (error) return error;
  }
  seen.delete(value);
  return undefined;
};

const normalizeDecoded = (value: unknown): unknown => {
  if (Buffer.isBuffer(value)) return new Uint8Array(value);
  if (Array.isArray(value)) return value.map(normalizeDecoded);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [
        key,
        normalizeDecoded(nested),
      ]),
    );
  }
  return value;
};

/** Encode the documented value domain into an ASCII-safe MessagePack envelope. */
export const encodeValue = (
  schemaId: string,
  kind: ValueKind,
  value: unknown,
  limits: Partial<StorageLimits> = defaultStorageLimits,
): Effect.Effect<string, UnsupportedStorageValue | StorageLimitExceeded> =>
  Effect.gen(function* () {
    const unsupported = validate(value, "$", new Set());
    if (unsupported) return yield* unsupported;
    const bytes = packr.pack([protocolVersion, schemaId, kind, value]);
    const maxValueBytes =
      limits.maxValueBytes ?? defaultStorageLimits.maxValueBytes;
    if (bytes.byteLength > maxValueBytes) {
      return yield* new StorageLimitExceeded({
        kind,
        actualBytes: bytes.byteLength,
        maxBytes: maxValueBytes,
      });
    }
    return `${prefix}${Buffer.from(bytes).toString("base64")}`;
  });

/** Decode and validate an envelope before schema decoding the enclosed value. */
export const decodeValue = (
  encoded: unknown,
  expectedSchemaId: string,
  expectedKind: ValueKind,
): Effect.Effect<
  unknown,
  CorruptStorageValue | UnsupportedProtocolVersion | SchemaIdentityMismatch
> =>
  Effect.gen(function* () {
    if (typeof encoded !== "string" || !encoded.startsWith(prefix)) {
      return yield* new CorruptStorageValue({
        message: "Stored value is not an effectmq v1 envelope",
      });
    }
    const envelope = yield* Effect.try({
      try: () =>
        packr.unpack(Buffer.from(encoded.slice(prefix.length), "base64")),
      catch: (cause) =>
        new CorruptStorageValue({
          message: "Invalid MessagePack envelope",
          cause,
        }),
    });
    if (!Array.isArray(envelope) || envelope.length !== 4) {
      return yield* new CorruptStorageValue({
        message: "Storage envelope must be a four-item tuple",
      });
    }
    const [version, schemaId, kind, value] = envelope;
    if (typeof version !== "number") {
      return yield* new CorruptStorageValue({
        message: "Storage envelope version is not numeric",
      });
    }
    if (!readableProtocolVersions.includes(version as 1)) {
      return yield* new UnsupportedProtocolVersion({
        encountered: version,
        supported: readableProtocolVersions,
      });
    }
    if (schemaId !== expectedSchemaId) {
      return yield* new SchemaIdentityMismatch({
        expected: expectedSchemaId,
        encountered: String(schemaId),
      });
    }
    if (kind !== expectedKind) {
      return yield* new CorruptStorageValue({
        message: `Expected ${expectedKind} envelope, received ${String(kind)}`,
      });
    }
    return normalizeDecoded(value);
  });
