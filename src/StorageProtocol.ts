/** Versioned storage envelopes for opaque user values. @module */
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import { Packr } from "msgpackr";

/**
 * The storage protocol version written by this release.
 *
 * @category Protocol
 * @since 0.3.0
 */
export const protocolVersion = 1 as const;
/**
 * Storage protocol versions this release can decode.
 *
 * @category Protocol
 * @since 0.3.0
 */
export const readableProtocolVersions = [1] as const;
/**
 * The only storage protocol version this release writes.
 *
 * @category Protocol
 * @since 0.3.0
 */
export const writableProtocolVersion = 1 as const;

/**
 * Stable tags reserved for failures created by the queue runtime.
 *
 * @category Protocol
 * @since 0.3.0
 */
export const builtInErrorTags = {
  stalled: "~effectmq/Error/Stalled",
  canceled: "~effectmq/Error/Canceled",
} as const;

/**
 * Identifies the semantic value carried by a storage envelope.
 *
 * @category Protocol
 * @since 0.3.0
 */
export type ValueKind = "payload" | "success" | "failure";

/**
 * Bounds persisted values and queue-owned collections.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface StorageLimits {
  readonly maxValueBytes: number;
  readonly maxErrorEntries: number;
  readonly maxRelationships: number;
  readonly maxEventEntries: number;
}

/**
 * Production defaults for storage bytes, error history, relationships, and events.
 *
 * Values are limited to 1 MiB, error history to 100 entries, retention
 * relationships to 1,000, and event history to 10,000 entries.
 *
 * @category Configuration
 * @since 0.3.0
 */
export const defaultStorageLimits: StorageLimits = {
  maxValueBytes: 1024 * 1024,
  maxErrorEntries: 100,
  maxRelationships: 1000,
  maxEventEntries: 10_000,
};

/**
 * Indicates that a value falls outside EffectMQ's lossless storage domain.
 *
 * @category Errors
 * @since 0.3.0
 */
export class UnsupportedStorageValue extends Data.TaggedError(
  "UnsupportedStorageValue",
)<{ readonly path: string; readonly valueType: string }> {}

/**
 * Indicates that an encoded payload, success, or failure exceeds its byte limit.
 *
 * @category Errors
 * @since 0.3.0
 */
export class StorageLimitExceeded extends Data.TaggedError(
  "StorageLimitExceeded",
)<{
  readonly kind: ValueKind;
  readonly actualBytes: number;
  readonly maxBytes: number;
}> {}

/**
 * Indicates that a bounded queue-owned collection exceeded its configured size.
 *
 * @category Errors
 * @since 0.3.0
 */
export class StorageCountLimitExceeded extends Data.TaggedError(
  "StorageCountLimitExceeded",
)<{
  readonly resource: "relationships";
  readonly scope: "holder" | "retained";
  readonly actualCount: number;
  readonly maxCount: number;
}> {}

/**
 * Indicates that stored data is malformed or has the wrong value kind.
 *
 * @category Errors
 * @since 0.3.0
 */
export class CorruptStorageValue extends Data.TaggedError(
  "CorruptStorageValue",
)<{ readonly message: string; readonly cause?: unknown }> {}

/** A supported value could not be serialized for durable storage. */
export class StorageEncodingError extends Data.TaggedError(
  "StorageEncodingError",
)<{
  readonly stage: "messagepack" | "base64";
  readonly path: string;
  readonly cause: unknown;
}> {}

/** External bytes could not be converted or deserialized safely. */
export class StorageDecodingError extends Data.TaggedError(
  "StorageDecodingError",
)<{
  readonly stage: "bytes" | "base64" | "messagepack";
  readonly path: string;
  readonly cause: unknown;
}> {}

/**
 * Indicates that an envelope uses a protocol version this release cannot read.
 *
 * @category Errors
 * @since 0.3.0
 */
export class UnsupportedProtocolVersion extends Data.TaggedError(
  "UnsupportedProtocolVersion",
)<{ readonly encountered: number; readonly supported: readonly number[] }> {}

/**
 * Indicates that an envelope was written for a different task schema identity.
 *
 * @category Errors
 * @since 0.3.0
 */
export class SchemaIdentityMismatch extends Data.TaggedError(
  "SchemaIdentityMismatch",
)<{ readonly expected: string; readonly encountered: string }> {}

/**
 * Every typed failure produced by EffectMQ's storage boundary.
 *
 * @category Errors
 * @since 0.3.0
 */
export type StorageProtocolError =
  | UnsupportedStorageValue
  | StorageLimitExceeded
  | StorageCountLimitExceeded
  | CorruptStorageValue
  | StorageEncodingError
  | StorageDecodingError
  | UnsupportedProtocolVersion
  | SchemaIdentityMismatch;

const packr = new Packr({
  useRecords: false,
  mapsAsObjects: false,
  int64AsType: "bigint",
});
const prefix = "effectmq:v1:";

const validate = (
  value: unknown,
  path: string,
  seen: Set<object>,
  allowRootUndefined = false,
): UnsupportedStorageValue | undefined => {
  if (value === undefined && path === "$" && allowRootUndefined) {
    return undefined;
  }
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    value instanceof Uint8Array
  ) {
    return undefined;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) &&
      Math.abs(value) <= Number.MAX_SAFE_INTEGER &&
      !Object.is(value, -0)
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
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (
        descriptor === undefined ||
        !descriptor.enumerable ||
        !("value" in descriptor)
      ) {
        return new UnsupportedStorageValue({
          path: `${path}[${index}]`,
          valueType: "sparse or non-data array entry",
        });
      }
      const error = validate(descriptor.value, `${path}[${index}]`, seen);
      if (error) return error;
    }
    for (const key of Reflect.ownKeys(value)) {
      if (key === "length") continue;
      if (
        typeof key !== "string" ||
        !/^(?:0|[1-9]\d*)$/.test(key) ||
        Number(key) >= value.length
      ) {
        return new UnsupportedStorageValue({
          path,
          valueType: "array with custom properties",
        });
      }
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
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string") {
      return new UnsupportedStorageValue({
        path,
        valueType: "symbol-keyed object",
      });
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (
      descriptor === undefined ||
      !descriptor.enumerable ||
      !("value" in descriptor)
    ) {
      return new UnsupportedStorageValue({
        path: `${path}.${key}`,
        valueType: "non-data property",
      });
    }
    const error = validate(descriptor.value, `${path}.${key}`, seen);
    if (error) return error;
  }
  seen.delete(value);
  return undefined;
};

const defineDataProperty = (
  target: Record<string, unknown>,
  key: string,
  value: unknown,
) => {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const normalizeDecoded = (value: unknown): unknown => {
  if (Buffer.isBuffer(value)) return new Uint8Array(value);
  if (typeof value === "bigint") {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) &&
      value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value;
  }
  if (Array.isArray(value)) return value.map(normalizeDecoded);
  if (value instanceof Map) {
    const normalized: Record<string, unknown> = {};
    for (const [key, nested] of value) {
      if (typeof key !== "string") {
        throw new TypeError("Storage object keys must be strings");
      }
      defineDataProperty(normalized, key, normalizeDecoded(nested));
    }
    return normalized;
  }
  if (
    typeof value === "object" &&
    value !== null &&
    Object.getPrototypeOf(value) === Object.prototype
  ) {
    const normalized: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) {
      defineDataProperty(normalized, key, normalizeDecoded(nested));
    }
    return normalized;
  }
  return value;
};

/**
 * Encodes a lossless JavaScript value into an ASCII-safe MessagePack envelope.
 *
 * Supported values are `null`, strings, booleans, finite safe numbers other
 * than negative zero,
 * `Uint8Array`, arrays, and plain objects composed recursively from those
 * values. A top-level `undefined` success is supported for `Schema.Void`;
 * nested `undefined` values and `undefined` payloads/failures remain invalid.
 * Cycles, class instances, unsafe numbers, `bigint`, functions, and symbols
 * fail with {@link UnsupportedStorageValue}.
 *
 * The size limit applies to the MessagePack bytes before base64 encoding.
 *
 * **Example: Round-trip an opaque payload**
 *
 * ```ts
 * import { Effect } from "effect"
 * import { StorageProtocol } from "@effectmq/core"
 *
 * const roundTrip = Effect.gen(function* () {
 *   const encoded = yield* StorageProtocol.encodeValue(
 *     "invoice/v1",
 *     "payload",
 *     { invoiceId: "inv-42", digest: new Uint8Array([1, 2, 3]) }
 *   )
 *   return yield* StorageProtocol.decodeValue(
 *     encoded,
 *     "invoice/v1",
 *     "payload"
 *   )
 * })
 * ```
 *
 * @category Encoding
 * @since 0.3.0
 */
export const encodeValue = (
  schemaId: string,
  kind: ValueKind,
  value: unknown,
  limits: Partial<StorageLimits> = defaultStorageLimits,
): Effect.Effect<
  string,
  UnsupportedStorageValue | StorageLimitExceeded | StorageEncodingError
> =>
  Effect.gen(function* () {
    const unsupported = yield* Effect.try({
      try: () => validate(value, "$", new Set(), kind === "success"),
      catch: (cause) =>
        new StorageEncodingError({
          stage: "messagepack",
          path: "$",
          cause,
        }),
    });
    if (unsupported) return yield* unsupported;
    const bytes = yield* Effect.try({
      try: () => packr.pack([protocolVersion, schemaId, kind, value]),
      catch: (cause) =>
        new StorageEncodingError({
          stage: "messagepack",
          path: "$",
          cause,
        }),
    });
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

/**
 * Decodes and validates an envelope before application schema decoding.
 *
 * The protocol version, schema identity, and value kind must all match the
 * caller's expectations. Binary values are normalized to `Uint8Array`.
 *
 * @category Encoding
 * @since 0.3.0
 */
export const decodeValue = (
  encoded: unknown,
  expectedSchemaId: string,
  expectedKind: ValueKind,
): Effect.Effect<
  unknown,
  | CorruptStorageValue
  | StorageDecodingError
  | UnsupportedProtocolVersion
  | SchemaIdentityMismatch
> =>
  Effect.gen(function* () {
    if (typeof encoded !== "string" || !encoded.startsWith(prefix)) {
      return yield* new CorruptStorageValue({
        message: "Stored value is not an effectmq v1 envelope",
      });
    }
    const base64 = encoded.slice(prefix.length);
    if (
      base64.length === 0 ||
      base64.length % 4 !== 0 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
        base64,
      )
    ) {
      return yield* new StorageDecodingError({
        stage: "base64",
        path: "$",
        cause: new TypeError("Storage envelope contains invalid base64"),
      });
    }
    const bytes = Buffer.from(base64, "base64");
    const envelope = yield* Effect.try({
      try: () => packr.unpack(bytes),
      catch: (cause) =>
        new StorageDecodingError({
          stage: "messagepack",
          path: "$",
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
    const normalized = yield* Effect.try({
      try: () => normalizeDecoded(value),
      catch: (cause) =>
        new CorruptStorageValue({
          message: "Stored value could not be normalized safely",
          cause,
        }),
    });
    const unsupported = validate(
      normalized,
      "$",
      new Set(),
      expectedKind === "success",
    );
    if (unsupported !== undefined) {
      return yield* new CorruptStorageValue({
        message: `Stored value is outside the supported storage domain at ${unsupported.path}`,
        cause: unsupported,
      });
    }
    return normalized;
  });
