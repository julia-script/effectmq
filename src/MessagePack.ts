/** MessagePack schema boundary shared by Redis record codecs. @internal */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import { Packr } from "msgpackr";

const packr = new Packr({ useRecords: false, int64AsType: "number" });

/** MessagePack bytes to and from an unknown decoded value. */
export const UnknownFromMsgpack = Schema.Uint8Array.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.transformOrFail(
      (bytes: Uint8Array, options): Effect.Effect<unknown, SchemaIssue.Issue> =>
        Effect.try({
          try: () => packr.unpack(bytes),
          catch: (cause) =>
            new SchemaIssue.InvalidValue(
              { message: `MessagePack decoding failed: ${String(cause)}` },
              bytes,
              options,
            ),
        }),
    ),
    encode: SchemaGetter.transformOrFail(
      (value: unknown, options): Effect.Effect<Uint8Array, SchemaIssue.Issue> =>
        Effect.try({
          try: () => packr.pack(value),
          catch: (cause) =>
            new SchemaIssue.InvalidValue(
              { message: `MessagePack encoding failed: ${String(cause)}` },
              value,
              options,
            ),
        }),
    ),
  }),
);
