import { readFileSync } from "node:fs";
import { layer } from "@effect/vitest";
import { Context, Cron, Effect, Schema, SchemaGetter } from "effect";
import type * as Crypto from "effect/Crypto";
import { expect, expectTypeOf, it } from "vitest";
import {
  Scheduler,
  StorageProtocol,
  Task,
  type TaskEngine,
  TaskHistory,
  TaskQueue,
  Worker,
} from "./index.js";
import { TestLayer } from "./testing/redisLayer.js";

class Encoder extends Context.Service<Encoder, { readonly prefix: string }>()(
  "history/Encoder",
) {}
class Decoder extends Context.Service<Decoder, { readonly prefix: string }>()(
  "history/Decoder",
) {}
const serviceSchema = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) =>
      Decoder.pipe(Effect.map(({ prefix }) => prefix + value)),
    ),
    encode: SchemaGetter.transformEffect((value) =>
      Encoder.pipe(Effect.map(({ prefix }) => prefix + value)),
    ),
  }),
);
const definition = Task.make({
  name: "history-services",
  payload: {},
  success: Schema.String,
  error: Schema.Never,
  progress: serviceSchema,
  idempotencyKey: () => "task",
});
const q = TaskQueue.make(definition.name, definition);
const typeContracts = (handle: TaskQueue.TaskHandle<string, never>) => {
  Scheduler.make({
    name: "progress-schedule",
    cron: Cron.parseUnsafe("0 * * * *"),
    queue: q,
    payload: () => ({}),
    missed: { _tag: "skip" },
  });
  const completed = TaskQueue.completeOne(q, (_, ctx) => {
    const emit = ctx.progress("hello");
    expectTypeOf<Effect.Services<typeof emit>>().toEqualTypeOf<Encoder>();
    expectTypeOf<
      Effect.Error<typeof emit>
    >().toEqualTypeOf<TaskHistory.ProgressWriteError>();
    expectTypeOf<Effect.Success<typeof emit>>().toEqualTypeOf<string>();
    return emit.pipe(Effect.as("ok"));
  });
  const read = TaskQueue.readEvents(q, handle);
  expectTypeOf<Effect.Services<typeof read>>().toEqualTypeOf<
    TaskEngine.TaskEngine | Decoder
  >();
  expectTypeOf<Effect.Services<typeof completed>>().toEqualTypeOf<
    TaskEngine.TaskEngine | Encoder | Crypto.Crypto
  >();
  expectTypeOf<
    Effect.Error<typeof completed>
  >().toEqualTypeOf<TaskQueue.CompleteOneError>();
  const worker = Worker.make(q, (_, ctx) =>
    ctx.progress("hi").pipe(Effect.as("ok")),
  );
  expectTypeOf(worker.handler).toBeFunction();
  const legacy = Task.make<Record<string, never>, Schema.String, Schema.Never>({
    name: "legacy",
    payload: {},
    success: Schema.String,
    error: Schema.Never,
    idempotencyKey: () => "id",
  });
  const omitted = TaskQueue.make("legacy", legacy);
  TaskQueue.complete(omitted, () => Effect.succeed("ok"));
  TaskQueue.complete(omitted, (_, ctx) => {
    // @ts-expect-error No progress schema means no legal progress value.
    ctx.progress("no");
    return Effect.succeed("ok");
  });
  const lifecycle = TaskQueue.make(
    "lifecycle",
    Task.make({
      name: "lifecycle",
      payload: {},
      success: Schema.Void,
      error: Schema.Never,
      progress: Schema.Never,
    }),
  );
  TaskQueue.completeOne(lifecycle, (_, ctx) => {
    // @ts-expect-error A lifecycle-only task cannot emit custom progress.
    ctx.progress({});
    return Effect.void;
  });
  const union = TaskQueue.make(
    "union",
    Task.make({
      name: "union",
      payload: {},
      success: Schema.Void,
      error: Schema.Never,
      progress: Schema.Union([
        Schema.Struct({ _tag: Schema.Literal("Text"), text: Schema.String }),
        Schema.Struct({
          _tag: Schema.Literal("Percent"),
          value: Schema.Number,
        }),
      ]),
    }),
  );
  TaskQueue.completeOne(union, (_, ctx) => {
    ctx.progress({ _tag: "Text", text: "ok" });
    // @ts-expect-error Progress must match the declared discriminated union.
    ctx.progress({ _tag: "Percent", value: "bad" });
    return Effect.void;
  });
};
it("preserves legacy generic positions and exact progress channels", () =>
  expect(typeof typeContracts).toBe("function"));
layer(TestLayer, { excludeTestServices: true })(
  "progress schema services",
  (it) => {
    it.effect(
      "uses only encoding services when writing and decoding services when reading",
      () =>
        Effect.gen(function* () {
          const handle = (yield* TaskQueue.offer(
            q,
            {},
            { onSuccessPolicy: "keep" },
          )).handle;
          yield* TaskQueue.completeOne(q, (_, ctx) =>
            ctx.progress("value").pipe(Effect.as("ok")),
          ).pipe(Effect.provideService(Encoder, { prefix: "encoded/" }));
          const page = yield* TaskQueue.readEvents(q, handle).pipe(
            Effect.provideService(Decoder, { prefix: "decoded/" }),
          );
          expect(
            page.entries.find((e) => e.event._tag === "Progress")?.event,
          ).toEqual({ _tag: "Progress", data: "decoded/encoded/value" });
          const disabled = TaskQueue.make(
            "history-no-schema",
            Task.make({
              name: "none",
              payload: {},
              success: Schema.Void,
              error: Schema.Never,
            }),
          );
          const disabledHandle = (yield* TaskQueue.offer(disabled, {})).handle;
          expect(
            (yield* TaskQueue.readEvents(disabled, disabledHandle).pipe(
              Effect.flip,
            ))._tag,
          ).toBe("HistoryDisabled");
          const lifecycle = TaskQueue.make(
            "history-lifecycle-only",
            Task.make({
              name: "lifecycle",
              payload: {},
              success: Schema.Void,
              error: Schema.Never,
              progress: Schema.Never,
            }),
          );
          const lifeHandle = (yield* TaskQueue.offer(
            lifecycle,
            {},
            { onSuccessPolicy: "keep" },
          )).handle;
          yield* TaskQueue.completeOne(lifecycle, () => Effect.void);
          expect(
            (yield* TaskQueue.readEvents(lifecycle, lifeHandle)).entries.every(
              (e) => e.event._tag === "Lifecycle",
            ),
          ).toBe(true);
        }),
    );
  },
);

it("decodes the committed v1 progress history fixture and preserves its envelope bytes", async () => {
  const fixture = JSON.parse(
    readFileSync(
      new URL("./testing/fixtures/task-history-v1.json", import.meta.url),
      "utf8",
    ),
  ) as {
    identity: TaskHistory.Identity;
    schemaId: string;
    id: string;
    fields: Record<string, string>;
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const value = { _tag: "Message", text: "working" };
      expect(
        yield* StorageProtocol.encodeValue(fixture.schemaId, "progress", value),
      ).toBe(fixture.fields.data);
      const decoded = yield* TaskHistory.decodeRecord(
        fixture.identity,
        fixture.schemaId,
        fixture.id,
        fixture.fields,
      );
      expect(decoded.sequence).toBe(1);
      expect(decoded.entry.attempt).toBe(1);
      expect(decoded.entry.timestamp).toEqual(new Date(1_000_000));
      expect(
        yield* StorageProtocol.decodeValue(
          fixture.fields.data,
          fixture.schemaId,
          "progress",
        ),
      ).toEqual(value);
      expect(
        (yield* TaskHistory.decodeRecord(
          fixture.identity,
          fixture.schemaId,
          fixture.id,
          { ...fixture.fields, protocolVersion: "2" },
        ).pipe(Effect.flip))._tag,
      ).toBe("UnsupportedProtocolVersion");
    }),
  );
});
