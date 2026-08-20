import { expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Task from "./Task.js";

const config = {
  name: "identity-test",
  payload: { value: Schema.String },
  success: Schema.Void,
  error: Schema.String,
};

it.effect("invalid task configuration fails in the typed channel", () =>
  Effect.gen(function* () {
    const error = yield* Task.make({ ...config, maxRetries: -1 }).pipe(
      Effect.flip,
    );
    expect(error).toMatchObject({
      _tag: "TaskConfigurationError",
      field: "maxRetries",
      actual: -1,
    });
  }),
);

it.effect("default identities come from the provided Crypto service", () =>
  Effect.gen(function* () {
    const task = yield* Task.make(config);
    const deterministicCrypto = Crypto.make({
      randomBytes: (size) => new Uint8Array(size),
      digest: (_algorithm, bytes) => Effect.succeed(bytes),
    });
    const first = yield* task
      .idempotencyKey({ value: "first" })
      .pipe(Effect.provideService(Crypto.Crypto, deterministicCrypto));
    const second = yield* task
      .idempotencyKey({ value: "second" })
      .pipe(Effect.provideService(Crypto.Crypto, deterministicCrypto));
    expect(first).toBe(second);
    expect(first).toMatch(/^identity-test\/[0-9a-f-]{36}$/);
  }),
);

it.effect("custom identity callback exceptions remain typed", () =>
  Effect.gen(function* () {
    const task = yield* Task.make({
      ...config,
      idempotencyKey: () => {
        throw new Error("identity failed");
      },
    });
    const error = yield* task
      .idempotencyKey({ value: "value" })
      .pipe(Effect.flip);
    expect(error).toMatchObject({
      _tag: "TaskIdentityGenerationError",
      taskName: "identity-test",
    });
  }),
);
