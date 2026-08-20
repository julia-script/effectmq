import { expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Redis from "effect/unstable/persistence/Redis";
import { fromProbes } from "./RedisReadiness.js";

it.effect("readiness recovers only expected Redis failures", () =>
  Effect.gen(function* () {
    const ready = yield* fromProbes([Effect.succeed("PONG")]);
    expect(ready).toBe(true);

    const unavailable = yield* fromProbes([
      Effect.fail(new Redis.RedisError({ cause: new Error("offline") })),
    ]);
    expect(unavailable).toBe(false);

    const defect = yield* fromProbes([Effect.die("defect")]).pipe(Effect.exit);
    expect(Exit.isFailure(defect)).toBe(true);
    if (Exit.isFailure(defect)) expect(Cause.hasDies(defect.cause)).toBe(true);
  }),
);

it.effect("readiness interruption propagates", () =>
  Effect.gen(function* () {
    const fiber = yield* fromProbes([Effect.never]).pipe(Effect.forkChild);
    yield* Fiber.interrupt(fiber);
    const exit = yield* Fiber.await(fiber);
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
    }
  }),
);
