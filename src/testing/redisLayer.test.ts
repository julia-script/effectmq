import { EventEmitter } from "node:events";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import { TestClock } from "effect/testing";
import {
  acquireTracked,
  makeTestResourceRegistry,
  TestInfrastructureError,
} from "./redisLayer.js";

const testFailure = (operation: "redis-ready" = "redis-ready") =>
  new TestInfrastructureError({ cause: new Error(operation), operation });

it.effect("tracked resources release after success and failure", () =>
  Effect.gen(function* () {
    const registry = makeTestResourceRegistry();
    let releases = 0;
    const resource = acquireTracked(
      registry,
      "resource",
      Effect.succeed("value"),
      () =>
        Effect.sync(() => {
          releases += 1;
        }),
    );

    yield* Effect.scoped(resource);
    expect(registry.active.size).toBe(0);
    expect(releases).toBe(1);

    const failed = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* resource;
        return yield* Effect.fail(testFailure());
      }),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(failed)).toBe(true);
    expect(registry.active.size).toBe(0);
    expect(releases).toBe(2);
  }),
);

it.effect("partial acquisition releases resources already acquired", () =>
  Effect.gen(function* () {
    const registry = makeTestResourceRegistry();
    let released = false;
    const exit = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireTracked(registry, "first", Effect.succeed("first"), () =>
          Effect.sync(() => {
            released = true;
          }),
        );
        yield* acquireTracked(
          registry,
          "second",
          Effect.fail(testFailure()),
          () => Effect.void,
        );
      }),
    ).pipe(Effect.exit);

    expect(Exit.isFailure(exit)).toBe(true);
    expect(released).toBe(true);
    expect(registry.active.size).toBe(0);
  }),
);

it.effect("scope cleanup runs after a defect, timeout, and interruption", () =>
  Effect.gen(function* () {
    const registry = makeTestResourceRegistry();

    const defectExit = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireTracked(
          registry,
          "defect",
          Effect.succeed(undefined),
          () => Effect.void,
        );
        return yield* Effect.die("assertion failed");
      }),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(defectExit)).toBe(true);
    expect(registry.active.size).toBe(0);

    const timeoutStarted = yield* Deferred.make<void>();
    const timeoutReleased = yield* Deferred.make<void>();
    const timeoutFiber = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireTracked(
          registry,
          "timeout",
          Effect.succeed(undefined),
          () => Deferred.succeed(timeoutReleased, undefined),
        );
        yield* Deferred.succeed(timeoutStarted, undefined);
        return yield* Effect.never;
      }),
    ).pipe(Effect.timeout("1 second"), Effect.forkChild);
    yield* Deferred.await(timeoutStarted);
    yield* TestClock.adjust("1 second");
    yield* Fiber.await(timeoutFiber);
    yield* Deferred.await(timeoutReleased);
    expect(registry.active.size).toBe(0);

    const interruptStarted = yield* Deferred.make<void>();
    const interruptReleased = yield* Deferred.make<void>();
    const interruptFiber = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireTracked(
          registry,
          "interrupt",
          Effect.succeed(undefined),
          () => Deferred.succeed(interruptReleased, undefined),
        );
        yield* Deferred.succeed(interruptStarted, undefined);
        return yield* Effect.never;
      }),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(interruptStarted);
    yield* Fiber.interrupt(interruptFiber);
    yield* Deferred.await(interruptReleased);
    expect(registry.active.size).toBe(0);
  }),
);

it.effect("scope cleanup removes listeners and interrupts child fibers", () =>
  Effect.gen(function* () {
    const registry = makeTestResourceRegistry();
    const emitter = new EventEmitter();
    const listener = () => undefined;

    const child = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireTracked(
          registry,
          "listener",
          Effect.sync(() => {
            emitter.on("event", listener);
          }),
          () =>
            Effect.sync(() => {
              emitter.off("event", listener);
            }),
        );
        return yield* Effect.never.pipe(Effect.forkScoped);
      }),
    );

    const childExit = yield* Fiber.await(child);
    expect(Exit.isFailure(childExit)).toBe(true);
    expect(emitter.listenerCount("event")).toBe(0);
    expect(registry.active.size).toBe(0);
  }),
);
