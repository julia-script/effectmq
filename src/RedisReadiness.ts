/** Readiness policy shared by Redis adapters. @internal */
import * as Effect from "effect/Effect";
import type * as Redis from "effect/unstable/persistence/Redis";

export const fromProbes = (
  probes: ReadonlyArray<Effect.Effect<unknown, Redis.RedisError>>,
): Effect.Effect<boolean> =>
  Effect.all(probes).pipe(
    Effect.as(true),
    Effect.catchTag("RedisError", () => Effect.succeed(false)),
  );
