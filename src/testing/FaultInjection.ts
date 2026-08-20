/** Deterministic, occurrence-counted failures for production-boundary tests. */
import { Context, Data, Effect, Layer, Ref } from "effect";

export type FaultPoint =
  | "offer"
  | "acquire"
  | "heartbeat"
  | "acknowledgement"
  | "event"
  | "cleanup"
  | "reconnect"
  | "restart"
  | "sentinel-failover";

export class InjectedFault extends Data.TaggedError("InjectedFault")<{
  readonly point: FaultPoint;
  readonly occurrence: number;
}> {}

export interface FaultInjector {
  readonly hit: (point: FaultPoint) => Effect.Effect<void, InjectedFault>;
  readonly before: <A, E, R>(
    point: FaultPoint,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | InjectedFault, R>;
  readonly after: <A, E, R>(
    point: FaultPoint,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | InjectedFault, R>;
}

export class FaultInjection extends Context.Service<
  FaultInjection,
  FaultInjector
>()("effectmq/testing/FaultInjection") {}

/**
 * Fail on the configured hit number for each point. A value of `1` fails the
 * first hit, `2` the second, and an omitted point never fails.
 */
export const make = (plan: Partial<Record<FaultPoint, number>> = {}) =>
  Effect.gen(function* () {
    const hits = yield* Ref.make<Partial<Record<FaultPoint, number>>>({});
    const hit: FaultInjector["hit"] = (point) =>
      Ref.modify(hits, (current) => {
        const occurrence = (current[point] ?? 0) + 1;
        const next = { ...current, [point]: occurrence };
        return [
          plan[point] === occurrence
            ? Effect.fail(new InjectedFault({ occurrence, point }))
            : Effect.void,
          next,
        ] as const;
      }).pipe(Effect.flatten);
    return {
      hit,
      before: (point, effect) => hit(point).pipe(Effect.andThen(effect)),
      after: (point, effect) => effect.pipe(Effect.tap(() => hit(point))),
    } satisfies FaultInjector;
  });

export const layer = (plan: Partial<Record<FaultPoint, number>> = {}) =>
  Layer.effect(FaultInjection, make(plan));
