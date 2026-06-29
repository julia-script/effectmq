import { Duration, Effect, Effectable, Schedule } from "effect";
import * as Cron from "effect/Cron";
import * as TaskEngine from "./TaskEngine.js";

const TypeId = "~effectmq/Scheduler" as const;
// Activity
export interface Scheduler<E, R>
  extends Effect.Effect<
    void,
    E | TaskEngine.TaskEngineError,
    R | TaskEngine.TaskEngine
  > {
  readonly [TypeId]: typeof TypeId;
  readonly name: string;
  readonly cron: Cron.Cron;
}

export const make = <E, R>(config: {
  cron: Cron.Cron;
  name: string;
  handler: Effect.Effect<void, E, R>;
}): Scheduler<E, R> => {
  const execute = Effect.gen(function* () {
    const taskEngine = yield* TaskEngine.TaskEngine;
    let next = yield* taskEngine.setSchedule(
      config.name,
      Cron.next(config.cron),
    );
    yield* Effect.gen(function* () {
      const toConsume = next;
      const toSchedule = Cron.next(config.cron, toConsume);
      const result = yield* taskEngine.consumeSchedule(
        config.name,
        toConsume,
        toSchedule,
      );
      if (result.consumed) {
        yield* config.handler;
      }
      next = result.next ? new Date(result.next) : Cron.next(config.cron);
      const sleepFor = Math.max(1000, next.getTime() - Date.now());
      yield* Effect.sleep(Duration.millis(sleepFor));
    }).pipe(Effect.repeat(Schedule.forever));
  });
  const self: Scheduler<E, R> = {
    ...Effectable.Prototype({
      label: "effectmq/Scheduler",
      evaluate(_) {
        return execute;
      },
    }),
    [TypeId]: TypeId,
    name: config.name,
    cron: config.cron,
  };
  return self;
};
