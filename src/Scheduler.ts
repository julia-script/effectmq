/**
 * Cron-driven scheduler: runs a handler on a cron schedule, using the
 * {@link TaskEngine} to coordinate the next-run time across workers so the
 * handler fires once per scheduled tick rather than once per worker.
 *
 * @module
 */
import { Duration, Effect, Effectable, Schedule } from "effect";
import * as Cron from "effect/Cron";
import * as TaskEngine from "./TaskEngine.js";

const TypeId = "~effectmq/Scheduler" as const;

/**
 * A runnable scheduler. It is an `Effect` that, when run, loops forever:
 * consuming each due cron tick and invoking the handler exactly once per tick.
 */
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

/**
 * Create a {@link Scheduler} that runs `handler` on the given `cron` schedule.
 * The `name` keys the shared schedule state in the engine, so multiple workers
 * running the same named scheduler will collectively fire the handler once per
 * cron tick.
 */
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
