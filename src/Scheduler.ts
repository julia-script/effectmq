/** Durable cron tick materialization through ordinary EffectMQ tasks. @module */
import { Duration, Effect, Effectable, Schedule, type Schema } from "effect";
import * as Cron from "effect/Cron";
import type * as StorageProtocol from "./StorageProtocol.js";
import * as TaskEngine from "./TaskEngine.js";
import * as TaskQueue from "./TaskQueue.js";

const TypeId = "~effectmq/Scheduler" as const;

/** The nominal interval represented by one durable scheduled task. */
export interface Tick {
  readonly scheduleName: string;
  readonly scheduledAt: Date;
  readonly missedFrom: Date;
  readonly missedTo: Date;
}

export type MissedTickPolicy =
  | { readonly _tag: "skip" }
  | { readonly _tag: "coalesce" }
  | { readonly _tag: "backfill"; readonly maxBackfill: number };

export interface SchedulerConfig<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
> {
  readonly name: string;
  /** Cron rule including its optional IANA time zone. */
  readonly cron: Cron.Cron;
  readonly queue: TaskQueue.TaskQueue<Payload, Success, Error, QueueR>;
  readonly payload: (tick: Tick) => Payload["Type"];
  readonly missed: MissedTickPolicy;
  /** Initial cursor for a brand-new schedule; defaults to the next future tick. */
  readonly startAt?: Date;
  readonly taskOptions?: Omit<
    TaskQueue.TaskOptions,
    "taskId" | "onDuplicate" | "retainResultUntil"
  >;
}

type SchedulerFailure =
  | StorageProtocol.StorageProtocolError
  | TaskEngine.TaskEngineError
  | TaskQueue.IndeterminateWriteError
  | TaskQueue.RetentionContextRequired
  | Schema.SchemaError;

/**
 * A scheduler only materializes deterministic queue tasks. Execution belongs
 * to a normal managed worker and therefore has at-least-once semantics.
 */
export interface Scheduler<Payload extends Schema.Top>
  extends Effect.Effect<
    void,
    SchedulerFailure,
    TaskEngine.TaskEngine | Payload["DecodingServices"]
  > {
  readonly [TypeId]: typeof TypeId;
  readonly name: string;
  readonly cron: Cron.Cron;
  readonly timeZone: Cron.Cron["tz"];
}

const tickId = (name: string, scheduledAt: Date) =>
  `${name}/${scheduledAt.toISOString()}`;

const recentBackfill = (
  cron: Cron.Cron,
  firstMissed: Date,
  now: Date,
  maximum: number,
) => {
  const ticks: Date[] = [];
  let cursor = Cron.next(cron, now);
  for (let index = 0; index < maximum; index++) {
    const tick = Cron.prev(cron, cursor);
    if (tick.getTime() < firstMissed.getTime()) break;
    ticks.push(tick);
    cursor = tick;
  }
  return ticks.reverse();
};

/** Materialize all work selected by one bounded scheduler observation. */
export const materializeDue = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR,
>(config: SchedulerConfig<Payload, Success, Error, QueueR>, now = new Date()) {
  if (
    config.missed._tag === "backfill" &&
    (!Number.isSafeInteger(config.missed.maxBackfill) ||
      config.missed.maxBackfill < 1)
  ) {
    return yield* Effect.die(
      new RangeError("maxBackfill must be a positive safe integer"),
    );
  }

  const engine = yield* TaskEngine.TaskEngine;
  const initial = config.startAt ?? Cron.next(config.cron, now);
  const firstDue = yield* engine.setSchedule(config.name, initial);
  if (firstDue.getTime() > now.getTime()) return firstDue;

  const following = Cron.next(config.cron, firstDue);
  const hasMissedBacklog = following.getTime() <= now.getTime();
  const nextFuture = Cron.next(config.cron, now);
  let ticks: Date[];

  if (!hasMissedBacklog) {
    ticks = [firstDue];
  } else if (config.missed._tag === "skip") {
    ticks = [];
  } else if (config.missed._tag === "coalesce") {
    ticks = [Cron.prev(config.cron, nextFuture)];
  } else {
    ticks = recentBackfill(
      config.cron,
      firstDue,
      now,
      config.missed.maxBackfill,
    );
  }

  // Offer comes before cursor advancement. A crash in between only causes an
  // idempotent re-offer of the same schedule/tick identity on restart.
  for (const scheduledAt of ticks) {
    const tick: Tick = {
      scheduleName: config.name,
      scheduledAt,
      missedFrom: config.missed._tag === "coalesce" ? firstDue : scheduledAt,
      missedTo: scheduledAt,
    };
    yield* TaskQueue.offer(config.queue, config.payload(tick), {
      ...config.taskOptions,
      taskId: tickId(config.name, scheduledAt),
      onDuplicate: "return-existing",
    });
  }

  const consumed = yield* engine.consumeSchedule(
    config.name,
    firstDue,
    nextFuture,
  );
  return consumed.next ?? nextFuture;
});

/** Create a long-running durable tick materializer. */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
>(
  config: SchedulerConfig<Payload, Success, Error, QueueR>,
): Scheduler<Payload> => {
  const execute = Effect.gen(function* () {
    let next = yield* materializeDue(config);
    yield* Effect.gen(function* () {
      const sleepFor = Math.max(100, next.getTime() - Date.now());
      yield* Effect.sleep(Duration.millis(sleepFor));
      next = yield* materializeDue(config);
    }).pipe(Effect.repeat(Schedule.forever));
  });
  return {
    ...Effectable.Prototype({
      label: "effectmq/Scheduler",
      evaluate() {
        return execute;
      },
    }),
    [TypeId]: TypeId,
    name: config.name,
    cron: config.cron,
    timeZone: config.cron.tz,
  } as Scheduler<Payload>;
};
