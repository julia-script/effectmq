/** Durable cron tick materialization through ordinary EffectMQ tasks. @module */
import * as Clock from "effect/Clock";
import * as Cron from "effect/Cron";
import type * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Effectable from "effect/Effectable";
import * as Schedule from "effect/Schedule";
import type * as Schema from "effect/Schema";
import type * as StorageProtocol from "./StorageProtocol.js";
import * as TaskEngine from "./TaskEngine.js";
import * as TaskQueue from "./TaskQueue.js";

const TypeId = "~effectmq/Scheduler" as const;

/**
 * The nominal cron interval represented by one durable scheduled task.
 *
 * For a coalesced tick, `missedFrom` and `missedTo` describe the interval that
 * one task represents. For ordinary and backfilled ticks, both equal
 * `scheduledAt`.
 *
 * @category Models
 * @since 0.3.0
 */
export interface Tick {
  readonly scheduleName: string;
  readonly scheduledAt: Date;
  readonly missedFrom: Date;
  readonly missedTo: Date;
}

/**
 * Selects what a scheduler materializes after downtime.
 *
 * `skip` discards missed ticks, `coalesce` creates one task for the most recent
 * missed tick, and `backfill` creates up to `maxBackfill` recent tasks.
 *
 * @category Configuration
 * @since 0.3.0
 */
export type MissedTickPolicy =
  | { readonly _tag: "skip" }
  | { readonly _tag: "coalesce" }
  | { readonly _tag: "backfill"; readonly maxBackfill: number };

/** Predictable validation failure for a scheduler definition. */
export class SchedulerConfigurationError extends Data.TaggedError(
  "SchedulerConfigurationError",
)<{
  readonly field: "maxBackfill";
  readonly constraint: string;
  readonly actual: unknown;
}> {}

const validateConfig = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR,
  QueueIdentityR,
>(
  config: SchedulerConfig<Payload, Success, Error, QueueR, QueueIdentityR>,
): Effect.Effect<void, SchedulerConfigurationError> => {
  if (
    config.missed._tag === "backfill" &&
    (!Number.isSafeInteger(config.missed.maxBackfill) ||
      config.missed.maxBackfill < 1)
  ) {
    return Effect.fail(
      new SchedulerConfigurationError({
        field: "maxBackfill",
        constraint: "a positive safe integer",
        actual: config.missed.maxBackfill,
      }),
    );
  }
  return Effect.void;
};

/**
 * Configures durable cron tick materialization into a task queue.
 *
 * `name` identifies the durable schedule cursor and should remain stable.
 * Generated task identifiers combine that name with the nominal tick time.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface SchedulerConfig<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  QueueIdentityR = Crypto.Crypto,
> {
  readonly name: string;
  /** Cron rule including its optional IANA time zone. */
  readonly cron: Cron.Cron;
  readonly queue: TaskQueue.TaskQueue<
    Payload,
    Success,
    Error,
    QueueR,
    QueueIdentityR
  >;
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
  | SchedulerConfigurationError
  | TaskQueue.OfferError
  | StorageProtocol.StorageProtocolError
  | TaskEngine.TaskEngineError
  | TaskQueue.IndeterminateWriteError
  | TaskQueue.RetentionContextRequired
  | Schema.SchemaError;

/**
 * A long-running Effect that materializes deterministic cron tasks.
 *
 * A scheduler persists its cursor in Redis and offers each selected tick before
 * advancing it. Competing schedulers and crash recovery can therefore re-offer
 * the same deterministic task identity without creating duplicate generations.
 *
 * **Gotchas**
 *
 * The scheduler does not execute tasks. A managed worker processes them with
 * normal queue leases, retries, and at-least-once delivery.
 *
 * @category Models
 * @since 0.1.0
 */
export interface Scheduler<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueIdentityR,
> extends Effect.Effect<
    void,
    SchedulerFailure,
    TaskQueue.OfferRequirements<Payload, Success, Error, QueueIdentityR>
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

/**
 * Materializes the work selected by one bounded scheduler observation.
 *
 * This operation is useful for deterministic tests and custom scheduler loops.
 * It initializes or reads the durable schedule cursor, applies the missed-tick
 * policy, offers deterministic tasks, and advances the cursor only after every
 * selected offer succeeds.
 *
 * @category Operations
 * @since 0.3.0
 */
export const materializeDue = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR,
  QueueIdentityR,
>(
  config: SchedulerConfig<Payload, Success, Error, QueueR, QueueIdentityR>,
  now?: Date,
) {
  yield* validateConfig(config);
  const observedAt = now ?? new Date(yield* Clock.currentTimeMillis);

  const engine = yield* TaskEngine.TaskEngine;
  const initial = config.startAt ?? Cron.next(config.cron, observedAt);
  const firstDue = yield* engine.setSchedule(config.name, initial);
  if (firstDue.getTime() > observedAt.getTime()) return firstDue;

  const following = Cron.next(config.cron, firstDue);
  const hasMissedBacklog = following.getTime() <= observedAt.getTime();
  const nextFuture = Cron.next(config.cron, observedAt);
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
      observedAt,
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

/**
 * Creates a long-running durable tick materializer.
 *
 * Run the returned value as an Effect alongside a `Worker.Worker`. It
 * sleeps until the next cron tick, with a minimum polling delay of 100 ms, and
 * repeats indefinitely.
 *
 * **Example: Materialize a coalesced daily task**
 *
 * ```ts
 * import { Cron, Effect, Schema } from "effect"
 * import { Scheduler, Task, TaskQueue } from "@effectmq/core"
 *
 * const daily = Effect.gen(function* () {
 *   const report = yield* Task.make({
 *     name: "report",
 *     payload: { scheduledAt: Schema.String },
 *     success: Schema.Void,
 *     error: Schema.String
 *   })
 *   const reports = TaskQueue.make("reports", report)
 *   return yield* Scheduler.make({
 *     name: "daily-report",
 *     cron: Cron.parseUnsafe("0 2 * * *", "UTC"),
 *     queue: reports,
 *     payload: (tick) => ({ scheduledAt: tick.scheduledAt.toISOString() }),
 *     missed: { _tag: "coalesce" }
 *   })
 * })
 * ```
 *
 * @category Constructors
 * @since 0.1.0
 */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  QueueR = never,
  QueueIdentityR = Crypto.Crypto,
>(
  config: SchedulerConfig<Payload, Success, Error, QueueR, QueueIdentityR>,
): Effect.Effect<
  Scheduler<Payload, Success, Error, QueueIdentityR>,
  SchedulerConfigurationError
> =>
  Effect.gen(function* () {
    yield* validateConfig(config);
    const execute = Effect.gen(function* () {
      let next = yield* materializeDue(config);
      yield* Effect.gen(function* () {
        const sleepFor = Math.max(
          100,
          next.getTime() - (yield* Clock.currentTimeMillis),
        );
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
    } as Scheduler<Payload, Success, Error, QueueIdentityR>;
  });
