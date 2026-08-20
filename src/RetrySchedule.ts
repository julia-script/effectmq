/** Retry schedule construction and stepping helpers. @internal */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Pull from "effect/Pull";
import * as Schedule from "effect/Schedule";

export const buildFromOptions = <Input>(options: {
  schedule?: Schedule.Schedule<any, Input, any, any>;
  while?: (input: Input) => boolean | Effect.Effect<boolean, any, any>;
  until?: (input: Input) => boolean | Effect.Effect<boolean, any, any>;
  times?: number;
}) => {
  const { while: whileFn, until: untilFn, times } = options;
  let schedule: Schedule.Schedule<any, Input, any, any> = options.schedule
    ? Schedule.passthrough(options.schedule)
    : Schedule.passthrough(Schedule.forever);
  if (whileFn) {
    schedule = Schedule.while(schedule, ({ input }) => {
      const applied = whileFn(input);
      return Effect.isEffect(applied) ? applied : Effect.succeed(applied);
    });
  }
  if (untilFn) {
    schedule = Schedule.while(schedule, ({ input }) => {
      const applied = untilFn(input);
      return Effect.isEffect(applied)
        ? Effect.map(applied, (value) => !value)
        : Effect.succeed(!applied);
    });
  }
  if (times !== undefined) {
    schedule = Schedule.while(schedule, ({ attempt }) =>
      Effect.succeed(attempt <= times),
    );
  }
  return schedule;
};

export const nextRunAt = Effect.fnUntraced(function* <R>(
  schedule: Schedule.Schedule<any, any, any, R>,
  createdAt: Date,
  errors: readonly { readonly timestamp: Date; readonly error: unknown }[],
) {
  const step = yield* Schedule.toStep(schedule);
  let time = createdAt.getTime();
  for (const error of errors) {
    const [, delay] = yield* Pull.catchDone(
      step(error.timestamp.getTime(), error.error),
      (value) => Effect.succeed([value, -1] as const),
    );
    if (delay === -1) return undefined;
    const delayMillis = Duration.toMillis(delay);
    time = error.timestamp.getTime() + delayMillis;
    if (!Number.isFinite(time) || Math.abs(time) > Number.MAX_SAFE_INTEGER) {
      return undefined;
    }
  }
  return time;
});
