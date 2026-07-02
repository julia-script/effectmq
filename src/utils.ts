import { Duration, Effect, Pull, Schedule } from "effect";

export const buildFromOptions = <Input>(options: {
  schedule?: Schedule.Schedule<any, Input, any, any> | undefined;
  while?:
    | ((input: Input) => boolean | Effect.Effect<boolean, any, any>)
    | undefined;
  until?:
    | ((input: Input) => boolean | Effect.Effect<boolean, any, any>)
    | undefined;
  times?: number | undefined;
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
        ? Effect.map(applied, (b) => !b)
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
  errors: { timestamp: Date; error: unknown }[],
) {
  const step = yield* Schedule.toStep(schedule);
  let time = createdAt.getTime();
  for (const error of errors) {
    const [_, delay] = yield* Pull.catchDone(
      step(error.timestamp.getTime(), error.error),
      (v) => {
        return Effect.succeed([v, -1] as const);
      },
    );
    if (delay === -1) {
      return undefined;
    }
    time = error.timestamp.getTime() + Duration.toMillis(delay);
  }
  return time;
});
