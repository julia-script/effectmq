import { NodeRedis } from "@effect/platform-node";
import { Cron, Effect, Layer, Schema } from "effect";
import { Scheduler, Task, TaskEngine, TaskQueue } from "./index.js";

const layers = Layer.provideMerge(TaskEngine.layer(), NodeRedis.layer({}));
const foo = Task.make({
  name: "test",
  successSchema: Schema.Number,
  errorSchema: Schema.String,
  payload: Schema.Struct({
    message: Schema.String,
  }),
});
const program = Effect.gen(function* () {
  const queue = TaskQueue.make("myqueue", foo);

  yield* TaskQueue.offer(queue, { message: "test" });
  yield* TaskQueue.offer(queue, { message: "test2" });

  const task = yield* TaskQueue.takeUnsafe(queue);
  yield* TaskQueue.complete(queue, (_task) =>
    Effect.gen(function* () {
      return yield* Effect.succeed(0);
    }),
  );

  yield* Effect.log("task", task);

  yield* Scheduler.make({
    cron: Cron.parseUnsafe("0/3 * * * * *", "America/Los_Angeles"),
    name: "test",
    handler: Effect.log("schedule consumed"),
  });
}).pipe(Effect.provide(layers));

program.pipe(Effect.runPromise);
