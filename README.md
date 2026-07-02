# @effectmq/core

It's a task queue built on [Effect](https://effect.website): typed payloads, typed results, typed errors, all the way down. You describe a unit of work as a schema, hand it to a queue, and process it with a handler that is just an `Effect`. Retries, delays, idempotency, cron schedules: handled. The available engine is backed by Redis, but, like many things in Effect, it can be swapped for a different implementation.

```bash
pnpm add @effectmq/core effect@4.0.0-beta.85 @effect/platform-node@4.0.0-beta.85
```

This library is built on the Effect 4 beta and doesn't work with the current stable Effect release. The examples below use the bundled `NodeRedisPool` layer, a connection-pooled Redis client that ships with the package (`@effect/platform-node` is only needed for `NodeRuntime`). This is beta-era software riding beta-era Effect; pin accordingly.

---

## In thirty seconds

Define a task, enqueue work, process it. The whole loop:

```ts
import { Effect, Layer, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { NodeRedisPool, Task, TaskEngine, TaskQueue } from "@effectmq/core";

const SendEmail = Task.make({
  name: "send-email",
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  error: Schema.Never,
});

const emails = TaskQueue.make("emails", SendEmail);

const program = Effect.gen(function* () {
  yield* TaskQueue.offer(emails, { to: "ada@example.com", subject: "Welcome" });

  yield* TaskQueue.complete(emails, (task) =>
    sendViaProvider(task.payload), // returns the provider message id
  );
});

// The engine + its Redis layer: the only wiring you need to run the above.
const AppLayer = Layer.provideMerge(TaskEngine.layer(), NodeRedisPool.layer());

program.pipe(Effect.provide(AppLayer), NodeRuntime.runMain);
```

That's the shape of it. The rest of this README explains the pieces (typed errors, retries, worker pools, schedules) and the one thing the library deliberately *doesn't* do.

---

## The setup, once

`TaskEngine.layer()` requires the `RedisPool` service. `NodeRedisPool` — bundled with the package, a connection pool backed by [node-redis](https://github.com/redis/node-redis) — provides it:

```ts
import { Layer } from "effect";
import { NodeRedisPool, TaskEngine } from "@effectmq/core";

const AppLayer = Layer.provideMerge(
  TaskEngine.layer(),
  NodeRedisPool.layer({ url: "redis://localhost:6379" }),
);
```

`NodeRedisPool.layer()` accepts node-redis client options and connects lazily on first command. It's the convenient default, but anything that provides the `RedisPool` service works — it's just `send` + `eval`, so you can back it with your own client (ioredis, an in-memory fake for tests) or a Redis-compatible server (Valkey, Dragonfly, and friends).

`TaskEngine` is the machinery underneath: atomic Lua scripts, locks, the lists tasks move between. Provide its layer and forget it; the API you live in is `TaskQueue` and `Scheduler`.

---

## Define a task

A task is a *schema*, not a function. You declare what goes in (`payload`), what a success looks like, and what a failure looks like. The `idempotencyKey` decides what "the same task" means: offer the same key twice and you get one task, not two.

A tagged error makes failures pattern-matchable downstream, so reach for `Schema.TaggedErrorClass` rather than a bare struct.

```ts
import { Schedule, Schema } from "effect";
import { Task, TaskQueue } from "@effectmq/core";

class EmailRejected extends Schema.TaggedErrorClass<EmailRejected>()(
  "EmailRejected",
  { reason: Schema.String },
) {}

const SendEmail = Task.make({
  name: "send-email",
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String, // e.g. a provider message id
  error: EmailRejected,
  // optional, but it's how you ensure the same job isn't enqueued twice
  idempotencyKey: (p) => `email:${p.to}:${p.subject}`,
  // retry with exponential backoff; maxRetries caps it (default 5)
  retry: Schedule.exponential("1 second"),
});

const emails = TaskQueue.make("emails", SendEmail);
```

## Offer work, then do it

`offer` enqueues a payload. `complete` takes the next task, runs your handler, reports the outcome back to the engine, and returns the task's id (a failing handler is routed per the queue's failure policy). The task your handler receives is fully decoded: `task.payload` is the real object, not a JSON string.

```ts
import { Effect } from "effect";

const program = Effect.gen(function* () {
  yield* TaskQueue.offer(emails, {
    to: "ada@example.com",
    subject: "Welcome",
  });

  // Take one task, run it, report the outcome. Resolves with the task id.
  const taskId = yield* TaskQueue.complete(emails, (task) =>
    Effect.gen(function* () {
      const id = yield* sendViaProvider(task.payload); // your code
      return id; // matches successSchema
      // ...or `yield* new EmailRejected({ reason })` to fail with the typed error
    }),
  );
  // Success resolves per onSuccessPolicy; a failing handler is routed per onFailurePolicy.
});
```

One `complete` processes one task. To process *many*, set your workers up accordingly.

### Predefine the handler

Handlers are just functions and workers are just Effects, so both are values you can name once and reuse. Type a handler with `TaskHandler` to declare it next to the task definition, before any queue exists; bind it to a queue with `complete` and you have a worker effect you can run, repeat, or fork like any other:

```ts
import { Effect, Schedule } from "effect";
import { TaskQueue, type TaskHandler } from "@effectmq/core";

// Declared against the task definition — no queue in sight yet.
const handleSendEmail: TaskHandler<
  typeof SendEmail.payloadSchema,
  typeof SendEmail.successSchema,
  typeof SendEmail.errorSchema
> = (task) => sendViaProvider(task.payload);

// Bound to a queue: an Effect that takes one task and runs it to completion.
const sendEmailWorker = TaskQueue.complete(emails, handleSendEmail);

const program = Effect.gen(function* () {
  yield* sendEmailWorker; // process one task...
  yield* sendEmailWorker.pipe(Effect.repeat(Schedule.forever)); // ...or loop forever
});
```

---

## Streaming & events

The engine publishes a lifecycle event to a per-queue Redis Stream every time a task changes state. `TaskQueue.stream` hands you those events as an Effect `Stream`, decoded against your queue's schemas: `task.created` and `task.updated` carry fully-typed tasks, `task.failed` carries your typed error, `task.completed` carries your typed success value, and `task.moved` reports the list transition.

```ts
import { Effect, Stream } from "effect";

const watch = TaskQueue.stream(emails).pipe(
  Stream.runForEach((event) => Effect.log(event._tag, event.taskId)),
);
```

Because it's just a stream of terminal events, you can also *wait on a specific task*. `wait` blocks until a task id reaches a terminal state, resolving with its success value or failing with its typed error. `execute` is the offer-and-wait shortcut: enqueue a payload and get its outcome back in one call.

```ts
// Offer + await the result in one call.
const messageId = yield* TaskQueue.execute(emails, {
  to: "ada@example.com",
  subject: "Welcome",
}); // resolves with the success value, or fails with EmailRejected

// Or await a task you already offered.
const task = yield* TaskQueue.offer(emails, payload);
const result = yield* TaskQueue.wait(emails, task.id);
```

`execute` opens the stream *before* offering, so even a handler that finishes near-instantly won't slip its terminal event past you. Streams poll Redis (default every second); pass a cursor to resume from a known event id.

---

## On concurrency

Differently than other queue libraries, `effectmq` doesn't have builtin concurrency, rate limiting, backpressure. It doesn't need to, it works perfectly with the Effect primitives you are used to.

Effect gives you the fine control you need from your workers,  so `complete` does exactly one task, and *you* decide how many run at once, with the same tools you use everywhere else:

```ts
import { Effect, Schedule, Semaphore } from "effect";

// Concurrency example with semaphore
const worker = Effect.gen(function* () {
  // At most 5 tasks in flight at any moment.
  const semaphore = yield* Semaphore.make(5);

  yield* Semaphore.withPermit(
    semaphore,
    TaskQueue.complete(emails, (task) => handle(task)),
  ).pipe(
    Effect.forkScoped,            // each worker is its own fiber
    Effect.repeat(Schedule.forever), // ...that keeps pulling work
  );
});
```

Want a rate limit instead of a raw permit count? Compose one from a `Semaphore` and a `Schedule`. Want retries with jitter? `Schedule`. Want to fan out across a cluster? Run more processes. None of it is our invention, all of it composes. The queue's job is to hand you the next task, exactly once, safely. What you do with your fibers is your business.

---

## "But Effect already has Workflow"

It does, and it's excellent, for a different problem. [Effect Workflow](https://effect.website) is **durable execution**: long-running, multi-step sagas that survive process death, resume exactly where they left off, and persist *every* intermediate step so the whole history can be replayed. It leans on clustering and sharding; nodes have to be live and coordinated; the durability is total because the use case demands it.

That power has a price that sometimes isn't worth paying. Sometimes you don't have a saga. You have a job. "Send this email." "Resize that image." "Spawn 5 AI agents to complete these tasks." There's no multi-step history worth replaying; there's a payload, a handler, and an outcome. Reaching for durable execution there is like renting a shipping container to mail a letter.

`effectmq` works whether you have a single worker running in a separate fiber or a hundred distributed across multiple processes.

So:

> If **Workflow is Temporal** (durable, replayable, cluster-coordinated orchestration) then **this is BullMQ**: a queue. You put work in, workers take it out, it runs once, retries if it must, and then it's done. No replay log, no shard map, no requirement that the whole cluster be breathing. Just a queue, with Effect's types and Effect's primitives.

Pick durable execution when the *process* is the thing you can't afford to lose. Pick a queue when the *work* is.

---

## Scheduling

For recurring work, `Scheduler.make` runs a handler on a cron expression. If you've used Effect's [`ClusterCron`](https://effect.website) (`effect/unstable/cluster/ClusterCron`), it'll feel familiar. The schedule state lives in the engine, so multiple processes running the same named scheduler will collectively fire the handler once per tick, not once per process.

```ts
import { Cron } from "effect";
import { Scheduler } from "@effectmq/core";

const nightlyReport = Scheduler.make({
  name: "nightly-report",
  cron: Cron.parseUnsafe("0 2 * * *"), // 02:00 every day
  handler: Effect.gen(function* () {
    yield* buildAndSendReport();
  }),
});

```

---

## Notes

- **Completion policies.** `offer` accepts `onSuccessPolicy` and `onFailurePolicy`, each one of `delete` | `keep` | `mark-as-success` | `mark-as-failure`. They decide where a finished task lands: gone, quietly retained, or parked on the success/failed list for inspection. Defaults are `delete`.
- **Retries.** Declare `retry` on the task definition (`Task.make`) as an Effect `Schedule` — or a `{ while, until, times, schedule }` options object. On failure the next run time is computed from the schedule and the task lands on the scheduled list until then; when the schedule is exhausted, the failure policy applies. `maxRetries` caps the attempts so an unbounded schedule (e.g. `Schedule.forever`) can't loop forever: it defaults to `5`, is overridable per-`offer` (the per-offer value wins), and set it to `null` for truly unbounded retries. A `Canceled` error skips remaining retries.
- **Idempotency.** The `idempotencyKey` is the task id. Same key, same task: offering again updates rather than duplicates.
- **Delays.** `offer(..., { delay })` schedules the task for the future; it sits on the scheduled list until its time comes.
- **The engine.** `TaskEngine` is the low-level, Lua-backed layer all of this sits on. You provide its layer; you rarely call it directly.

---

## License

MIT.
