# @effectmq/core

It's a task queue built on [Effect](https://effect.website): typed payloads, typed results, typed errors, all the way down. You describe a unit of work as a schema, hand it to a queue, and process it with a handler that is just an `Effect`. Retries, delays, idempotency, cron schedules: handled. The available engine is backed by Redis, but, like many things in Effect, it can be swapped for a different implementation.

```bash
pnpm add @effectmq/core effect@4.0.0-beta.107 @effect/platform-node@4.0.0-beta.107
```

This library is built on the Effect 4 beta and doesn't work with the current stable Effect release. The examples below use the bundled `NodeRedisPool` layer, a connection-pooled Redis client that ships with the package (`@effect/platform-node` is only needed for `NodeRuntime`). This is beta-era software riding beta-era Effect; pin accordingly.

Node.js 22.19 or newer is required; the release matrix verifies Node.js 22 and 24.

---

## In thirty seconds

Define a task, enqueue work, process it. The whole loop:

```ts
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { Task, TaskEngine, TaskQueue } from "@effectmq/core";

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
    Effect.succeed(`provider:${task.payload.to}`),
  );
});

// The engine + its Redis layer: the only wiring you need to run the above.
const AppLayer = TaskEngine.layer({
  redis: { url: "redis://localhost:6379" },
});

program.pipe(Effect.provide(AppLayer), NodeRuntime.runMain);
```

That's the shape of it. The rest of this README explains the pieces (typed errors, retries, worker pools, schedules) and the one thing the library deliberately *doesn't* do.

---

## The setup, once

`TaskEngine.layer()` is the complete Node live graph: it provides the engine,
cryptographic identity generation, and the retained Redis pool, role, and
health services:

```ts
import { TaskEngine } from "@effectmq/core";

const AppLayer = TaskEngine.layer({
  redis: { url: "redis://localhost:6379" },
});
```

Use `TaskEngine.layerNoDeps()` when composing a custom `RedisPool`
implementation. `NodeRedisPool.layer()` remains available independently and
accepts node-redis client options. It establishes separate producer, worker,
and maintenance pools when the Layer starts. It supports standalone Redis and
Sentinel; Redis Cluster fails startup because queue transitions use multi-key
atomic scripts. See the [operations runbook](./docs/operations.md) for TLS,
ACL, bounded-pool, persistence, failover, health, and shutdown guidance.

The tested platform matrix is in the [support policy](./docs/support-policy.md), and reproducible throughput/tail-latency results are published as [performance evidence](./docs/performance.md).

`TaskEngine` is the machinery underneath: atomic Lua scripts, locks, the lists tasks move between. Provide its layer and forget it; the API you live in is `TaskQueue` and `Scheduler`.

---

## Define a task

A task is a *schema*, not a function. You declare what goes in (`payload`), what a success looks like, and what a failure looks like. The `idempotencyKey` decides what "the same task" means: offer the same key twice and you get one task, not two.

A tagged error makes failures pattern-matchable downstream, so reach for `Schema.TaggedError` rather than a bare struct.

```ts docs-check=email
import { Effect, Schedule, Schema, Stream } from "effect";
import { Task, TaskQueue, type TaskHandler, Worker } from "@effectmq/core";

class EmailRejected extends Schema.TaggedError<EmailRejected>()(
  "EmailRejected",
  { reason: Schema.String },
) {}

const SendEmail = Task.make({
  name: "send-email",
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  error: EmailRejected,
  idempotencyKey: (p) => `email:${p.to}:${p.subject}`,
  retry: Schedule.exponential("1 second"),
});

const emails = TaskQueue.make("emails", SendEmail);

const sendViaProvider = (payload: { readonly to: string }) =>
  Effect.succeed(`provider:${payload.to}`);
```

## Offer work, then do it

`offer` enqueues a payload. `complete` takes the next task, runs your handler, reports the outcome back to the engine, and returns the task's id (a failing handler is routed per the queue's failure policy). The task your handler receives is fully decoded: `task.payload` is the real object, not a JSON string.

```ts docs-check=email
const offerAndCompleteProgram = Effect.gen(function* () {
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

```ts docs-check=email
// Declared against the task definition — no queue in sight yet.
const handleSendEmail: TaskHandler<
  typeof SendEmail.payloadSchema,
  typeof SendEmail.successSchema,
  typeof SendEmail.errorSchema
> = (task) => sendViaProvider(task.payload);

// Bound to a queue: an Effect that takes one task and runs it to completion.
const sendEmailWorker = TaskQueue.complete(emails, handleSendEmail);

const repeatedWorkerProgram = Effect.gen(function* () {
  yield* sendEmailWorker; // process one task...
  yield* sendEmailWorker.pipe(Effect.repeat(Schedule.forever)); // ...or loop forever
});
```

---

## Streaming & events

The engine publishes a lifecycle event to a per-queue Redis Stream every time a task changes state. `TaskQueue.stream` hands you those events as an Effect `Stream`, decoded against your queue's schemas: `task.created` and `task.updated` carry fully-typed tasks, `task.failed` carries your typed error or a built-in stalled/canceled error, `task.completed` carries your typed success value, and `task.moved` reports the list transition.

```ts docs-check=email
const watch = TaskQueue.stream(emails).pipe(
  Stream.runForEach((event) => Effect.log(event._tag, event.taskId)),
);
```

Because it's just a stream of terminal events, you can also *wait on a specific task*. `wait` blocks until a task id reaches a terminal state, resolving with its success value or failing with its typed error. `execute` is the offer-and-wait shortcut: enqueue a payload and get its outcome back in one call.

```ts docs-check=email
// Offer + await the result in one call.
const executeMessage = TaskQueue.execute(emails, {
  to: "ada@example.com",
  subject: "Welcome",
}); // resolves with the success value, or fails with EmailRejected

// Or await a task you already offered.
const offerAndWait = Effect.gen(function* () {
  const task = yield* TaskQueue.offer(emails, {
    to: "grace@example.com",
    subject: "Welcome",
  });
  return yield* TaskQueue.wait(emails, task.handle);
});
```

`wait` reads durable state, subscribes from the handle's authoritative Redis cursor, and rechecks state after subscription, so completion before or during subscription is observed. Streams use a blocking Redis read (default two-second block); persist a retained cursor when building a resumable event consumer.

---

## On concurrency

`complete` processes exactly one task. For a long-running process, `Worker`
provides bounded local concurrency, lease supervision, maintenance, and graceful
draining:

```ts docs-check=email
const worker = Worker.make(emails, handleSendEmail, { concurrency: 5 });
const program = Worker.run(worker);
```

The built-in worker does not impose distributed/global concurrency or rate
limits. Compose those policies from Effect primitives or external coordination,
and run more worker processes to fan out. The queue preserves eligible work and
fences the current attempt; handlers remain at-least-once and must make external
side effects idempotent.

---

## "But Effect already has Workflow"

It does, and it's excellent, for a different problem. [Effect Workflow](https://effect.website) is **durable execution**: long-running, multi-step sagas that survive process death, resume exactly where they left off, and persist *every* intermediate step so the whole history can be replayed. It leans on clustering and sharding; nodes have to be live and coordinated; the durability is total because the use case demands it.

That power has a price that sometimes isn't worth paying. Sometimes you don't have a saga. You have a job. "Send this email." "Resize that image." "Spawn 5 AI agents to complete these tasks." There's no multi-step history worth replaying; there's a payload, a handler, and an outcome. Reaching for durable execution there is like renting a shipping container to mail a letter.

`effectmq` works whether you have a single worker running in a separate fiber or a hundred distributed across multiple processes.

So:

> If **Workflow is Temporal** (durable, replayable, cluster-coordinated orchestration) then **this is BullMQ**: a queue. You put work in, workers take attempts, and Redis keeps unfinished work recoverable across retries and worker loss. No workflow replay log or shard map—just a queue with Effect's types and primitives.

Pick durable execution when the *process* is the thing you can't afford to lose. Pick a queue when the *work* is.

---

## Scheduling

For recurring work, `Scheduler.make` durably materializes an ordinary queue
task for each selected cron tick. The task id is derived from the schedule name
and nominal tick time, so competing schedulers and crash recovery can safely
re-offer it. A managed worker executes the task with the queue's normal leases,
retries, and **at-least-once** delivery semantics.

```ts
import { Cron, Effect, Schema } from "effect";
import { Scheduler, Task, TaskQueue, Worker } from "@effectmq/core";

const reportTask = Task.make({
  name: "nightly-report-task",
  payload: { scheduledAt: Schema.String },
  success: Schema.Void,
  error: Schema.String,
});
const reportQueue = TaskQueue.make("nightly-reports", reportTask);
const schedule = Scheduler.make({
  name: "nightly-report",
  cron: Cron.parseUnsafe("0 2 * * *", "UTC"),
  queue: reportQueue,
  payload: (tick) => ({ scheduledAt: tick.scheduledAt.toISOString() }),
  missed: { _tag: "coalesce" },
});
const worker = Worker.make(reportQueue, ({ payload }) =>
  Effect.log(`Building report for ${payload.scheduledAt}`),
);
```

---

## Notes

- **Completion policies.** `offer` accepts `onSuccessPolicy` and `onFailurePolicy`, each one of `delete` | `keep` | `mark-as-success` | `mark-as-failure`. They decide where a finished task lands: gone, quietly retained, or parked on the success/failed list for inspection. Defaults are `delete`.
- **Retries.** Declare `retry` on the task definition (`Task.make`) as an Effect `Schedule` — or a `{ while, until, times, schedule }` options object. On failure the next run time is computed from the schedule and the task lands on the scheduled list until then; when the schedule is exhausted, the failure policy applies. `maxRetries` caps the attempts so an unbounded schedule (e.g. `Schedule.forever`) can't loop forever: it defaults to `5`, is overridable per-`offer` (the per-offer value wins), and set it to `null` for truly unbounded retries. A `Canceled` error skips remaining retries.
- **Idempotency.** The `idempotencyKey` is the task id. By default, offering the same key returns the existing generation unchanged; replacement requires explicit new-generation mode.
- **Delays.** `offer(..., { delay })` schedules the task for the future; it sits on the scheduled list until its time comes.
- **The engine.** `TaskEngine` is the low-level, Lua-backed layer all of this sits on. You provide its layer; you rarely call it directly.

## Production guides

- [Architecture](./docs/architecture.md)
- [Runtime boundaries](./docs/runtime-boundaries.md)
- [API reference](./docs/api-reference.md)
- [Delivery guarantees](./docs/delivery-guarantees.md)
- [Idempotent offers](./docs/idempotent-offers.md)
- [Task relationships](./docs/task-relationships.md)
- [Durable scheduling](./docs/scheduler.md)
- [Storage protocol v1](./docs/storage-protocol-v1.md)
- [Operations](./docs/operations.md)
- [Support policy](./docs/support-policy.md)
- [Upgrade and rollback](./docs/upgrade-and-rollback.md)
- [Performance evidence](./docs/performance.md)
- [Soak evidence](./docs/soak.md)
- [Release process](./docs/releasing.md)
- [Current release record](./docs/release-readiness.md)

---

## License

MIT.
