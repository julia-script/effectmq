# effectmq

**A typed, Redis-backed task queue for Effect 4.** Define work with schemas, run
handlers as Effects, and keep payloads, results, and failures typed from producer
to worker.

[Website](https://docs-one-eta-87.vercel.app/) ·
[Documentation](https://docs-one-eta-87.vercel.app/docs) ·
[Getting started](https://docs-one-eta-87.vercel.app/docs/tutorials/getting-started) ·
[API reference](https://docs-one-eta-87.vercel.app/docs/reference/task-queue) ·
[npm](https://www.npmjs.com/package/@effectmq/core)

effectmq is for background jobs that need durable Redis state without becoming a
workflow engine: send an email, resize an image, refresh a cache, or materialize
a scheduled report. It provides:

- schema-checked payloads, successes, and failures;
- at-least-once delivery with fenced attempts and stalled-worker recovery;
- Effect `Schedule` retries, delayed offers, deduplication, and durable cron;
- bounded local worker concurrency, graceful draining, and maintenance;
- typed lifecycle streams plus `wait` and `execute` for durable results.

## Install

```bash
# Node
pnpm add @effectmq/core@0.3.0-rc.0 effect@4.0.0-beta.107 @effect/platform-node@4.0.0-beta.107

# Bun (node-redis is bundled; add platform-bun for BunRuntime and BunCrypto)
bun add @effectmq/core@0.3.0-rc.0 effect@4.0.0-beta.107 @effect/platform-bun@4.0.0-beta.107
```

> [!IMPORTANT]
> effectmq currently targets the Effect 4 beta and is not compatible with the
> stable Effect 3 release. Pin the versions shown above. Node.js 22.19 or newer
> is required; CI verifies Node.js 22 and 24. Bun plus node-redis is a
> supported composition. It is not yet a CI platform.

---

## Quick start

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

That is the complete producer-to-worker loop. For a clean-room walkthrough with
Redis startup and expected output, follow the
[getting-started tutorial](https://docs-one-eta-87.vercel.app/docs/tutorials/getting-started).

---

## Runtime setup

`TaskEngine.layer()` is the live graph: engine, cryptographic identity, and
the Redis pool, role, and health services. Run it with `NodeRuntime` or
`BunRuntime`. The Redis adapter stays `NodeRedisPool` on both.

```ts
import { TaskEngine } from "@effectmq/core";

const AppLayer = TaskEngine.layer({
  redis: { url: "redis://localhost:6379" },
});
```

Bun plus node-redis uses the same `TaskEngine.layer` call and
`BunRuntime.runMain`. Swap `NodeCrypto` for `BunCrypto` when you compose
yourself through `TaskEngine.layerNoDeps()`. Bun's built-in `RedisClient`
is not the supported adapter yet. It has no binary `send`.

```ts
import { Effect, Layer } from "effect";
import { BunCrypto, BunRuntime } from "@effect/platform-bun";
import { NodeRedisPool, TaskEngine } from "@effectmq/core";

const AppLayer = TaskEngine.layerNoDeps().pipe(
  Layer.provideMerge(
    Layer.merge(
      NodeRedisPool.layer({ url: "redis://localhost:6379" }),
      BunCrypto.layer,
    ),
  ),
);

Effect.void.pipe(Effect.provide(AppLayer), BunRuntime.runMain);
```

Use `TaskEngine.layerNoDeps()` when you bring your own `RedisPool`.
`NodeRedisPool.layer()` remains available independently and
accepts node-redis client options. It establishes separate producer, worker,
and maintenance pools when the Layer starts. It supports standalone Redis and
Sentinel; Redis Cluster fails startup because queue transitions use multi-key
atomic scripts. See the [operations runbook](./docs/operations.md) for TLS,
ACL, bounded-pool, persistence, failover, health, and shutdown guidance.

The tested platform matrix is in the [support policy](./docs/support-policy.md), and reproducible throughput/tail-latency results are published as [performance evidence](./docs/performance.md).

`TaskEngine` is the machinery underneath: atomic Lua scripts, leases, and the
lists tasks move between. Provide its layer once; application code normally
lives in `TaskQueue`, `Worker`, and `Scheduler`.

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

Handlers are functions and workers are Effects, so both are values you can name
once and reuse. Type a handler with `TaskHandler` to declare it next to the task
definition before any queue exists. Bind it with `complete` for one task, or use
the managed `Worker` shown below for a long-running process:

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

## Run a worker

`complete` processes exactly one task. For a long-running process, `Worker`
provides bounded local concurrency, lease supervision, maintenance, and graceful
draining:

```ts docs-check=email
const worker = Worker.make(emails, handleSendEmail, { concurrency: 5 });
const program = Worker.run(worker);
```

`concurrency` is local to one worker process (valid values: 1–1000). Run more
processes to fan out. Distributed/global concurrency and rate limits require
external coordination; effectmq does not pretend a process-local semaphore can
enforce them. The queue fences each attempt, but handlers remain at-least-once,
so make external side effects idempotent.

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
- **Retries.** Declare `retry` on the task definition (`Task.make`) as an Effect `Schedule` — or a `{ while, until, times, schedule }` options object. On failure the next run time is computed from the schedule and the task lands on the scheduled list until then; when the schedule is exhausted, the failure policy applies. The task-level `maxRetries` cap defaults to `5`; pass `null` there for an intentionally unbounded cap. An `offer` may override the cap with a finite non-negative number. Built-in canceled or stalled failures are not retried by the handler schedule.
- **Idempotency.** The `idempotencyKey` is the task id. By default, offering the same key returns the existing generation unchanged; replacement requires explicit new-generation mode.
- **Delays.** `offer(..., { delay })` schedules the task for the future; it sits on the scheduled list until its time comes.
- **The engine.** `TaskEngine` is the low-level, Lua-backed layer all of this sits on. You provide its layer; you rarely call it directly.

## Go deeper

| If you need to… | Read… |
| --- | --- |
| learn the library from a running example | [Getting started](https://docs-one-eta-87.vercel.app/docs/tutorials/getting-started) |
| process, schedule, retry, or await tasks | [How-to guides](https://docs-one-eta-87.vercel.app/docs/how-to/process-tasks) |
| look up exact API behavior | [API reference](./docs/api-reference.md) |
| understand delivery and task identity | [Delivery guarantees](./docs/delivery-guarantees.md) · [Idempotent offers](./docs/idempotent-offers.md) |
| operate Redis and plan upgrades | [Operations](./docs/operations.md) · [Upgrade and rollback](./docs/upgrade-and-rollback.md) |
| inspect architecture and storage contracts | [Architecture](./docs/architecture.md) · [Runtime boundaries](./docs/runtime-boundaries.md) · [Storage protocol v1](./docs/storage-protocol-v1.md) |
| evaluate support and performance | [Support policy](./docs/support-policy.md) · [Performance evidence](./docs/performance.md) · [Soak evidence](./docs/soak.md) |
| release the package | [Release process](./docs/releasing.md) · [Current release record](./docs/release-readiness.md) |

---

## License

MIT.
