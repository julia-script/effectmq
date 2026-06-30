# effectmq

An Effect-first, Redis-backed task queue.

It is meant to occupy a familiar province: the durable queueing country of
BullMQ, but reached by the roads of Effect. Define work with schemas. Offer it
as an Effect. Complete it as an Effect. Let Redis keep the memory when the
process forgets.

## Install

```sh
pnpm add effectmq effect
```

`effectmq` expects a Redis service from `effect/unstable/persistence/Redis`.
Use the Redis client of your choice.

```ts
import { Effect, Layer } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import IORedis from "ioredis";

const RedisLive = Layer.scoped(
  Redis.Redis,
  Effect.acquireRelease(
    Effect.succeed(new IORedis(process.env.REDIS_URL ?? "redis://localhost:6379")),
    (client) => Effect.sync(() => client.disconnect()),
  ).pipe(
    Effect.map((client) =>
      Redis.make({
        send: <A = unknown>(command: string, ...args: ReadonlyArray<string>) =>
          Effect.tryPromise({
            try: () => client.call(command, ...args) as Promise<A>,
            catch: (cause) => new Redis.RedisError({ cause }),
          }),
      }),
    ),
    Effect.flatten,
  ),
);
```

## A Small Civilized Queue

```ts
import { Effect, Layer, Schema } from "effect";
import { Task, TaskEngine, TaskQueue } from "effectmq";

const SendEmail = Task.make({
  name: "send-email",
  payload: {
    userId: Schema.String,
    template: Schema.String,
  },
  successSchema: Schema.String,
  errorSchema: Schema.Struct({ reason: Schema.String }),
  idempotencyKey: (payload) => `email/${payload.userId}/${payload.template}`,
});

const EmailQueue = TaskQueue.make("emails", SendEmail);

const EngineLive = TaskEngine.layer({
  prefix: "my-app",
});

const AppLive = Layer.mergeAll(RedisLive, EngineLive);
```

The task definition is the little law by which the queue lives: payload,
success, error, and idempotency are declared once, then carried through the
producer and worker.

## Produce Work

```ts
const enqueueWelcomeEmail = TaskQueue.offer(
  EmailQueue,
  {
    userId: "usr_123",
    template: "welcome",
  },
  {
    maxRetries: 3,
    onFailurePolicy: "mark-as-failure",
  },
);

await Effect.runPromise(enqueueWelcomeEmail.pipe(Effect.provide(AppLive)));
```

The payload is encoded through its schema before Redis sees it. The returned
task has the engine fields: `id`, `name`, `payload`, timestamps, retry policy,
and error history.

## Consume Work

```ts
const worker = TaskQueue.complete(EmailQueue, (task) =>
  Effect.gen(function* () {
    yield* Effect.log(`sending ${task.payload.template} to ${task.payload.userId}`);

    // Call the mailer here.
    return "sent";
  }),
);

await Effect.runPromise(worker.pipe(Effect.forever, Effect.provide(AppLive)));
```

`complete` takes one task, refreshes its lock while the handler runs, writes the
typed success value on success, or writes the typed error on failure.

```ts
const worker = TaskQueue.complete(EmailQueue, (task) =>
  task.payload.template === "welcome"
    ? Effect.succeed("sent")
    : Effect.fail({ reason: "unknown template" }),
);
```

## Delay, Retry, Remember

```ts
yield* TaskQueue.offer(
  EmailQueue,
  { userId: "usr_123", template: "trial-ending" },
  {
    delay: 60_000,
    maxRetries: 5,
    onSuccessPolicy: "delete",
    onFailurePolicy: "mark-as-failure",
  },
);
```

Completion policies:

- `delete`: remove the task after completion.
- `keep`: remove it from active lists, but leave the task hash.
- `mark-as-success`: move it to the success list.
- `mark-as-failure`: move it to the failure list.

## Cron Without Multiplication

When many workers are alive, a scheduled job should not become many jobs.
`Scheduler` stores the next tick in the engine and lets one worker consume it.

```ts
import * as Cron from "effect/Cron";
import { Scheduler, TaskQueue } from "effectmq";

const NightlyDigest = Scheduler.make({
  name: "nightly-digest",
  cron: Cron.unsafeParse("0 0 * * *"),
  handler: TaskQueue.offer(EmailQueue, {
    userId: "all",
    template: "digest",
  }).pipe(Effect.asVoid),
});

await Effect.runPromise(NightlyDigest.pipe(Effect.provide(AppLive)));
```

## The Shape of It

- `Task`: a typed description of work.
- `TaskQueue`: the high-level API: `make`, `offer`, `complete`, `takeUnsafe`.
- `TaskEngine`: the Redis/Lua engine underneath.
- `Scheduler`: cron coordination built on the same engine.

BullMQ gives you the machinery of queues. `effectmq` tries to give you the same
machinery without asking you to step outside the grammar of Effect.

## Status

This package is young. The API is small on purpose. The machinery is Redis,
Lua scripts, locks, retries, delayed work, and cron coordination; the public
surface is kept narrow so the next version does not have to apologize for this
one.
