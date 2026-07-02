# @effectmq/core

## 0.2.0

### Minor Changes

- 981b9e2: Add task lifecycle events and streaming APIs. The engine publishes `task.created`, `task.updated`, `task.failed`, `task.completed`, and `task.moved` events to a per-queue Redis Stream, exposed as a typed Effect `Stream` via `TaskQueue.stream`. New `TaskQueue.wait(queue, taskId)` awaits a task's terminal outcome, and `TaskQueue.execute(queue, payload)` offers and awaits in one call.

  Also fixes a double-JSON-encoding bug where `task.completed` (and thus `wait`/`execute`) returned the success value wrapped in extra quotes.

- 724e977: Add the `RedisPool` service — the minimal `send`/`eval` Redis surface `TaskEngine` now depends on instead of `Redis` from `effect/unstable` — and `NodeRedisPool`, a bundled connection-pooled implementation backed by [node-redis](https://github.com/redis/node-redis)'s `createClientPool`. `NodeRedisPool.layer(options)` provides `RedisPool` (and the generic `Redis` service for interop), connects lazily on first command, and closes when the layer scope ends.
- 5b8daa7: Move retry configuration to the task definition and drive it with Effect `Schedule`.

  `Task.make` now takes a `retry` option — an Effect `Schedule` (or a `{ while, until, times, schedule }` options object) — that decides when a failed task is retried. On failure the next run time is computed from the schedule and the task lands on the scheduled list until then; when the schedule is exhausted, the failure policy applies. `maxRetries` caps the attempts so an unbounded schedule can't loop forever: it defaults to `5`, is overridable per-`offer` (the per-offer value wins), and can be set to `null` for truly unbounded retries. A `Canceled` error still skips remaining retries. Each stored error entry and the `task.failed` event now carry `retryAt`.

  `Task.make`'s config keys `successSchema`/`errorSchema` are renamed to `success`/`error` (aligning with `payload`).

  The manual queue primitives `takeUnsafe`, `succeed`, and `fail` are no longer exported — use `TaskQueue.complete`, which applies the definition's retry policy on failure.

  Also fixes a double-JSON-encoding bug in success values and a double-decode in the `task.failed`/`task.completed` event schemas.

## 0.1.1

### Patch Changes

- a1630bd: Add a README (quickstart, worker-pool concurrency guidance, Workflow comparison, scheduling) and switch the license to MIT.
