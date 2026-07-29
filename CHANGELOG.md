# @effectmq/core

## 0.3.0

### Minor Changes

- 44b3652: Tasks offered from inside a `TaskQueue.complete` handler now automatically reference the task being processed: the new task is pinned by it (`heldBy`), so its record and result survive until the outer task dies, and is attributed to it (`createdBy`). Pass `detached: true` in the offer options to skip the pin while keeping the `createdBy` attribution. Offers made outside a handler are unchanged.
- 7aa8346: Update Effect to `4.0.0-beta.102`. The `effect` peer dependency range is now `>=4.0.0-beta.102`, and `@effect/platform-node` (dev) is updated to match.
- 9c15e04: Add engine-level task pinning: tasks can hold references on other tasks so their records (and results) survive until every holder is gone.

  - `TaskEngine.createTask` accepts `heldBy` (a list of `{prefix, id}` task refs): each holder pins the new task by incrementing its `refCount`, atomically and validated at creation (holders must exist and be alive). Refs work across queues. Re-creating an existing task (idempotent re-offer) never double-pins, so replaying holders are safe.
  - New alive/dead lifecycle: a task dies only when it is done (terminal success/failure) **and** `refCount` is 0. Completion policies (`onSuccessPolicy`/`onFailurePolicy`) now apply at death — a done-but-pinned task keeps its record in no list until its last holder dies. Unpinned tasks (the default) are observably unchanged. Death releases the task's own refs, cascading through held children in the same atomic operation; the `success`/`failed` lists only ever contain dead tasks.
  - `removeTask` on a pinned task now fails ("task is pinned") — remove the holders first. On an unpinned task it releases the task's refs before deleting, so removing a holder cannot leak its children.
  - New optional `createdBy` task ref on creation: pure provenance metadata for tooling, never used by the lifecycle.

  The user-facing `TaskQueue.offer` options for pinning ship separately.

### Patch Changes

- e4baef6: Fix `stream` (and everything built on it: `wait`, `execute`, `TaskQueue.stream`) failing with a `SchemaError` when using the bundled `NodeRedisPool`: node-redis returns `XREAD` replies as an object keyed by stream name, while the engine only decoded the ioredis-style `[stream, entries]` tuple array. The reply is now normalized before decoding, so both Redis clients work.
- 9408132: Fix four task-engine bugs surfaced by new scheduler and locking tests:

  - Task locks now expire after the intended number of milliseconds (`PX`) instead of interpreting the timeout as seconds (`EX`), so tasks whose worker died are actually recovered as stalled instead of staying locked ~1000× longer than configured.
  - Stalled tasks record the proper `~effectmq/Error/Stalled` tag; previously the raw `Stalled` tag made the typed task decode fail, so a stalled task could never be processed again through `TaskQueue`.
  - `Canceled` errors now short-circuit retries even when a retry time was computed, as the retry-policy spec requires (the tag check read the wrong field).
  - `consumeSchedule` honors the debug-mode mock clock and reports the corrective next run time when a tick is not consumed (Lua `false` return values were being converted to null replies that truncated the response array).

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
