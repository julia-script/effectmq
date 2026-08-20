# @effectmq/core

## 0.3.0-rc.0

### Minor Changes

- Harden the queue protocol and runtime for a first production candidate.

  This is a breaking pre-1.0 storage and API release. Existing pre-v1 Redis data
  must be drained before upgrading; the package includes a read-only inspector.

  - Add generation-specific task handles, explicit created/existing offer
    outcomes, safe duplicate behavior, and deliberate new-generation offers.
  - Fence every attempt with a unique lease token, separate handler failures from
    stalled attempts, use Redis server time, supervise heartbeats, and add scoped
    bounded-concurrency workers with graceful drain.
  - Replace implicit task lifecycle relationships with informational creator
    provenance and explicit set-idempotent result-retention holds.
  - Add the versioned, bounded MessagePack storage protocol with typed corruption,
    compatibility, schema, size, and count failures.
  - Load real packaged Lua through content-addressed `SCRIPT LOAD` / `EVALSHA`
    with binary-safe arguments and `NOSCRIPT` recovery. Redis Functions are not
    used and mixed application versions do not replace one another's scripts.
  - Make events, handle-based waiting, retention, inspection, and maintenance
    bounded and race-safe, with metrics for queue and Redis health.
  - Redesign scheduling as durable idempotent tick-task materialization with
    skip, coalesce, and bounded-backfill policies. Handler execution remains
    at-least-once.
  - Support standalone Redis and Sentinel with explicit TLS/ACL/timeout/pool
    configuration and fail startup for Redis Cluster.
- Add compatibility, property, fault, restart/failover, package-consumer,
  benchmark, soak, documentation, and provenance-bearing release gates.
- Refresh `effect` and direct `@effect/*` development dependencies to
  `4.0.0-beta.107`, the npm `beta` baseline used to verify this candidate.

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
