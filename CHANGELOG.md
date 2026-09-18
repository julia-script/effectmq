# @effectmq/core

## 0.3.0

### Minor Changes

- b053211: Add durable application event queues with idempotent named subscriptions,
  independent fenced deliveries and acknowledgements, removal waivers, optional
  expiration, and queue-configured deletion or archival. Export EventQueue,
  EventEngine, and EventRecord with managed processing and bounded maintenance.
- 56d3720: Harden the queue protocol and runtime for a first production candidate.

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
  - Refresh `effect` and direct `@effect/*` development dependencies to the npm
    `beta` baseline resolved for the candidate (`4.0.0-beta.107`).
  - Require Node.js 22.19 or newer, matching the runtime floor of the verified
    Effect beta platform stack; Node.js 20 is not part of the support contract.

- 9927ba8: Add opt-in typed task progress and lifecycle history backed by per-generation Redis Streams. Managed handlers can emit progress through an attempt-bound context, and readers can page history using durable cursors with explicit trimming gaps. History is unlimited by default, supports an optional oldest-first count cap, and follows the task record's retention and disposal. Progress failures remain operational, including uncertain writes that are not automatically replayed.

  Upgrade every queue process before enabling progress; dispose of enabled generations through the upgraded engine before rolling back to a version without history support.

### Patch Changes

- d485338: Make public Effect contracts honest and restructure the package around focused
  task-record and task-event modules. Task, queue, worker, and scheduler
  definitions remain pure while programmer-authored definition invariants become
  defects at their first runtime use. TaskEngine errors use semantic reason tags,
  the standard `TaskEngine.layer` is fully wired, and malformed codec/Redis inputs
  remain in typed error channels.
- c42c4be: Upgrade Effect, @effect/platform-node, and @effect/vitest to 4.0.0-rc.115,
  and raise the Effect peer dependency minimum to the same release candidate.
  Adapt schema codecs and configuration to the RC APIs, preserve scoped Redis
  subscriptions in the live and test adapters, and update the matching test
  integration to Vitest 5.
- e13bdb8: Fix queue correctness and boundary validation found during a repository-wide
  review:

  - allow `Schema.Void` tasks to persist and recover successful completion;
  - keep `wait` and `execute` subscribed across retryable failures;
  - reject queue/task descriptors that do not match a persisted handle;
  - validate numeric offer, lease, and retry-timestamp inputs before mutating
    Redis;
  - validate decoded storage values, preserve prototype-sensitive object keys,
    and reject corrupt Redis numbers and cursors in typed error channels;
  - schema-validate built-in failure events and keep their public type precise;
  - keep stalled-attempt history out of handler retry schedules, settle attempts
    when retry-policy evaluation fails, stop safely when retained history is too
    short to replay, and retain terminal failures even when error history is
    disabled;
  - preserve retry-policy interruption for lease recovery and avoid full
    wait-list scans on creation, acquisition, and non-waiting transitions; and
  - reject unsafe worker concurrency, timing, and lease supervision options
    before acquiring work or starting fibers.

- e4baef6: Fix `stream` (and everything built on it: `wait`, `execute`, `TaskQueue.stream`) failing with a `SchemaError` when using the bundled `NodeRedisPool`: node-redis returns `XREAD` replies as an object keyed by stream name, while the engine only decoded the ioredis-style `[stream, entries]` tuple array. The reply is now normalized before decoding, so both Redis clients work.
- 9408132: Fix four task-engine bugs surfaced by new scheduler and locking tests:

  - Task locks now expire after the intended number of milliseconds (`PX`) instead of interpreting the timeout as seconds (`EX`), so tasks whose worker died are actually recovered as stalled instead of staying locked ~1000× longer than configured.
  - Stalled tasks record the proper `~effectmq/Error/Stalled` tag; previously the raw `Stalled` tag made the typed task decode fail, so a stalled task could never be processed again through `TaskQueue`.
  - `Canceled` errors now short-circuit retries even when a retry time was computed, as the retry-policy spec requires (the tag check read the wrong field).
  - `consumeSchedule` honors the debug-mode mock clock and reports the corrective next run time when a tick is not consumed (Lua `false` return values were being converted to null replies that truncated the response array).

## 0.3.0-rc.2

### Minor Changes

- b053211: Add durable application event queues with idempotent named subscriptions,
  independent fenced deliveries and acknowledgements, removal waivers, optional
  expiration, and queue-configured deletion or archival. Export EventQueue,
  EventEngine, and EventRecord with managed processing and bounded maintenance.
- 9927ba8: Add opt-in typed task progress and lifecycle history backed by per-generation Redis Streams. Managed handlers can emit progress through an attempt-bound context, and readers can page history using durable cursors with explicit trimming gaps. History is unlimited by default, supports an optional oldest-first count cap, and follows the task record's retention and disposal. Progress failures remain operational, including uncertain writes that are not automatically replayed.

  Upgrade every queue process before enabling progress; dispose of enabled generations through the upgraded engine before rolling back to a version without history support.

## 0.3.0-rc.1

### Patch Changes

- d485338: Make public Effect contracts honest and restructure the package around focused
  task-record and task-event modules. Task, queue, worker, and scheduler
  definitions remain pure while programmer-authored definition invariants become
  defects at their first runtime use. TaskEngine errors use semantic reason tags,
  the standard `TaskEngine.layer` is fully wired, and malformed codec/Redis inputs
  remain in typed error channels.
- c42c4be: Upgrade Effect, @effect/platform-node, and @effect/vitest to 4.0.0-rc.115,
  and raise the Effect peer dependency minimum to the same release candidate.
  Adapt schema codecs and configuration to the RC APIs, preserve scoped Redis
  subscriptions in the live and test adapters, and update the matching test
  integration to Vitest 5.
- e13bdb8: Fix queue correctness and boundary validation found during a repository-wide
  review:

  - allow `Schema.Void` tasks to persist and recover successful completion;
  - keep `wait` and `execute` subscribed across retryable failures;
  - reject queue/task descriptors that do not match a persisted handle;
  - validate numeric offer, lease, and retry-timestamp inputs before mutating
    Redis;
  - validate decoded storage values, preserve prototype-sensitive object keys,
    and reject corrupt Redis numbers and cursors in typed error channels;
  - schema-validate built-in failure events and keep their public type precise;
  - keep stalled-attempt history out of handler retry schedules, settle attempts
    when retry-policy evaluation fails, stop safely when retained history is too
    short to replay, and retain terminal failures even when error history is
    disabled;
  - preserve retry-policy interruption for lease recovery and avoid full
    wait-list scans on creation, acquisition, and non-waiting transitions; and
  - reject unsafe worker concurrency, timing, and lease supervision options
    before acquiring work or starting fibers.

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
- Require Node.js 22.19 or newer, matching the verified Effect beta platform
  stack; the ESM consumer matrix covers Node.js 22 and 24.

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
