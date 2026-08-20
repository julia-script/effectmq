# API reference

The package is ESM and exposes the root module plus stable module subpaths.
Public TypeScript declarations in the packed tarball are authoritative; this
page describes the intended entry points and their contracts.

## `Task`

- `Task.make(config)` synchronously defines payload, success, and typed-failure
  schemas, stable `schemaId`, idempotency key, retry schedule/cap, storage
  limits, and retention. It does not evaluate an Effect or validate runtime
  invariants.
- `defaultRetentionPolicy` is 7 days for task records and terminal indexes,
  1 day for results, 30 days for dead-letter entries, and 7 days for events.
- Queue and worker operations check definition invariants at first use. Invalid
  programmer-authored task configuration is a defect, not a typed failure.

## `TaskQueue`

- `make(name, definition)` binds a task definition to a queue name.
- `offer(queue, payload, options?)` returns `TaskCreated | TaskExisting`; both
  include the decoded task and a generation-specific `TaskHandle`.
- `wait(queue, handle, options?)` resolves the exact generation's typed success
  or fails with a typed task/storage/cursor/timeout outcome.
- `execute(queue, payload, options?)` is offer followed by handle-based wait.
- `completeOne(queue, handler, processing?)` acquires and supervises at most one
  attempt, returning whether work was processed.
- `complete(queue, handler)` processes one task and returns its id.
- `stream(queue, options?)` decodes versioned lifecycle events from a cursor.

Important offer options include `taskId`, `delay`, `maxRetries`, completion
policies, `onDuplicate`, and
`retainResultUntil: "current-task-settles"`. Numeric overrides are validated
before Redis is mutated. Processing options configure lease duration,
heartbeat interval, and bounded heartbeat transport retry.

## `Worker`

- `make(queue, handler, options?)` describes a managed worker.
- `run(worker)` runs scoped acquisition slots plus maintenance until
  interrupted. Options include concurrency, poll/maintenance intervals, drain
  timeout, and processing supervision. Invalid concurrency, durations, or
  processing settings fail with `WorkerConfigurationError` before any fibers
  start.

## `Scheduler`

- `make(config)` synchronously creates a long-running durable materializer
  Effect descriptor. Invalid definition configuration is a defect when the
  scheduler first runs.
- `materializeDue(config, now?)` performs one deterministic bounded observation,
  useful for tests and externally driven scheduler loops.
- Missed policy is `skip`, `coalesce`, or bounded `backfill`.

## `NodeRedisPool` and `RedisPool`

- `NodeRedisPool.layer(config?)` provides independent producer, worker, and
  maintenance services plus Effect's Redis service and
  `RedisConnectionHealth`. Standalone and Sentinel are supported; Cluster
  fails with `UnsupportedRedisTopology`.
- `RedisConnectionHealth.snapshot` is passive and secret-free;
  `readiness` actively pings every role.
- `RedisPool` is the minimal custom-client boundary: text/binary `send` and
  content-addressed `evalScript` with `NOSCRIPT` recovery.

## `TaskEngine`

`TaskEngine` is the lower-level storage protocol. Its public operations include
offer/get/result, fenced acquire/renew/succeed/fail/release, bounded maintenance,
paginated state inspection, event cursors/streaming, schedule cursor
coordination, ordinary removal, and administrative force removal. Prefer
`TaskQueue`, `Worker`, and `Scheduler` unless building tooling or an alternate
runtime.

- `TaskEngine.layer(config?)` is the zero-requirement Node live graph and retains
  Redis operational services in its output.
- `TaskEngine.layerNoDeps(config?)` requires an ambient `RedisPool` for custom
  client compositions.
- Invalid configuration and Redis reply shapes use structured typed errors;
  diagnostic strings are retained only as causes.

## `TaskRecord` and `TaskEvent`

`TaskRecord` owns public durable task identity/state schemas and typed record
codecs. `TaskEvent` owns public versioned lifecycle event schemas. MessagePack,
raw engine-record, and retry-schedule modules are internal and unsupported as
package subpaths.

## `StorageProtocol` and `Observability`

`StorageProtocol` owns the versioned opaque-value codec and typed corruption,
version, schema, value, size, and count errors. `Observability` exports Effect
metrics for depth/age/backlogs, Redis errors/reconnects/script reloads,
ownership loss, and retention failure.

Stable subpaths are `./NodeRedisPool`, `./Observability`, `./RedisPool`,
`./Scheduler`, `./StorageProtocol`, `./Task`, `./TaskEngine`, `./TaskEvent`,
`./TaskQueue`, `./TaskRecord`, and `./Worker`.
