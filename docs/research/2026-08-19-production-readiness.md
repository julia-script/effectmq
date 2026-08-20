# effectmq production-readiness research

**Date:** 2026-08-19
**Scope:** current `main` plus the staged `lua-files-and-msgpack` change
**Sources:** repository code and first-party Redis, Effect, MessagePack, msgpackr, BullMQ, node-redis, and npm materials

## Executive verdict

`effectmq` has the beginnings of a distinctive library: Effect-native handlers, typed payload/result/error schemas, atomic Redis transitions, retries expressed as `Schedule`s, lifecycle events, and task pinning. It is worth shipping as an explicitly experimental beta after a small set of correctness fixes.

It is **not production-ready today**. The gap is not a missing feature checklist. The immediate blockers are state-machine correctness, honest delivery semantics, bounded Redis work, lease fencing, retention, upgrade safety, and fault testing.

The staged change should be split into three independent decisions:

| Decision | Recommendation |
|---|---|
| Keep Lua in real `.lua` source files | **Yes.** This is a clear maintainability improvement. Add deterministic generation and CI drift detection. |
| Centralize serialization at the Node/Redis boundary | **Yes.** This is the best part of the change. Make the wire profile lossless and versioned before choosing a permanent codec. |
| Replace the existing script runner with `FUNCTION LOAD REPLACE` + `FCALL` | **No, not as implemented.** The stated performance premise is false, and the current design adds deployment, mixed-version, ACL, and cluster hazards. Keep the existing `SCRIPT LOAD`/`EVALSHA` path for the first production release, or treat Redis Functions as separately provisioned, immutable, versioned database artifacts. |

Most importantly, remove every “exactly once” claim. This design is an **at-least-once queue**. A worker can perform an external side effect and die before acknowledging Redis; after its lease expires, another worker must run the task again. BullMQ documents the same fundamental contract and explicitly warns that a stalled job can be double-processed ([BullMQ important notes](https://docs.bullmq.io/bull/important-notes), [BullMQ concurrency](https://docs.bullmq.io/guide/workers/concurrency)). The current README instead promises “exactly once, safely” and “it runs once” ([README](../../README.md), lines 181–219).

## The Lua-loading premise needs correction

The staged proposal says the old engine sends the full Lua source with `EVAL` on every call. It does not.

The baseline `TaskEngine` calls `RedisPool.eval`, which comes from Effect’s `effect/unstable/persistence/Redis`. In the installed, pinned `effect@4.0.0-beta.85` source, `Redis.make`:

1. loads each script with `SCRIPT LOAD`,
2. caches the returned SHA,
3. calls it with `EVALSHA`, and
4. catches `NOSCRIPT`, refreshes the SHA, and retries.

The primary source is `node_modules/effect/src/unstable/persistence/Redis.ts`, lines 42–79 in this checkout (the published source is also available from the [`effect@4.0.0-beta.85` package](https://www.npmjs.com/package/effect/v/4.0.0-beta.85)). This matches Redis’s recommended application-owned script flow: the script cache is volatile across restart, failover, and `SCRIPT FLUSH`, so callers should use `EVALSHA` and reload on `NOSCRIPT` ([Redis scripting guide](https://redis.io/docs/latest/develop/interact/programmability/eval-intro/)).

Therefore Redis Functions do **not** remove a current per-call source-transfer or compile cost. The real trade-off is:

| | `SCRIPT LOAD` + `EVALSHA` | Redis Functions |
|---|---|---|
| Ownership | Application artifact | Database artifact |
| Minimum Redis | Broad compatibility | Redis 7+ |
| Restart/failover | Cache can disappear; existing Effect code reloads on `NOSCRIPT` | Persisted and replicated with data |
| Cluster deployment | Script must be available on the node that receives the call; client fallback handles a miss | Libraries must be loaded on **every cluster master**; Redis Cluster does not propagate this automatically |
| Rolling app upgrades | Each script is addressed by its content SHA | Function names are global; replacing an unversioned library changes code underneath every app version |
| Operational model | Natural for code shipped inside an npm library | Redis recommends managing Functions separately from client-library startup |

Redis describes Functions as database-managed, persisted and replicated artifacts, and explicitly says cluster administrators must load them on every master ([Redis Functions](https://redis.io/docs/latest/develop/programmability/functions-intro/)). `FUNCTION LOAD REPLACE` replaces a library with the same name, while function names must be unique across libraries ([FUNCTION LOAD](https://redis.io/docs/latest/commands/function-load/)).

The staged engine does the opposite of that intended operational model: every `TaskEngine` startup executes `FUNCTION LOAD REPLACE` for the global library name `effectmq`, and every call uses unversioned global names such as `effectmq_takeTask` ([TaskEngine.ts](../../src/TaskEngine.ts), lines 174–248; [taskEngine.lua](../../src/lua/taskEngine.lua), lines 1 and 312–603). During a rolling upgrade, whichever old or new process starts last silently selects the implementation for all processes sharing that Redis database. An ABI change in argument order, reply shape, encoding, or stored data makes that a correctness incident.

### Recommendation for the next release

Keep the `.lua` extraction, but retain a binary-safe equivalent of Effect’s current `SCRIPT LOAD`/cached `EVALSHA`/`NOSCRIPT` retry logic. The current Effect helper stringifies all arguments, so the library will need a small binary-capable script runner if MessagePack remains; preserve its cache/reload behavior rather than replacing it with startup `FUNCTION LOAD REPLACE`.

If Functions are adopted later:

- give the library and functions immutable protocol-versioned names (for example, `effectmq_v1` and `effectmq_v1_takeTask`);
- provision them in a migration/deployment command, not from every worker startup;
- verify the expected library/version at readiness time and fail closed on mismatch;
- keep old and new versions installed during rolling upgrades;
- publish a cluster-wide install command and an ACL manifest;
- test upgrade, downgrade, missing-library, failover, and mixed-version behavior.

## Redis Cluster is currently unsupported, not merely untested

Both the baseline scripts and the staged Functions declare zero keys and construct every Redis key from regular arguments. The staged wrapper sends `FCALL <name> 0 ...` ([TaskEngine.ts](../../src/TaskEngine.ts), lines 201–215), while Lua derives task, list, event, lock, and schedule keys from `ARGV` ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 28–38).

Redis requires **all** keys accessed by `FCALL` or `EVAL` to be passed explicitly as key arguments; functions/scripts should not access programmatically generated key names or names discovered from stored data ([FCALL](https://redis.io/docs/latest/commands/fcall/), [EVAL](https://redis.io/docs/latest/commands/eval/)). Redis Cluster also requires every key touched by one Lua execution to be in the same hash slot ([Redis Cluster scaling guide](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)).

Task pin release makes the incompatibility structural: `releaseRefs` recursively discovers cross-queue task keys from stored reference data ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 216–239). Those keys cannot all be declared before invocation, and separate queue prefixes naturally hash to separate slots.

For the first production release, explicitly support **standalone Redis and non-sharded HA (for example Sentinel-managed primary/replica)** only. Say “multiple worker processes” rather than “cluster” in user-facing docs. Redis Cluster support needs a separate data-model design. A single global hash tag could co-locate all effectmq keys, but it would sacrifice sharding and would still not cure the undeclared/dynamically discovered key violation.

## P0 correctness blockers

### 1. `moveToList` can strand a task

`moveToList` removes a task from its current state structure and then returns when the old state equals the target state ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 60–123):

```lua
local currentList = removeFromCurrentLists(prefix, id)
if currentList == list then
  return
end
```

The task has already been removed at the point of return.

Two concrete failures follow:

- Re-offering an already waiting idempotent task moves it to `wait`; it is removed from the wait list and never re-added.
- `extendLock` calls `lockTask`, which calls `moveToList(..., "active")` ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 174–180 and 508–522). The first heartbeat removes the task from the active set. If the worker later dies and the lock expires, `syncLocks` cannot find it, so the task is never recovered.

This must be fixed before any release, with invariant tests that assert exactly one state membership after **every** transition, including same-state transitions.

### 2. Leases need per-attempt fencing tokens

The engine config creates one `workerId` and reuses it for every task attempt ([TaskEngine.ts](../../src/TaskEngine.ts), lines 182–186). Locks store only that worker id. This creates an ABA problem: if an old attempt loses its lease and the same process later retakes the same task, the old attempt and new attempt have the same lock owner. A late completion from the old handler can satisfy the new lease’s owner check.

Use a unique lease token for every `takeTask` result (ideally a monotonic fencing value stored with the task). Require that exact token for renew, success, failure, and release. `extendLock` must return a typed `LeaseLost` error when the lock is absent; it currently returns success when no lock exists ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 508–523).

The managed worker must race/supervise the heartbeat with the handler. Today the heartbeat is a child fiber whose failure is not used to interrupt the handler ([TaskQueue.ts](../../src/TaskQueue.ts), lines 301–321). Losing Redis connectivity or the lease can therefore leave user code running without ownership.

Even after fencing, delivery remains at-least-once because fencing protects Redis state, not external side effects. Document idempotent handlers and transactional-outbox/inbox patterns.

### 3. Stalled jobs retry forever

`syncLocks` turns every expired lock into `failTask(..., retryAt = 0)` ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 284–295). `failTask` treats every non-canceled error with any non-null retry time as retryable, so a crash-looping or CPU-starved task is returned to `wait` indefinitely ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 252–279). The user’s retry schedule and `maxRetries` are calculated only in TypeScript handler failure code ([TaskQueue.ts](../../src/TaskQueue.ts), lines 210–232), so stalled failures bypass them.

Add `maxStalledCount` (or fold stalls into the same attempt budget), a terminal failure/dead-letter outcome, and a `task.stalled` signal. BullMQ caps automatic stalled recovery for exactly this reason ([BullMQ stalled jobs](https://docs.bullmq.io/guide/jobs/stalled)).

### 4. Idempotent re-offer has unsafe state semantics

When a task id already exists, `createTask` overwrites name, creation time, payload, delay, retry limit, policies, and errors, but retains fields such as lock, refs, refCount, outcome, success, and `dead`; it then moves the task to waiting or scheduled ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 323–403). Re-offering an active task can therefore place the same logical record back in the queue while its old handler and lock still exist. Re-offering a retained terminal record can create a hybrid of new inputs and old terminal state.

Define duplicate behavior by current state and make it explicit in the API:

- waiting/delayed/active: default to “return existing unchanged”;
- optional debounce/replace: allow replacement only in states where it is safe;
- completed/failed retained: return the terminal result, reject, or create a new generation—never partially reset the old record;
- active: never mutate the payload under a running handler.

BullMQ separates simple deduplication, throttle, and debounce/replace semantics rather than treating every duplicate as an update ([BullMQ deduplication](https://docs.bullmq.io/guide/jobs/deduplication)).

### 5. Atomic functions perform unbounded work

Every create, take, success, error, remove, and list read runs `syncAll`. `syncLocks` reads the entire active sorted set and checks each lock; `syncDelayed` fetches every due task; release cascades can traverse an entire descendant graph ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 216–239 and 282–310). Redis reports `ZRANGEBYSCORE` as `O(log(N)+M)`, where `M` is the number returned ([ZRANGEBYSCORE](https://redis.io/docs/latest/commands/zrangebyscore/)).

Redis Functions/scripts execute atomically by blocking all server activity, and Redis says they should finish quickly ([Redis Functions](https://redis.io/docs/latest/develop/programmability/functions-intro/), [Redis programmability](https://redis.io/docs/latest/develop/interact/programmability/)). A large backlog, mass lease expiry, or large pin cascade can therefore turn one innocent queue call into a Redis-wide latency outage.

Redesign maintenance work to be bounded:

- store lease expiry as the active-set score and sweep only expired members with `LIMIT`;
- promote delayed tasks in bounded batches;
- use a dedicated, supervised scheduler/sweeper rather than making every command scan;
- process large release cascades incrementally with durable continuation state;
- make list/getter APIs cursor-based rather than returning every id;
- put payload, result, error-list length, refs, and fan-out limits in configuration.

Publish p50/p95/p99 latency and throughput benchmarks across backlog sizes. “Atomic” is not enough; the maximum amount of work in one atomic section must be known.

### 6. Event/result retention and waiting are not safe yet

Every transition calls `XADD` with no `MAXLEN` or `MINID`, so event streams grow without bound ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 48–58). Redis supports efficient approximate capping directly in `XADD` (`MAXLEN ~ ...`) ([XADD](https://redis.io/docs/latest/commands/xadd/)). Failed/success retained sets and kept task hashes also have no TTL or trimming policy.

At the same time, `wait` depends solely on seeing a future terminal event ([TaskQueue.ts](../../src/TaskQueue.ts), lines 400–432). Its default cursor is the application machine’s `Date.now()` ([TaskQueue.ts](../../src/TaskQueue.ts), lines 336–347; [TaskEngine.ts](../../src/TaskEngine.ts), lines 360–389), while Redis generates stream ids from the Redis server’s clock. Client/server clock skew can place the cursor ahead of new events. Calling `wait` after a task already finished can wait forever, especially because the default completion policy deletes the task record.

Design result waiting and retention together:

- return an authoritative Redis stream cursor from `offer`, or store a durable terminal outcome/result record with a configurable TTL;
- have `wait` first check terminal state, then subscribe from an explicit cursor, then re-check to close the race;
- add timeout and task-not-found/result-expired errors;
- cap event history by configurable age/length and document the resulting resume window;
- consider compact events rather than embedding full old and new task snapshots on each update.

Redis documents that `XREAD` returns ids greater than the supplied id and that `$` means entries added only after the call; subsequent reads must advance from the last returned id ([XREAD](https://redis.io/docs/latest/commands/xread/)). A correctness-sensitive wait API should expose that cursor model rather than synthesize ids from a different clock.

### 7. The scheduler is at-most-once, not exactly-once

`Scheduler` atomically advances the next tick and then invokes the user handler ([Scheduler.ts](../../src/Scheduler.ts), lines 40–60). If the process dies after `consumeSchedule` and before or during the handler, that tick is lost. The TSDoc and README claim the handler runs “exactly once per tick” ([Scheduler.ts](../../src/Scheduler.ts), lines 14–33; [README](../../README.md), lines 225–239).

Either document it as an at-most-once trigger, or implement schedules by atomically enqueuing an idempotent task keyed by schedule name + tick. The queue’s lease/retry machinery can then provide at-least-once execution without duplicate schedule records.

## Serialization: keep the boundary, reject lossy data

The staged refactor correctly recognizes that serialization belongs in one explicit boundary. `UnknownFromMsgpack` and the raw RESP-entry decoding are much easier to reason about than scattered `JSON.stringify`/`cjson` calls ([Schemas.ts](../../src/Schemas.ts), lines 8–27 and 220–302).

MessagePack itself has distinct nil, boolean, integer, float, string, binary, array, map, and extension types ([MessagePack specification](https://github.com/msgpack/msgpack/blob/master/spec.md)). `msgpackr` records are a non-standard extension, so `{ useRecords: false }` is the correct interoperability setting; its `int64AsType` option controls whether 64-bit integers become bigint, number, string, or automatic values ([msgpackr README](https://github.com/kriszyp/msgpackr)).

The current implementation is still not a production wire contract:

1. **User error values are knowingly lossy.** Lua unpacks an error so it can inspect `_tag`, appends it to a Lua table, and repacks the whole error list ([taskEngine.lua](../../src/lua/taskEngine.lua), lines 157–170 and 252–269). The staged changeset acknowledges that nested `null` fields are dropped ([changeset](../../.changeset/lua-files-and-msgpack.md)). A typed queue must not silently mutate typed error data.
2. **Malformed list data is silently erased.** `msgpackListFromBytes` converts every decoded non-array—not only the empty-map ambiguity—to `[]` ([Schemas.ts](../../src/Schemas.ts), lines 250–263). Corruption should be a schema error, not an empty history/ref list.
3. **There is no version marker.** The changeset requires users to drain or flush all queues. That is acceptable for an experiment, not a world-class rolling-upgrade story.
4. **The supported value profile is unspecified.** `Schema.Unknown` lets codec-specific values/extensions reach `msgpackr`. Payloads happen to stay opaque in Lua, while errors do not, so the same apparent type can have different compatibility depending on which field carries it.

Recommended boundary:

- define a `QueueCodec` service and a documented, JSON-like canonical value profile (or explicitly document every supported MessagePack extension);
- add an envelope containing protocol version, task-schema id/version, and encoded bytes;
- store payload, success, and user error bytes opaquely end to end;
- pass retry disposition/cancellation as separate trusted scalar metadata so Lua never needs to decode a user error;
- store error entries as independent opaque records (for example a Redis list of already-packed envelopes) or pass an already-packed complete entry from Node;
- validate sizes and reject unsupported/unsafe values at enqueue/complete time;
- create golden cross-runtime fixtures covering nulls, empty arrays/maps, unicode, binary, large/safe integers, dates if supported, custom tagged errors, corruption, and old/new codec versions;
- benchmark JSON and MessagePack on realistic payload distributions before asserting a performance benefit.

The codec abstraction and lossless edges matter more than whether the first codec is JSON or MessagePack.

## Production Redis and client contract

A queue is primary data, not a disposable cache. The deployment guide must specify:

- `maxmemory-policy noeviction`; Redis otherwise may evict queue keys, while `noeviction` rejects new writes instead ([Redis eviction](https://redis.io/docs/latest/develop/reference/eviction/));
- persistence choices and expected data-loss windows: RDB is point-in-time, while AOF can fsync every write, every second, or never; Redis recommends understanding the durability/performance trade-off and backups ([Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/));
- replication is asynchronous by default. `WAIT` improves the number of acknowledged copies but does not make Redis strongly consistent or guarantee acknowledged writes survive every failover ([Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/));
- standalone versus Sentinel support, TLS, ACL key/command permissions, connection/command timeouts, reconnection, and shutdown behavior;
- what happens to an indeterminate write: if the socket drops after Redis committed but before the reply arrived, the producer must retry with the same idempotency key.

`NodeRedisPool` currently wraps a standalone `createClientPool`, exposes raw client options, and maps every failure to one generic Redis error ([NodeRedisPool.ts](../../src/NodeRedisPool.ts)). It does not attach an `error` listener. node-redis says a client **must** have an `error` listener or an emitted connection error can terminate the process; it also documents automatic reconnection, configurable backoff, TLS, connection events, Cluster, and Sentinel-specific clients ([node-redis repository](https://github.com/redis/node-redis), [node-redis connection guide](https://redis.io/docs/latest/develop/clients/nodejs/connect/), [node-redis Sentinel guide](https://github.com/redis/node-redis/blob/master/docs/sentinel.md)).

Provide separate producer and worker connection policies. Producers usually need bounded command latency and a clear indeterminate-write error; workers and blocking stream reads need long-lived supervised connections and can wait through outages. BullMQ’s first-party connection guide makes the same distinction ([BullMQ connections](https://docs.bullmq.io/guide/connections)).

## What “world-class” should mean for this library

The differentiator should not be “every BullMQ feature, but Effect.” It should be a small, deep queue with unusually strong semantics:

- every public operation has typed domain errors and an explicit interruption/retry contract;
- every state transition is atomic, bounded, observable, and model-tested;
- handlers are supervised Effect fibers with graceful drain and lease-loss interruption;
- schemas and codec versions make rolling deploys safe;
- the delivery guarantee is honest and teaches users how to make effects idempotent;
- Redis requirements and failure modes are operationally explicit.

After that foundation, the highest-value product features are:

1. a first-class `Worker` constructor with concurrency, graceful shutdown/drain, handler timeout, heartbeat supervision, and backpressure;
2. pause/resume, retry, cancel, promote, and drain operations with typed outcomes;
3. dead-letter handling, configurable task/result/event retention, and paginated inspection;
4. priority, bulk offer, progress, global concurrency, and global/per-key rate limiting;
5. OpenTelemetry spans and metrics for offer-to-start latency, run duration, successes/failures/retries/stalls, queue depth by state, delayed age, lease-renew failures, Redis latency/errors, and sweep duration;
6. an admin/diagnostic API before an admin UI.

BullMQ’s current first-party surface is a useful parity reference—not a mandate—including local/distributed concurrency, global rate limiting, deduplication modes, stalled-job limits, and OpenTelemetry metrics ([concurrency](https://docs.bullmq.io/guide/workers/concurrency), [rate limiting](https://docs.bullmq.io/guide/rate-limiting), [deduplication](https://docs.bullmq.io/guide/jobs/deduplication), [metrics](https://docs.bullmq.io/guide/telemetry/metrics)).

## Effect patterns review: `src/`

### Summary

The package is genuinely Effect-first rather than an imperative queue with an Effect wrapper: Redis is behind a service boundary, the engine is a `Context.Service`, implementations are Layers, handler errors stay typed, and most reusable operations use `Effect.fnUntraced`. The most important Effect-level gaps are ambient clocks in reusable logic, unsafe decoding casts at the Redis boundary, unsupervised resource/heartbeat failures, and a test harness that escapes every Effect through a global `ManagedRuntime`.

### Findings

#### 1. Ambient time bypasses Effect's `Clock` — C — boundary

`src/Scheduler.ts:58`, `src/TaskQueue.ts:229`, `src/TaskQueue.ts:343`, `src/TaskEngine.ts:361`

Retry calculation, scheduler sleep calculation, and stream cursors use `Date.now()`/`new Date()` directly. This makes deterministic fault tests harder and, for stream cursors, mixes the application clock with Redis-generated ids. Use `Clock.currentTimeMillis` for application timing; redesign stream cursors around Redis state rather than substituting a different clock.

#### 2. Lease heartbeat failure is not part of handler supervision — C/G — structural

`src/TaskQueue.ts:301-321`

The heartbeat is forked as a child, but its failure neither interrupts the handler nor changes the result. Model task ownership as a scoped resource: acquisition returns a unique lease, a supervised renewal fiber races the handler, loss of ownership interrupts work with a typed `LeaseLost`, and finalization deliberately releases or abandons the lease according to exit cause.

#### 3. Redis decoding trusts casts and mutable normal objects — E — boundary

`src/Schemas.ts:225-263`, `src/TaskEngine.ts:143-155`, `src/TaskEngine.ts:368-430`

Unknown reply values are cast to `Uint8Array`/`object`, while runtime-controlled field names are written into ordinary `{}` records. The MessagePack list decoder also turns every non-array into an empty list. Validate reply shapes before transformation, fail corruption in the typed channel, and fold dynamic keys into a `Map` or null-prototype dictionary.

#### 4. Cleanup failure is promoted to an accidental defect — C — boundary

`src/NodeRedisPool.ts:31-40`

Connection errors are wrapped with `tryPromise`, but `client.close()` runs in `Effect.promise`, so a rejected close is an unchecked defect. Keep acquisition and release in one scoped boundary and choose an explicit cleanup policy (`tryPromise` plus observation/ignore, or deliberate promotion) rather than getting one accidentally.

#### 5. Tests discard Effect's native test services — H — hygiene

`src/testing/redisLayer.ts:109-118` and all six integration suites

The suites use ordinary Vitest tests plus a module-global `ManagedRuntime` and `runPromise`. Migrate to `@effect/vitest`, suite-scoped Layers, `it.effect`, `TestClock`, and Effect assertions. Keep a small number of ordinary async black-box consumer tests for the published package, rather than using the foreign-runtime bridge as the unit/integration test runner.

### What's done well

- Modules mostly orbit coherent concepts (`Task`, `TaskQueue`, `TaskEngine`, `Scheduler`, `RedisPool`) and the public package exposes stable subpaths.
- External Redis promises are confined to `NodeRedisPool`; queue orchestration stays in the typed Effect channel.
- `TaskEngine` is a capability-bearing service rather than a method-heavy domain entity, and concrete wiring is exposed as Layers.
- Public domain errors are yieldable tagged errors, and handler failures remain schema-typed.
- Resource lifetime for the Redis client pool is tied to Scope rather than a caller-owned `dispose()` method.

### Conventions doc

`CLAUDE.md` is present and covers several Effect conventions, public exports, TSDoc, and release commands, but it does not cover the full boundary/error/resource/test rules used in this review. It is also stale about inline Lua and the `send`/`eval` pool surface. Update it only after the Lua runner and production contracts are decided, so it records policy rather than the current experiment.

## Verification program

### State-model and property tests

Build a small reference state machine in TypeScript and compare random command sequences against real Redis. Assert after every step:

- a runnable task is in exactly one of waiting, delayed, or active;
- terminal/dead tasks cannot become runnable without an explicit new generation;
- active implies a live matching lease token;
- a lost/old lease can never mutate task state;
- ref counts never go negative and equal live incoming pins;
- death/release is idempotent;
- duplicate offer behavior matches its state-specific contract;
- event ordering and payloads agree with state changes.

### Fault and compatibility matrix

Automate real Redis tests for:

- process death before handler, during handler, after side effect, and after Redis completion;
- heartbeat delay, event-loop starvation, lost lease, and same-process retake;
- Redis restart, `SCRIPT FLUSH`, primary failover, transient disconnect, timeout after commit, and reconnect;
- disk-full/write-denied, OOM/noeviction, malformed stored bytes, and partial old-version data;
- mixed old/new workers during rolling upgrade and rollback;
- server/client clock skew;
- thousands of due delayed tasks, expired leases, retained results, events, refs, and a wide/deep release cascade;
- every claimed Redis, Node, Effect, RESP, and client version.

The current tests cover happy paths and several lock/pinning cases, but there is no benchmark, property/fuzz suite, failover/restart test, mixed-version test, or Redis Cluster/Sentinel test in the repository. The same-state membership bug shows why transition invariants are more valuable than adding more isolated examples.

### Release gates

Before calling a release production-ready:

- all P0 cases above have regression tests;
- fault tests pass repeatedly without leaked tasks or invalid transitions;
- latency budgets hold at realistic and adversarial backlog sizes;
- a published compatibility matrix passes in CI;
- packed tarball smoke tests verify every export and a consumer app;
- the upgrade/rollback test uses two actual package versions and preserved Redis data;
- README claims match the proven delivery and scheduler semantics;
- operations docs cover persistence, HA, eviction, ACLs, TLS, sizing, retention, alerts, backup/restore, and disaster recovery.

## Release engineering and project trust

The package manifest has empty keywords and author fields and no repository, homepage, bugs, engine, or publish configuration ([package.json](../../package.json)). Add the metadata users and tooling need, a supported Node/Redis/Effect matrix, `CONTRIBUTING.md`, `SECURITY.md`, issue templates, and an explicit pre-1.0 compatibility policy. npm documents these package fields and the encapsulation provided by `exports` ([npm package.json reference](https://docs.npmjs.com/files/package.json/)).

The release workflow can publish on a push to `main` independently of the CI workflow and uses a long-lived `NPM_TOKEN` ([release workflow](../../.github/workflows/release.yml), [CI workflow](../../.github/workflows/ci.yml)). Make publishing depend on a successful full verification job and move to npm trusted publishing/OIDC with provenance. npm recommends trusted publishing because it removes long-lived publish tokens and automatically generates provenance for eligible public packages ([npm trusted publishers](https://docs.npmjs.com/trusted-publishers/)).

The Lua generator currently rewrites a committed `.ts` file during build/test ([gen-lua.ts](../../scripts/gen-lua.ts)). Make output deterministic and add a CI step that runs generation and fails on `git diff --exit-code`; otherwise a stale committed artifact can pass the early typecheck and be silently regenerated later.

The packed artifact also needs tightening. A local `npm pack --dry-run --json` includes `dist/scratchpad/demo.*`, so experimental code is currently shipped, and the generated Lua module produces an approximately 22 KB declaration file containing the entire script as a string-literal type. Exclude `src/scratchpad` from production compilation and emit the generated source through a value explicitly typed as `string` so its declaration stays small. `publint` reports the ESM package structure itself as valid; `@arethetypeswrong/cli` resolves the package for ESM and bundler consumers but confirms that CommonJS `require` is unsupported, which should be stated explicitly.

The documented lint commands also disagree: `package.json` defines `pnpm lint` through `turbo`, which is not installed, while CI directly runs Biome. Keep one canonical release command and make `prepublishOnly`/the release job run generation drift check, typecheck, lint, tests, build, packed-artifact inspection, and a consumer smoke test.

The project is also pinned to an Effect 4 beta while its peer range begins at that beta and is open-ended ([package.json](../../package.json), lines 55–73). Because the code imports unstable APIs, test a narrow declared range (including the intended Effect 4 RC/stable target) rather than implying all future Effect versions are compatible.

## Prioritized ship plan

### Milestone 0 — honest experimental beta

- Fix `moveToList` same-state loss.
- Define at-least-once semantics and remove exactly-once wording.
- Add unique per-attempt lease tokens; make lost heartbeat/lease interrupt the handler.
- Bound stalled recovery and add terminal/dead-letter behavior.
- Make duplicate offer state-aware and non-mutating by default.
- Preserve user error bytes losslessly; add codec version/profile and golden fixtures.
- Keep `.lua` sources but retain binary-safe `EVALSHA` + `NOSCRIPT` recovery.
- Add event/result retention and a race-safe `wait` protocol.
- Correct scheduler semantics or schedule idempotent queue tasks.

### Milestone 1 — operational beta

- Replace unbounded `syncAll` with bounded sweepers and paginate getters.
- Add retention/size limits, graceful worker drain, typed operational errors, health/readiness, metrics, and tracing.
- Document standalone/Sentinel production deployment, persistence, noeviction, ACL/TLS, backup, restore, and indeterminate writes.
- Add crash/restart/failover/backlog/clock-skew/mixed-version tests and published benchmarks.

### Milestone 2 — stable 1.0

- Freeze the task state machine, Redis key schema, codec envelope, and compatibility policy.
- Prove rolling upgrade/rollback across supported versions.
- Ship secure, gated, provenance-bearing releases and consumer tarball tests.
- Add the highest-value worker/admin features based on real beta users, not parity pressure.

## Local verification note

The generated Lua module was deterministic in this checkout (`pnpm gen:lua` followed by `git diff --exit-code -- src/lua/taskEngine.ts`). `pnpm exec tsc --noEmit` and `pnpm build` passed. With the repository's local-Redis fallback, all 57 tests in the six engine, locking, pinning, queue, event, and scheduler files passed. The two separate node-redis integration tests could not be independently completed because the local Docker daemon returned `EOF`; both timed out waiting for their containers, after which the run was stopped. This is an environment failure, not evidence of a product-code failure, but it means 57 of 59 tests were independently verified in this run.

The exact CI lint command, `pnpm exec biome check src/`, does **not** currently pass: it reports two errors and two warnings in `src/scratchpad/demo.ts`. This also contradicts the staged OpenSpec claim that lint is clean. `pnpm audit --prod --audit-level=moderate` reported no known production-dependency vulnerabilities, and `publint` passed.
