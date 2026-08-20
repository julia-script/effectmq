## Context

`effectmq` is a pre-1.0 Effect-native Redis queue with a compact TypeScript API and an atomic Lua state engine. That is a promising base, but the current implementation mixes several independent ideas: execution state, task-record lifetime, creator provenance, and implicit "child" references. It also identifies lease ownership by worker rather than attempt, performs some unbounded maintenance, has ambiguous duplicate-offer behavior, and describes scheduler and handler execution more strongly than the implementation can guarantee.

The in-progress Redis Functions/MessagePack work contains useful serialization-boundary improvements, but it also replaces Effect's existing cached `SCRIPT LOAD`/`EVALSHA`/`NOSCRIPT` path with `FUNCTION LOAD REPLACE` during startup. That creates a rolling-version race and a larger Redis deployment contract without providing the assumed per-call script-transfer improvement. The production release therefore treats script execution and value encoding as independent decisions.

This design intentionally freezes a narrow first production contract: at-least-once execution on standalone Redis or Sentinel-managed non-sharded Redis, with explicit operational and compatibility limits. It prioritizes state-machine safety, bounded work, recoverability, and honest user-facing semantics over feature breadth.

## Goals / Non-Goals

**Goals:**

- Make every task transition safe under replay, worker crash, lease expiry, duplicate delivery, Redis restart, and rolling application upgrades.
- Give each execution attempt fenced ownership and stop managed handler work promptly when ownership is lost.
- Separate execution state, terminal-result retention, creator provenance, and execution dependency in both the model and API.
- Preserve typed payloads, successes, and failures losslessly behind a versioned storage boundary.
- Bound Redis-side work and retained data so backlog spikes cannot turn one atomic operation into an outage.
- Define race-safe result waiting, durable scheduling, explicit topology support, and actionable operational errors.
- Make release readiness an evidence-based gate covering state-model tests, fault tests, compatibility, packaging, documentation, and publication provenance.

**Non-Goals:**

- Exactly-once handler execution or exactly-once external side effects.
- Redis Cluster support in the first production release.
- Parent/child joining, cancellation propagation, failure propagation, DAGs, workflows, batches, or sagas.
- Priorities, rate limiting, a dashboard, multi-region active/active operation, or a general orchestration engine.
- Supporting arbitrary JavaScript values outside the documented codec domain.
- Preserving wire compatibility with unpublished pre-production data at the expense of a coherent v1 protocol.

## Decisions

### 1. Model execution state independently from record retention

Each task generation has exactly one execution state: `delayed`, `waiting`, `leased`, `retry-scheduled`, `succeeded`, or `failed`. A terminal outcome is settlement; deletion of its Redis record is disposal. An explicit retention hold can postpone disposal, but cannot hide settlement, return a task to a runnable state, or imply that another task must wait for it.

All state changes pass through one transition primitive that removes membership from the prior state only when the target differs, establishes the target membership, then validates the stored state. A transition to the current state is a lossless no-op or a repair that restores the one required membership; it never removes the task from its current index. This directly eliminates the same-state `moveToList` stranding behavior.

The terms `alive`, `done`, `dead`, and `pin` are removed from the public model because they collapse execution and storage lifetime into one ambiguous lifecycle.

### 2. Identify a task by queue, id, and generation

The stable identity of one execution lifecycle is `(queue, taskId, generation)`. `TaskQueue.offer` returns a `TaskHandle` containing that identity plus the authoritative event cursor needed for waiting. User-supplied task ids remain idempotency identities; generation distinguishes an intentional new lifecycle from a replay of the same offer.

Offering an existing id is unchanged-by-default in every state. The result explicitly says whether the generation was created or already existed. Replacing a runnable or leased generation in place is not supported. A caller that intentionally wants to run the logical id again requests a new generation, which is accepted only under the documented terminal-generation rules. No payload, retry history, lease, relationship, or outcome fields leak into the new generation.

This favors a predictable idempotency contract over an `upsert` API whose meaning changes with state.

### 3. Fence ownership per attempt

Acquisition creates a cryptographically strong opaque lease token for that attempt and stores a lease expiry in a score-ordered active index. Renewal, success, typed failure, and voluntary release compare the exact token atomically. A token from any previous attempt receives `LeaseLost` and cannot mutate state, history, or events.

The low-level acquisition result is a separate `TaskAttempt { task, leaseToken }` value. Durable task data never contains the ownership credential. Managed handlers receive only `task`; the worker scope retains the `TaskAttempt` and must present its token for heartbeat, success, failure, or release. This keeps attempt authority out of ordinary handler code and makes accidental acknowledgement with a task id alone impossible.

Attempt number, handler-failure count, and stalled-attempt count are separate fields. Handler failures use the task's typed retry schedule and `maxRetries`; lease expiry and ownership loss use the built-in `maxStalledCount`. Exhausting the latter settles the task with a built-in stalled failure rather than retrying forever or passing an infrastructure condition into the user's error schedule.

Redis server time is used for lease deadlines and due-time comparisons so hosts with skewed clocks cannot disagree about ownership.

### 4. Supervise the handler and heartbeat as one scoped operation

The managed worker acquires a task into a scoped attempt, races the handler with the supervised heartbeat, and acknowledges only inside that scope. A definitive missing-token response interrupts the handler with `LeaseLost`. Redis transport failures are retried only within a bounded heartbeat policy and remaining safety margin; when ownership can no longer be established before expiry, the worker treats the lease as lost and interrupts local work.

Interruption is cooperative and cannot undo an external side effect already performed. The public guarantee therefore remains at-least-once, and documentation requires idempotency keys or an inbox/outbox for non-repeatable effects.

Shutdown stops acquisition, allows a configurable drain period, releases or lets expire unfinished leases, and closes Redis resources through Effect scopes. Producer, blocking worker, and maintenance use separate connections/pools so blocking reads and reconnection cannot starve acknowledgements or heartbeats.

### 5. Separate creator provenance, result retention, and execution dependency

When a managed handler offers another task, the new task records a `creator` reference to the running task generation. This is immutable informational provenance only.

Result retention is affirmative. `TaskQueue.offer` accepts `retainResultUntil: "current-task-settles"` only when called from a managed task context; lower-level administrative APIs can name an explicit live holder generation. The relationship is a set keyed by `(holder generation, retained generation)`, so replay is idempotent and two holders are independent. Ordinary nested offers acquire no hold. Holder settlement or removal releases owned holds in bounded batches; a held terminal record is visibly settled but cannot be disposed until its final hold releases or an explicit administrative force-removal policy applies.

No current relationship delays settlement, joins execution, propagates cancellation, or propagates failure. The public term `child task` is reserved for a future structured-lifecycle contract that would actually provide those semantics. Today the precise terms are creator, spawned task, and retention hold.

### 6. Make result handles and waiting replay-safe

`TaskHandle<Success, Failure>` is the only input needed to await one task generation. It carries queue identity, task id, generation, protocol/schema identity, and the Redis event cursor returned atomically with offer. It does not contain the result itself.

Waiting uses a three-part protocol: read the durable generation state, subscribe after the handle cursor, then re-read the state once the subscription is established. The same generation check applies at every step. This closes the completion-before-subscribe race while still allowing immediate resolution for already-settled retained results.

The terminal outcomes are distinct typed cases: decoded success, decoded task failure, `TaskNotFound`, `ResultExpired`, `CursorExpired`, schema/protocol incompatibility, and caller timeout. Event retention is an observability/replay window, not the sole source of completion truth.

### 7. Use versioned `.lua` scripts through a binary-safe EVALSHA boundary

Lua remains in real `.lua` source files so it is reviewable, testable, syntax-checkable, and packaged deliberately. A script runner derives the SHA from exact content, loads it through `SCRIPT LOAD`, invokes it with binary-safe `EVALSHA`, and on `NOSCRIPT` reloads that same content and retries once. Each package version addresses its own script content, so rolling processes do not replace one global Redis Function library.

The runner owns conversion of Redis keys, binary arguments, and binary replies; queue modules never call raw `eval` or stringify structured user data. Redis Functions are not used for this release. This choice preserves the already-proven cache/recovery behavior while allowing the serialization improvements to proceed independently.

### 8. Introduce a versioned, lossless storage envelope

The Redis engine operates only on protocol metadata and opaque byte strings for user payloads, successes, and typed failures. The TypeScript boundary encodes each value in an envelope containing storage protocol version and queue schema identity. Built-in control errors such as lease loss and stalled exhaustion use separate protocol tags and never masquerade as user failures.

The default codec has a documented value domain and canonical representation, including nested nulls, Unicode, binary values, and safe numeric values. MessagePack may be the default codec if golden fixtures establish cross-version stability, but the protocol does not depend on Redis Functions and does not silently coerce unsupported values. Decode failures, wrong shapes, unsupported versions, and schema mismatches remain typed corruption/compatibility failures.

Each release declares readable and writable protocol versions. Compatible rolling releases use a mutually readable write version. A release that cannot do so requires an offline migration before mixed versions start. New protocols are tested by golden fixtures and old-reader/new-writer matrices before they are declared rollable.

### 9. Bound all atomic and maintenance work

Lease recovery and delayed promotion use score-ordered indexes queried by due time with a configurable `LIMIT`. Retention trimming, relationship release, terminal cleanup, and dead-letter maintenance process a bounded number of records per script and return a continuation. Continuations remain durably discoverable so a worker crash does not abandon cleanup.

List/read APIs are cursor-paginated and capped; an unbounded `getAll` is not part of the stable production API. Event streams use approximate `MAXLEN`/age retention with a discoverable earliest cursor. Task records, results, events, error histories, relationships, payloads, and outcome bytes all have count or size limits enforced before unbounded work occurs.

Maintenance lag, oldest task age, due backlog, expired-lease backlog, event trim position, retention failures, script reloads, Redis reconnects, and ownership loss are observable through Effect metrics and structured logs.

### 10. Materialize schedules as ordinary durable tasks

A schedule definition has a stable name, cron/calendar rule, target queue, a payload constructor, timezone, and an explicit missed-tick policy: skip, coalesce, or bounded backfill. The scheduler derives a task id from schedule name plus nominal tick timestamp and offers it idempotently. It records no separate claim that a handler ran.

Execution then uses the normal queue lease, retry, result, and observability semantics. Multiple scheduler processes may race safely to materialize a tick; the deterministic offer identity produces one generation for that nominal tick, while the handler remains at-least-once.

### 11. Support standalone Redis and Sentinel, not Redis Cluster

The first production release supports one writable Redis keyspace, directly or through Sentinel failover. Startup validates topology and refuses Cluster configuration. This is explicit because atomic scripts currently span queue, task, relationship, and event keys that do not form a proven cluster-slot contract.

The Node Redis adapter installs error listeners before use, reports connection/readiness health, supports TLS and ACL configuration without logging secrets, and classifies connection loss after a write as an indeterminate outcome. Offer retries use the same `(queue, taskId, generation request)` identity so the caller can safely learn whether the first write committed.

The operations guide requires a deliberate persistence policy, `noeviction`, backup/restore testing, documented replication data-loss windows, timeouts, reconnection backoff, graceful shutdown, and least-privilege Redis ACLs.

### 12. Gate the production claim on evidence

The state machine has a small executable reference model. Property tests generate offers, duplicate offers, transitions, renewals, acknowledgements, expiry sweeps, settlements, holds, releases, and removals, comparing Redis state with the model and asserting one-state/one-owner invariants after every operation. Fault tests kill workers at transition boundaries, pause heartbeats, flush the script cache, restart Redis, trigger Sentinel failover, and mix compatible client versions.

Benchmarks publish throughput and p50/p95/p99 latency across payload sizes, concurrency, backlog, and maximum maintenance batches; they include event and retention pressure rather than only a warm happy path. CI builds and packs the exact tarball, checks its file list and generated declarations, installs it in representative ESM consumers, and exercises every public export.

Publication uses a protected release environment, npm trusted publishing, provenance, immutable tags, and a gate tied to the exact commit. A production-ready label is not applied until the compatibility matrix, fault suite, documentation, security policy, and upgrade/rollback runbook are all present.

Immediately before cutting the candidate, the release resolves npm's `beta`
dist-tag for `effect` and each direct `@effect/*` dependency, records the exact
versions in the manifest and compatibility evidence, and reruns every gate.
This is a deliberate beta refresh, not an automatic move to an `rc` tag.

## Risks / Trade-offs

- **Breaking pre-1.0 API and storage changes**: existing users must update offer/wait calls and may need to drain old queues. This is cheaper and safer before promising stability than preserving the ambiguous pinning model.
- **At-least-once is a weaker marketing phrase**: it is the honest distributed-systems guarantee. Examples and docs must teach idempotent side effects so users understand the practical contract.
- **Per-attempt tokens and generations add metadata**: the storage and API become slightly larger, but stale acknowledgements become rejectable and replay becomes well-defined.
- **Explicit retention adds a choice**: callers that truly need a result beyond normal retention must opt in. The default becomes simpler and avoids implicit workflow semantics and unbounded lifecycle graphs.
- **Finite retention can expire awaited results**: callers receive `ResultExpired` rather than hanging or decoding absence as success. Capacity planning and retention metrics make the trade-off visible.
- **Bounded sweepers are eventually complete**: cleanup may take multiple passes under heavy backlog. Continuations and lag metrics trade immediate global cleanup for predictable Redis latency.
- **No Redis Cluster narrows deployment options**: documenting the unsupported topology is safer than relying on accidental hash-slot behavior. Cluster support can be designed later around a deliberately partitioned key model.
- **Offline migration may be required from pre-release data**: a v1 namespace avoids silent corruption. A drain/export path is preferable to an under-specified dual reader for unpublished formats.

## Migration Plan

1. Freeze feature work and split the current serialization changes from Redis Functions. Preserve `.lua` sources and binary serialization tests; remove `FUNCTION LOAD REPLACE`/`FCALL` startup coupling in favor of the content-addressed EVALSHA runner.
2. Add a regression test for same-state transition stranding, then repair the transition primitive before building new semantics on it. Establish the executable reference model and invariant harness.
3. Introduce `TaskHandle`, generation identity, explicit offer outcomes, per-attempt lease tokens, `LeaseLost`, stalled limits, and the supervised worker scope behind a new storage namespace.
4. Replace implicit `heldBy`/`detached` behavior with creator provenance and explicit `retainResultUntil`. Add bounded relationship-release maintenance and migrate call sites at compile time.
5. Introduce the versioned storage envelope, golden fixtures, size limits, typed corruption/compatibility errors, and the race-safe wait protocol. Write only the new protocol in the new namespace.
6. Convert delayed/lease recovery, retention, event trimming, queries, and cleanup to bounded operations with continuations and metrics. Convert scheduler callbacks into deterministic queued tick tasks.
7. Run old/new mixed-client tests only for versions explicitly declared wire-compatible. For existing pre-production queues, provide a drain/export inspection command and require quiescing old workers before moving to the new namespace; do not silently reinterpret old records.
8. Ship a release candidate and run the full compatibility, fault, failover, benchmark, documentation, and packed-consumer gates. Upgrade guidance starts producers, then workers, then schedulers only within a declared compatibility window.
9. Rollback before new-protocol writes is a package rollback. After new-protocol writes begin, rollback is only to a reader declared compatible with that protocol; otherwise restore the pre-migration backup or drain the new namespace. The runbook states this boundary explicitly.

No unresolved correctness or compatibility decision is deferred past the release candidate. Redis Cluster, structured child-task semantics, and higher-level workflow features are separately scoped future designs.
