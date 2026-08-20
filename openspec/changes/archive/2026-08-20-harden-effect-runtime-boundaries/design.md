## Context

See `proposal.md` for motivation. The node-redis adapter already centralizes most client access, but its promise conversion, readiness recovery, finalization, and reply normalization are inconsistent. Domain modules also read `Date`, `Date.now`, and `crypto.randomUUID` directly, and the inspection CLI constructs and closes Redis imperatively.

## Goals / Non-Goals

**Goals:**

- Make one module own each external API and translate its failures once.
- Preserve Effect cancellation, defects, and service substitution across every boundary.
- Make Redis resources leak-free under success, failure, and interruption.
- Validate untyped Redis data before domain construction.

**Non-Goals:**

- Replace node-redis or change supported Redis topologies.
- Treat Redis event callbacks as ordinary Effect APIs; a small foreign callback bridge remains necessary.
- Virtualize Redis server time or convert every integration timing test to `TestClock`.
- Change MessagePack bytes or the Redis key layout.

## Decisions

### 1. NodeRedisPool is the single node-redis boundary

All client promises, event callbacks, reply normalization, and connection lifecycle stay inside `NodeRedisPool`. Helpers use `Effect.tryPromise` (or async with a canceler where the API supports cancellation) and map rejections into the semantic engine/Redis errors defined by `make-effect-contracts-honest`. Domain modules never inspect node-redis error classes or messages.

Scattering `tryPromise` at call sites was rejected because it duplicates vendor translation and allows raw client values to leak into domain code.

### 2. Recovery handles only typed failures

Readiness and health operations catch only the explicit Redis failure channel. `Cause`-wide recovery is reserved for logging followed by re-failure when the policy truly applies to every cause. Interruption and defects therefore retain their original semantics.

Returning `false` from `catchCause` was rejected because it makes cancellation and programmer defects indistinguishable from an unavailable server.

### 3. Connections and listeners are acquired with Scope

Pool construction uses `Effect.acquireRelease`/`acquireUseRelease` per owned client and composes them in one layer scope. Finalizers use typed promise adapters and an explicit shutdown policy: attempt graceful close, fall back to forced destruction only for the documented close failures, aggregate diagnostics, and never leave a floating promise. Listener registrations are removed on release. The existing short `runFork` event callbacks remain a confined foreign bridge; their bodies must require no services and do bounded, immediate work. If that changes, the adapter will capture a scoped runtime or FiberSet and supervise the fibers.

A process-global `ManagedRuntime` was rejected because it weakens ownership and introduces manual disposal obligations.

### 4. Clock and randomness are read inside Effects

All timestamps use Effect `Clock` at the point of execution. UUID/default-id generation uses an Effect-owned cryptographic randomness capability rather than a default function that calls ambient `crypto`. Pure functions continue accepting explicit instants or identifiers as values. Scheduler materialization retains an explicit-time pure core, while its live wrapper reads Clock and passes the value in.

Passing `new Date()` as a default parameter was rejected because default evaluation can occur before the Effect runs. Keeping injectable callback defaults was rejected because the default still bypasses Effect services.

### 5. Redis replies pass through operation-specific decoders

Each Redis command family has a decoder that accepts `unknown` and produces a validated domain value or `InvalidRedisReply`. Stream tuple shape, nullable replies, numeric bounds, buffers, and text values are checked explicitly. Dynamic keyed collections use `Map` by default; where an object is required for an API, it is created with a null prototype and encoded immediately.

Broad casts and generic `String(value)` conversion were rejected because they silently accept protocol drift. Plain `{}` records were rejected for untrusted keys because prototype names have special behavior.

### 6. The CLI is one scoped Effect program

The inspection command reads its URL and options through Effect Config, acquires the same Redis adapter/layer as the library, runs the bounded scan, and releases through Scope. `NodeRuntime.runMain` is called only in the executable entry module. The scan loop may remain locally imperative inside one wrapped operation if that produces clearer code, but it cannot own connection lifecycle or leak untyped rejections.

Keeping `try/finally` around raw awaits was rejected because it creates a second lifecycle and error model outside the package architecture.

## Risks / Trade-offs

- [Explicit Clock/randomness requirements widen public environments] → Publish exact aliases and provide them through the standard live layer; tests can substitute deterministic services.
- [Strict reply validation rejects values previously coerced] → Include operation and a bounded representation of the received value in errors, and add compatibility fixtures for every supported RESP form.
- [Shutdown errors obscure the primary failure] → Preserve the primary cause and attach finalizer diagnostics using Effect's cause composition/logging policy.
- [Removing listeners races an in-flight callback] → Keep callback bodies bounded and make release idempotent; add interruption and reconnect lifecycle tests.

## Migration Plan

1. Land the semantic error algebra from `make-effect-contracts-honest` or introduce compatible internal placeholders.
2. Add operation-specific Redis reply decoders and replace casts from the leaves inward.
3. Convert connection acquisition, listeners, readiness, and finalization to scoped typed adapters.
4. Move time and UUID reads into Effect services and update dependent public contracts.
5. Rewrite the CLI as a scoped Effect entry point.
6. Run malformed-reply, interruption, Redis restart, Sentinel, and resource-leak tests. Rollback is release-wide because the service requirements intentionally break source compatibility.
