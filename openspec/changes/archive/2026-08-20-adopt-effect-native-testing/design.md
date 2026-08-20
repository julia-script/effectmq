## Context

See `proposal.md` for motivation. Vitest currently transpiles test TypeScript with an empty raw tsconfig while `tsconfig.json` excludes tests and testing support. The Redis harness warms a module-level `ManagedRuntime` to amortize container startup, but does not deliberately dispose it. At least five strict test type errors are therefore invisible to `pnpm typecheck`, and several suites coordinate through sleeps or direct runtime calls.

## Goals / Non-Goals

**Goals:**

- Make Effect failures, defects, services, and scopes native to the test runner.
- Share expensive integration resources without process-global unmanaged state.
- Make the entire test surface part of the TypeScript and release contract.
- Make unit-level timing and concurrency deterministic.

**Non-Goals:**

- Eliminate real Redis integration tests or container reuse within a suite scope.
- Force Redis server clocks and TTLs onto Effect `TestClock`.
- Replace Vitest assertions or rewrite pure synchronous tests as Effects.
- Preserve the current `TestRuntime.runPromise` helper API.

## Decisions

### 1. Use `@effect/vitest` for Effectful tests

Add the Effect-version-matched `@effect/vitest` package. Effectful cases use `it.effect`; suites needing services use its layer facility so acquisition and release are owned by the suite scope. Pure schema/value tests continue using ordinary Vitest `it`. Assertions remain explicit Vitest assertions inside the Effect body.

A custom wrapper around `Effect.runPromise` was rejected because it recreates runner integration and hides structured Effect causes. A module-level `ManagedRuntime` was rejected because sharing and disposal are coupled to import lifetime rather than a test scope.

### 2. Share Redis through a suite-scoped Layer

`src/testing` exposes a Redis integration Layer that acquires the container or configured local server, clients, and package services with `Effect.acquireRelease`. Suites install that layer once at the narrowest useful scope. Long container startup is handled by an explicit suite/hook timeout and health check, not by top-level warming. Fault and Sentinel variants compose their own layers from the same acquisition primitives.

Starting a container per test was rejected as unnecessarily slow. Process-global caching was rejected because failures and interruption cannot reliably release ownership.

### 3. Add a dedicated strict test compiler program

Create `tsconfig.test.json` extending production compiler options and including all `src/**/*.test.ts`, `src/testing/**/*.ts`, and type-contract fixtures. Add `typecheck:test` and make `typecheck`/`check`/CI run both production and test programs. Vitest's esbuild transformation remains an execution optimization, never the diagnostic typechecker.

Expanding the production build config to emit tests was rejected because tests must not enter `dist`. Maintaining a one-off shell list of test files was rejected because it can silently miss new files.

### 4. Assert public channels without suppression comments

Dedicated `*.types.test.ts` files use compile-time equality/assignability helpers and Vitest's type assertions to inspect `Effect.Success`, `Effect.Error`, and `Effect.Context` for public operations. Tests cover both positive exact equality and guards that detect `any`/`unknown`. They do not rely on `@ts-expect-error`, casts, or runtime execution to prove types.

Snapshotting generated declarations was rejected because textual snapshots are noisy and do not prove assignability. Type-suppression tests were rejected because they can continue passing after an unrelated error changes.

### 5. Use virtual time and explicit latches by default

Retry schedules, worker coordination, cancellation, and timeouts use `TestClock`, `Deferred`, `Latch`, `Queue`, or observable events. Tests advance time only after the relevant fiber is known to be waiting. Redis TTL, restart, and Sentinel failover tests remain real-time integration tests; they use bounded retry/poll effects with useful timeout diagnostics instead of fixed sleeps. File naming or test metadata distinguishes these suites.

Mocking `Date.now` was rejected because it does not control Effect scheduling. Applying TestClock to Redis server expiration was rejected because the server owns that clock.

### 6. Test infrastructure obeys production boundary rules

Testcontainers, node-redis, and ioredis promises use `Effect.tryPromise` with focused `TestInfrastructureError` reasons. Acquired resources attach their finalizers immediately. Infallible `Effect.succeed` and `Effect.promise` are used only when the callback is demonstrably non-throwing/non-rejecting. Cleanup tests exercise success, failed acquisition, assertion failure, timeout, and interruption.

## Risks / Trade-offs

- [Suite-layer API changes across Effect beta versions] → Pin `@effect/vitest` to the exact Effect beta used by development dependencies and update them together.
- [Shared integration layers permit cross-test state leakage] → Allocate unique queue prefixes per test and reset only resources owned by that prefix; keep parallelism explicit.
- [Strict test compilation initially creates a large migration] → Fix the five known errors first, then migrate suites incrementally while keeping `typecheck:test` mandatory once green.
- [Leak detection hangs CI] → Use bounded finalizer timeouts and report the identities of remaining resources/fibers before failing.

## Migration Plan

1. Add `@effect/vitest`, `tsconfig.test.json`, type-contract helpers, and CI scripts; fix the five currently known strict test errors.
2. Build the scoped Redis/container Layers and lifecycle tests.
3. Migrate integration suites from `TestRuntime.runPromise` to Effect-aware suite layers, then delete the ManagedRuntime runner.
4. Migrate isolated direct runtime calls and unsafe test promise boundaries.
5. Replace unit sleeps with virtual time/latches and classify unavoidable real-time Redis suites.
6. Update contributor guidance and release checks; run typecheck, lint, unit, integration, fault, package, and leak gates. Rollback is a release revert because the old runner utility is removed.
