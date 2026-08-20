## 1. Install and Enforce the Test Toolchain

- [x] 1.1 Add `@effect/vitest` pinned to the exact Effect beta used by development dependencies.
- [x] 1.2 Add `tsconfig.test.json` extending strict production options and including every test, testing-support, and type-contract file without emitting them.
- [x] 1.3 Add `typecheck:test` and wire production plus test typechecks into `typecheck`, `check`, and CI before behavioral tests.
- [x] 1.4 Remove the empty `tsconfigRaw` diagnostic bypass from Vitest configuration or document it as transform-only after strict compilation is mandatory.

## 2. Repair the Existing Test Type Surface

- [x] 2.1 Fix the NodeRedisPool union narrowing error in `NodeRedisPool.test.ts` without a cast.
- [x] 2.2 Fix the invalid Redis restart socket option shape in `RedisRestart.test.ts`.
- [x] 2.3 Fix the conditional retry schedule overload mismatch in `Scheduler.test.ts` with a correctly typed test value.
- [x] 2.4 Fix the TaskQueue handler error-channel mismatch and unknown-to-number assignment in `TaskQueue.test.ts`.
- [x] 2.5 Run the strict test compiler and resolve any additional errors without assertions or suppressions.

## 3. Add Public Contract Type Tests

- [x] 3.1 Add reusable compile-time helpers that detect exact equality plus `any` and `unknown` channels without suppression comments.
- [x] 3.2 Add positive exact assertions for `complete`, `completeOne`, stored-task decoding, `wait`, and `execute` success/error/context types.
- [x] 3.3 Add mutation checks or focused fixtures proving the contract suite fails when a required service/error is erased or widened.

## 4. Build Scoped Integration Layers

- [x] 4.1 Define typed test-infrastructure errors and wrap Testcontainers, node-redis, and ioredis promises with fallible Effect adapters.
- [x] 4.2 Build suite-scoped Layers for container-backed Redis, configured local Redis, Sentinel, and fault injection using `Effect.acquireRelease`.
- [x] 4.3 Configure explicit acquisition/hook timeouts and health checks instead of module-import warming.
- [x] 4.4 Add lifecycle tests for successful release, partial acquisition failure, assertion failure, timeout, interruption, and listener/fiber cleanup.

## 5. Migrate Test Execution

- [x] 5.1 Migrate Redis-backed suites to `@effect/vitest` Effect cases and the narrowest shared suite layer.
- [x] 5.2 Migrate isolated async Vitest callbacks and direct `Effect.runPromise` calls, including the direct TaskQueue execution test, to Effect-aware cases.
- [x] 5.3 Replace unsafe `Effect.promise`/`Effect.succeed` test boundaries with typed adapters where callbacks can reject or throw.
- [x] 5.4 Delete the warmed ManagedRuntime runner and verify no test-owned runtime remains undisposed.
- [x] 5.5 Preserve ordinary Vitest cases for pure synchronous/value tests and keep assertions explicit.

## 6. Make Timing and Concurrency Deterministic

- [x] 6.1 Replace unit-test sleeps for retries, worker coordination, and cancellation letswith TestClock and explicit Deferred/Latch/Queue synchronization.
- [x] 6.2 Advance virtual time only after tested fibers signal that they are waiting, preventing scheduler races.
- [x] 6.3 Classify Redis TTL, restart, and Sentinel tests as real-time integration tests and replace fixed sleeps with bounded polling or event latches.
- [x] 6.4 Add actionable timeout diagnostics showing the awaited state, queue prefix, and outstanding resource/fiber identities.

## 7. Documentation and Release Verification

- [x] 7.1 Update `CLAUDE.md` and contributor documentation to require Effect-aware tests, suite-scoped layers, strict test typechecking, and deterministic timing.
- [x] 7.2 Add a release lifecycle gate that detects or times out on undisposed test resources.
- [x] 7.3 Run production/test typechecks, lint, unit/integration/fault suites, deterministic timing tests, lifecycle checks, docs checks, and packed-package verification.
