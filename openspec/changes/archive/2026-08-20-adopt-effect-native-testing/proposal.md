## Why

Production typechecking currently excludes tests, the test harness uses a warmed `ManagedRuntime` as a runner without disposing it, and timing tests rely on sleeps and wall-clock scheduling. Consequently, passing tests can conceal invalid Effect contracts, resource leaks, and nondeterministic behavior.

## What Changes

- **BREAKING** Replace direct `Effect.runPromise` and module-level `ManagedRuntime` execution with `@effect/vitest` Effect tests and suite-scoped test layers.
- Make Redis test infrastructure acquire and release containers, clients, and layers through Effect scopes with typed promise adaptation.
- Add strict compilation of every test and testing-support file, plus compile-time contract assertions for public Effect error and service channels.
- Replace arbitrary sleeps and wall-clock timing assertions with `TestClock`, `Deferred`, latches, or explicit real-time integration boundaries.
- Update repository guidance and CI so typecheck, lint, unit tests, integration tests, and lifecycle checks enforce the same model.

## Capabilities

### New Capabilities

- `effect-test-harness`: Defines the Effect-native test runner, scoped layer lifecycle, strict typechecking, compile-time contract tests, and deterministic concurrency/timing rules.

### Modified Capabilities

- `production-release`: Requires the release gate to compile the complete test surface and detect leaked test resources in addition to executing the behavioral suites.

## Impact

The change adds `@effect/vitest`, replaces shared test-runtime utilities, touches all Effectful test suites, adds a dedicated strict test typecheck configuration, and changes CI and contributor documentation. Test setup may become structurally different, but runtime product behavior is unchanged.
