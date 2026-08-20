# effect-test-harness

## Purpose

Defines an Effect-native test harness whose execution, typechecking, timing, and resource lifecycle provide trustworthy evidence about the package's public contracts.

## Requirements

### Requirement: Effect tests use the Effect-aware runner
Tests whose body is an Effect SHALL execute through the project's Effect-aware Vitest integration and SHALL receive dependencies through test layers. Test bodies SHALL NOT manually call Effect runtimes or use a `ManagedRuntime` as a general-purpose runner.

#### Scenario: Queue integration test runs
- **WHEN** a queue integration test needs Redis and package services
- **THEN** the test declares an Effect body under a suite-scoped layer
- **AND** the runner reports Effect failures and defects with their structured causes

### Requirement: Test resources are scoped and released
Containers, Redis clients, listeners, fibers, and test layers SHALL be acquired and released by Effect scopes. Suite completion SHALL dispose every acquired resource under success, test failure, timeout, and interruption.

#### Scenario: Test assertion fails after Redis acquisition
- **WHEN** an assertion fails after a suite layer has acquired Redis resources
- **THEN** the suite scope closes all owned clients and containers

#### Scenario: Test suite completes
- **WHEN** the final test using a shared suite layer finishes
- **THEN** no warmed runtime, client, container, listener, or supervised fiber remains live

### Requirement: Complete test source is strictly typechecked
Every test and testing-support TypeScript file SHALL compile under strict settings compatible with production. The test runner's transpilation path SHALL NOT substitute for this diagnostic typecheck.

#### Scenario: Test fixture passes a malformed option
- **WHEN** a test supplies an option shape that is not accepted by the public API
- **THEN** the test typecheck fails before the behavioral suite runs

### Requirement: Public Effect contracts have compile-time assertions
The test suite SHALL assert the exact success, failure, and service channels of public Effect APIs whose contracts compose other operations. Assertions SHALL cover completion, task decoding, waiting, and execution and SHALL fail if a channel widens to `any`, `unknown`, or omits a required member.

#### Scenario: Execute loses a service requirement
- **WHEN** an implementation annotation accidentally removes a schema service from `execute`
- **THEN** a compile-time contract test fails

#### Scenario: CompleteOne widens to any
- **WHEN** the one-item completion failure channel becomes `any`
- **THEN** a compile-time assertion rejects the declaration

### Requirement: Concurrency and timing tests are deterministic
Unit tests SHALL coordinate fibers with virtual time and explicit synchronization primitives rather than arbitrary sleeps or wall-clock race windows. Integration tests that necessarily exercise Redis server time SHALL be labeled as real-time tests and use bounded polling or event latches with documented timeouts.

#### Scenario: Retry delay is tested
- **WHEN** a unit test verifies a retry scheduled after a duration
- **THEN** it advances virtual time and observes the transition without waiting for wall-clock time

#### Scenario: Redis TTL is tested
- **WHEN** an integration test verifies server-owned expiration
- **THEN** it uses a bounded real-time wait classified as integration behavior
- **AND** a timeout produces a diagnostic failure rather than a flaky fixed sleep

### Requirement: Test foreign APIs use typed adapters
Testing support that calls promise- or callback-based external APIs SHALL wrap them with Effect's fallible asynchronous boundaries and semantic test-infrastructure errors. It SHALL NOT place throwing work in infallible Effect constructors.

#### Scenario: Container startup rejects
- **WHEN** the test container library rejects during startup
- **THEN** suite acquisition fails with a typed test-infrastructure error retaining the original cause
