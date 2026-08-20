## Why

Redis, the CLI, wall-clock time, randomness, and JavaScript promise callbacks are external boundaries, but several paths currently bypass Effect services, trust unchecked replies, or erase defects and interruption. Those shortcuts make lifecycle, determinism, and failure behavior depend on ambient process state.

## What Changes

- Move CLI configuration, Redis acquisition, execution, and shutdown into one scoped Effect program with `NodeRuntime.runMain` at the process edge.
- Wrap every promise- or callback-based Redis operation at its owning boundary with semantic typed errors and scoped finalizers; never swallow defects or interruption while interpreting readiness failures.
- **BREAKING** Require Effect `Clock` and cryptographic randomness services for timestamps and generated task identities instead of reading `Date`, `Date.now`, or `crypto.randomUUID` directly.
- Validate all Redis replies before use, including stream replies and text conversion, and fail with a semantic invalid-reply error instead of asserting shapes.
- Replace prototype-bearing dynamic records with safe maps or null-prototype dictionaries at untrusted-key boundaries.

## Capabilities

### New Capabilities

- `effect-runtime-boundaries`: Defines Effect-owned configuration, time, randomness, promise adaptation, resource lifetime, and external-data validation rules.

### Modified Capabilities

- `redis-operations`: Requires Redis connection lifecycle, readiness, and reply decoding to preserve semantic failures, defects, interruption, and scoped cleanup.
- `storage-protocol`: Requires externally sourced Redis values to be structurally validated before storage records or events are constructed.

## Impact

The change affects `NodeRedisPool`, `TaskEngine`, `TaskQueue`, `Scheduler`, `Task`, the inspection CLI, Redis adapters, and tests that currently rely on ambient time or UUID generation. Public effects gain explicit `Clock` and randomness requirements where those capabilities are actually used.
