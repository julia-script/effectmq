## Purpose

Defines how effectmq owns ambient capabilities, asynchronous JavaScript integrations, and resource lifetimes so execution remains typed, deterministic, and interruptible.

## ADDED Requirements

### Requirement: Ambient capabilities are explicit
Operations that observe time, generate task identities, or read process configuration SHALL obtain those capabilities from their Effect environment at execution time. They SHALL NOT capture wall-clock time when an Effect is constructed or read ambient randomness/configuration inside domain logic.

#### Scenario: Delayed effect observes current execution time
- **WHEN** an Effect is constructed and executed after the clock has advanced
- **THEN** its timestamp is based on execution time rather than construction time

#### Scenario: Generated task identity is deterministic in a test
- **WHEN** a caller supplies a deterministic cryptographic-randomness service
- **THEN** default task identity generation uses that service

### Requirement: Asynchronous foreign APIs are adapted honestly
Promise and callback integrations SHALL be wrapped at the module that owns the foreign API. Predictable rejections SHALL become semantic typed errors, cancellation SHALL interrupt cancelable work when supported, and defects or interruption SHALL NOT be caught and reclassified as an expected negative result.

#### Scenario: Readiness check is interrupted
- **WHEN** a fiber checking Redis readiness is interrupted
- **THEN** interruption propagates instead of being converted to `false`

#### Scenario: Redis promise rejects
- **WHEN** a Redis client promise rejects with an expected connection failure
- **THEN** the owning adapter returns a semantic typed error containing the original cause

### Requirement: Resources are scoped
Every acquired Redis client, runtime bridge, listener registration, and test or CLI resource SHALL have a release action attached to the same Effect scope. Release failures SHALL be observed according to the public shutdown policy rather than becoming untracked promise rejections.

#### Scenario: CLI inspection is interrupted
- **WHEN** the inspection process is interrupted while scanning
- **THEN** the Redis connection is closed by scope finalization before process exit

#### Scenario: Connection acquisition fails midway
- **WHEN** one resource in a multi-connection pool fails after earlier resources were acquired
- **THEN** every successfully acquired resource is released

### Requirement: Process entry points stay at the edge
Command-line entry points SHALL compose configuration, layers, logging, and the application as a single Effect and invoke the runtime only once at the outermost process boundary.

#### Scenario: Redis URL is absent
- **WHEN** the inspection command starts without required Redis configuration
- **THEN** it exits through a typed configuration failure rendered by the process runner
- **AND** no Redis client is constructed
