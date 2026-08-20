## MODIFIED Requirements

### Requirement: maxRetries caps an unbounded schedule

`maxRetries` SHALL cap retries caused by typed handler failures and SHALL NOT be overloaded with lease-loss recovery. A task-definition default of 5 SHALL apply unless explicitly overridden for the task; `null` SHALL disable the handler-failure cap. The stored attempt history SHALL distinguish handler failures from stalls and ownership loss.

#### Scenario: Default cap prevents infinite handler retries
- **WHEN** a task uses an unbounded retry schedule and no override
- **THEN** handler-failure retries stop at the default cap of 5
- **AND** its terminal failure policy is applied subject to result retention

#### Scenario: Per-task override wins
- **WHEN** a task is offered with an explicit handler retry cap
- **THEN** that cap is used instead of the task-definition default

#### Scenario: Explicit unbounded handler retries
- **WHEN** the effective handler retry cap is `null`
- **THEN** the schedule alone determines whether another handler attempt is made

## ADDED Requirements

### Requirement: Stalled attempts have a separate bounded policy

Lease expiry and ownership loss SHALL increment a stalled-attempt counter and SHALL NOT be fed into the user's typed error `Schedule`. A configurable `maxStalledCount` SHALL bound recovery and default to a finite value.

#### Scenario: Stall below the cap
- **WHEN** an attempt loses its lease and stalled attempts remain
- **THEN** the task returns to an eligible retry state with a stalled event

#### Scenario: Stall cap exhausted
- **WHEN** another lease expires after the stalled cap is exhausted
- **THEN** the task settles with a terminal built-in stalled error

### Requirement: Ownership loss is not a handler failure

A stale or lost lease SHALL fail the processing attempt with `LeaseLost` and SHALL NOT append the handler's typed error or run its retry schedule.

#### Scenario: Old attempt reports failure
- **WHEN** a stale attempt tries to report a typed handler failure
- **THEN** acknowledgement fails with `LeaseLost`
- **AND** neither error history nor retry schedule state changes
