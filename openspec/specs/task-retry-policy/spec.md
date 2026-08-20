# task-retry-policy

## Purpose

Defines how a task's retry-on-failure behavior is declared on its `TaskDefinition`, how the engine computes the next run time from an Effect `Schedule` and reschedules failed tasks, how `maxRetries` caps unbounded schedules, and how retry timing is recorded and emitted. Also constrains the public API to the managed `complete` path so the retry flow is applied consistently.

## Requirements

### Requirement: Retry policy declared on the task definition

The retry policy SHALL be declared on the `TaskDefinition` via `Task.make`, not per-offer. `Task.make` MUST accept a `retry` value that is either an Effect `Schedule` (keyed on the task's error type) or an options object `{ while?, until?, times?, schedule? }` that is normalized into a `Schedule`. When no `retry` is provided, a failing task MUST NOT be retried.

#### Scenario: Task defined with a Schedule

- **WHEN** a task is defined with `retry: Schedule.exponential("1 second")`
- **THEN** the resulting `TaskDefinition` carries that schedule as its `retrySchedule`
- **AND** offering and failing that task once schedules a retry per the schedule

#### Scenario: Task defined with retry options

- **WHEN** a task is defined with `retry: { times: 3, schedule: Schedule.spaced("5 seconds") }`
- **THEN** the options are normalized into a single `Schedule`
- **AND** the task retries at the spaced interval up to the options' bound

#### Scenario: No retry configured

- **WHEN** a task is defined without a `retry` value and its handler fails
- **THEN** the task is not re-scheduled and the queue's failure policy is applied immediately

### Requirement: Failed tasks are scheduled at the computed next run time

On failure, when a retry is due, the engine SHALL compute the next run time from the definition's `Schedule` applied to the task's `createdAt` (plus initial `delay`) and its accumulated error history, and route the task to the **scheduled** list at that time. A task whose computed next run time is already in the past SHALL go to the **wait** list instead.

#### Scenario: Next run time in the future

- **WHEN** a task fails and its schedule yields a next run time later than now
- **THEN** the task is placed on the scheduled list keyed to that time
- **AND** it becomes available on the wait list once that time is reached

#### Scenario: Next run time already elapsed

- **WHEN** a task fails and its computed next run time is at or before now
- **THEN** the task is placed directly on the wait list

#### Scenario: Schedule is exhausted

- **WHEN** a task fails and its schedule yields no further run time
- **THEN** the task is not retried and its failure policy is applied

### Requirement: maxRetries caps an unbounded schedule

`maxRetries` SHALL cap retries caused by typed handler failures and SHALL NOT be
overloaded with lease-loss recovery. A task-definition default of 5 SHALL apply
unless explicitly overridden for the task; `null` SHALL disable the
handler-failure cap. The stored attempt history SHALL distinguish handler
failures from stalls and ownership loss.

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

### Requirement: Stalled attempts have a separate bounded policy

Lease expiry and ownership loss SHALL increment a stalled-attempt counter and
SHALL NOT be fed into the user's typed error `Schedule`. A configurable
`maxStalledCount` SHALL bound recovery and default to a finite value.

#### Scenario: Stall below the cap

- **WHEN** an attempt loses its lease and stalled attempts remain
- **THEN** the task returns to an eligible retry state with a stalled event

#### Scenario: Stall cap exhausted

- **WHEN** another lease expires after the stalled cap is exhausted
- **THEN** the task settles with a terminal built-in stalled error

### Requirement: Ownership loss is not a handler failure

A stale or lost lease SHALL fail the processing attempt with `LeaseLost` and
SHALL NOT append the handler's typed error or run its retry schedule.

#### Scenario: Old attempt reports failure

- **WHEN** a stale attempt tries to report a typed handler failure
- **THEN** acknowledgement fails with `LeaseLost`
- **AND** neither error history nor retry schedule state changes

### Requirement: Canceled errors short-circuit retries

A task that fails with the built-in `Canceled` error SHALL NOT be retried, regardless of its schedule or remaining cap; its failure policy applies immediately.

#### Scenario: Cancellation skips remaining retries

- **WHEN** a task fails with a `Canceled` error and retries remain
- **THEN** the task is not re-scheduled and its failure policy is applied

### Requirement: Retry time is recorded and emitted

Each stored error entry SHALL record the `retryAt` time computed for that failure (absent when no retry is scheduled), and the `task.failed` lifecycle event SHALL include the `retryAt` value so consumers can observe when a retry will run.

#### Scenario: Failed event carries retryAt

- **WHEN** a task fails and a retry is scheduled
- **THEN** the emitted `task.failed` event payload includes the `retryAt` timestamp
- **AND** the task's stored error entry records the same `retryAt`

### Requirement: Manual queue primitives are not part of the public API

The low-level primitives that hand-drive the queue — taking a task, reporting success, and reporting failure (`takeUnsafe`, `succeed`, `fail`) — SHALL NOT be exported from the public API. The managed `complete` path SHALL be the supported way to process a task, so that the retry-on-failure flow is applied consistently.

#### Scenario: Primitives are not reachable

- **WHEN** a consumer imports the library's public surface
- **THEN** `takeUnsafe`, `succeed`, and `fail` are not available
- **AND** `complete` is available and applies the definition's retry policy on failure
