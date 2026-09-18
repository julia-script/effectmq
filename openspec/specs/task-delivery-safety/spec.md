# task-delivery-safety

## Purpose

Defines the execution-state and ownership guarantees that make task delivery
safe under retries, crashes, lease expiry, duplicate offers, and concurrent
workers.

## Requirements

### Requirement: Delivery guarantee is at-least-once

The library SHALL describe task execution as at-least-once and SHALL NOT claim
that a handler or its external side effects execute exactly once.

#### Scenario: Worker dies after an external side effect
- **WHEN** a handler performs an external side effect and the worker dies before acknowledging success
- **THEN** the task becomes eligible for another attempt after ownership expires
- **AND** the documentation identifies idempotent handlers or an inbox/outbox as the consumer's duplicate-safety mechanism

### Requirement: Every runnable task has exactly one execution state

A task SHALL be in exactly one execution state among delayed, waiting, leased,
retry-scheduled, succeeded, or failed. Repeating a transition to the current
state SHALL preserve membership and SHALL NOT strand or duplicate the task.

#### Scenario: Lease renewal preserves active membership
- **WHEN** the current owner renews a leased task
- **THEN** the task remains leased exactly once and remains discoverable for expiry recovery

#### Scenario: Repeating a waiting transition
- **WHEN** a waiting task is transitioned to waiting again
- **THEN** it remains present exactly once in the waiting state

### Requirement: Each attempt has a fenced lease

Every task attempt SHALL receive a unique lease token. Renewal, success,
failure, release, and custom progress append SHALL require the current token,
and a token from an earlier attempt SHALL never mutate the task or its history.
Progress append SHALL additionally validate the exact task generation, enabled
history, leased execution state, and unexpired ownership atomically with the
append. Expired ownership SHALL reject progress even before another worker
acquires the task or maintenance recovers it.

#### Scenario: Late acknowledgement from an old attempt
- **WHEN** attempt A loses its lease, attempt B acquires a new lease, and attempt A later reports success
- **THEN** the acknowledgement fails with `LeaseLost`
- **AND** attempt B remains the sole owner

#### Scenario: Late progress from an old attempt
- **WHEN** attempt B owns the task and attempt A attempts to append progress
- **THEN** the append fails with a typed ownership error
- **AND** no entry is added and attempt B remains the sole owner

#### Scenario: Expired lease has not yet been recovered
- **WHEN** a worker attempts to append after its lease deadline but before another worker or maintenance recovers the task
- **THEN** the append fails with a typed ownership error and adds no entry

#### Scenario: Append races completion or deletion
- **WHEN** custom progress and settlement or task deletion execute concurrently
- **THEN** progress is accepted only if the task is still leased by that attempt at the atomic append
- **AND** no custom progress is accepted after settlement or recreates a deleted history

#### Scenario: Current attempt emits progress
- **WHEN** the current owner appends valid progress to an enabled live generation
- **THEN** the append succeeds without changing its lease deadline, execution state, or retry counters

### Requirement: Lease loss stops managed work

The managed processing API SHALL supervise lease renewal together with the
handler. Missing ownership or a terminal renewal failure SHALL interrupt the
handler and fail processing with a typed ownership error.

#### Scenario: Heartbeat loses ownership
- **WHEN** lease renewal reports that the token is no longer current
- **THEN** the handler fiber is interrupted
- **AND** the stale attempt does not report a task outcome

### Requirement: Duplicate offers are state-aware

Offering an existing id SHALL return the existing task unchanged by default.
Replacement SHALL require an explicit mode and SHALL be rejected for leased or
terminal tasks unless a new generation is requested.

#### Scenario: Duplicate offer while leased
- **WHEN** a producer offers an id that is currently leased
- **THEN** the existing payload, lease, history, and state remain unchanged
- **AND** the producer receives the existing-task outcome

#### Scenario: New generation after terminal outcome
- **WHEN** a producer explicitly requests a new generation for a terminal task id
- **THEN** the new task has a distinct generation identity and no fields from the terminal generation are retained implicitly

### Requirement: Stalled recovery is bounded

Lease-expired attempts SHALL be counted separately from handler failures and
SHALL be retried only up to a configurable `maxStalledCount`. Exceeding the
limit SHALL produce a terminal stalled failure eligible for dead-letter
retention.

#### Scenario: Repeated worker crashes
- **WHEN** a task exceeds its configured stalled-attempt limit
- **THEN** it is not returned to waiting
- **AND** it settles with a typed terminal stalled error
