# Spec Delta

## MODIFIED Requirements

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
