# task-completion-policies

## Purpose

Defines terminal index placement and record disposal after settlement, separate
from retry and explicit result retention.

## Requirements

### Requirement: Canceled failures skip handler retries

An error tagged `~effectmq/Error/Canceled` SHALL settle according to the failure
policy without consuming remaining handler retries.

#### Scenario: Canceled attempt
- **WHEN** a current attempt reports the built-in canceled tag
- **THEN** the task does not return to a runnable state

### Requirement: onFailurePolicy placement

After handler retries are exhausted or skipped, the engine SHALL settle the
generation as failed. `delete` removes its record when no retention hold
prevents disposal; `mark-as-failure` indexes and retains it; `keep` retains it
outside terminal indexes. Active holds SHALL affect disposal only and SHALL NOT
hide terminal state.

#### Scenario: Marked failure with a hold
- **WHEN** a held task settles with `mark-as-failure`
- **THEN** it appears in the failed index immediately and remains readable

### Requirement: onSuccessPolicy placement

On a valid fenced acknowledgement, `delete` removes the record when allowed,
`mark-as-success` indexes and retains it, and `keep` retains it outside terminal
indexes. Active holds SHALL affect disposal only.

#### Scenario: Delete success with a hold
- **WHEN** a held task succeeds under delete policy
- **THEN** it is terminal and non-runnable while its result remains readable

### Requirement: Terminal indexes contain terminal tasks

Success and failed indexes SHALL contain only settled generations. A terminal
generation MAY have active retention holds; membership describes outcome and
does not imply immediate disposability.

#### Scenario: Held marked success
- **WHEN** a held task succeeds with `mark-as-success`
- **THEN** it appears in the success index immediately
