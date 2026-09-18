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

After retries are exhausted or skipped, the engine SHALL settle the task as
failed and apply `onFailurePolicy`: `delete` removes the task from execution
indexes and deletes its record once no explicit result-retention holds remain;
`mark-as-failure` places it in the failed index and retains its record according
to configured retention; `keep` retains the record outside terminal indexes
according to configured retention. Active holds SHALL affect record disposal
only and SHALL NOT hide the terminal execution state.

#### Scenario: Delete policy without holds
- **WHEN** a terminally failed task has delete policy and no result-retention holds
- **THEN** it appears in no execution index and its record is deleted

#### Scenario: Delete policy with a hold
- **WHEN** a terminally failed task has delete policy and an active result-retention hold
- **THEN** it is visibly terminal, appears in no runnable state, and its failure remains readable until the final hold releases

#### Scenario: Mark-as-failure policy with a hold
- **WHEN** a terminally failed task has mark-as-failure policy and an active hold
- **THEN** it appears in the failed index immediately and its record remains readable

#### Scenario: Keep failure policy
- **WHEN** a terminally failed task has keep policy
- **THEN** it appears in no execution index and its record is retained until configured expiry or administrative removal

### Requirement: onSuccessPolicy placement

On valid success acknowledgement, the engine SHALL settle the task as succeeded
and apply `onSuccessPolicy`: `delete` removes the task from execution indexes
and deletes its record once no explicit result-retention holds remain;
`mark-as-success` places it in the success index and retains its record according
to configured retention; `keep` retains the record outside terminal indexes
according to configured retention. Active holds SHALL affect record disposal
only and SHALL NOT hide the terminal execution state.

#### Scenario: Delete success with a hold
- **WHEN** a succeeded task has delete policy and an active result-retention hold
- **THEN** its success remains readable until the final hold releases
- **AND** it is not runnable or hidden as an unfinished task

#### Scenario: Mark-as-success policy
- **WHEN** a succeeded task has mark-as-success policy
- **THEN** it appears in the success index immediately and remains according to configured retention

#### Scenario: Keep success policy
- **WHEN** a succeeded task has keep policy
- **THEN** it appears in no execution index and its record remains readable until configured expiry or administrative removal

### Requirement: Success and failed lists contain only dead tasks

The success and failed indexes SHALL contain only terminal tasks. A terminal
task MAY have active result-retention holds; index membership records outcome
and SHALL NOT imply that the task record is immediately disposable.

#### Scenario: Held terminal task is indexed
- **WHEN** a task with mark-as-success policy succeeds while a result-retention hold is active
- **THEN** it appears in the success index immediately
- **AND** its result remains protected from disposal by the hold

### Requirement: Task history shares task-record disposal

An enabled task generation's history SHALL remain available while its task record is retained, subject only to its configured entry-count trimming policy. Disposing of that record SHALL remove its history in the same atomic operation. This rule SHALL apply to success and failure completion policies, retention expiry, ordinary and forced removal, and generation replacement. History SHALL have no independent time-to-live. Retaining a terminal result or an index entry without its task record SHALL NOT retain history. Retention holds that preserve the task record SHALL preserve its history as well.

#### Scenario: Delete-on-completion without holds
- **WHEN** a task with history enabled settles using delete policy with no retention holds
- **THEN** its record and history are removed together
- **AND** polling readers are not guaranteed to receive its terminal history entries

#### Scenario: Retained completion
- **WHEN** an enabled task succeeds or fails with keep or the corresponding mark policy
- **THEN** its history remains readable for as long as the task record remains retained

#### Scenario: Record retention expires
- **WHEN** maintenance disposes of a terminal task record after its retention expires
- **THEN** the associated history is removed in the same operation

#### Scenario: Hold delays deletion
- **WHEN** a task settles with delete policy while a retention hold preserves its record
- **THEN** its history remains readable
- **AND** release of the last hold removes the history when it removes the record

#### Scenario: Ordinary removal is rejected
- **WHEN** ordinary task removal is rejected because the task is leased or held
- **THEN** its history remains unchanged

#### Scenario: Forced removal
- **WHEN** administrative force removal deletes a leased or held task
- **THEN** it also deletes that generation's history
- **AND** a late progress write cannot recreate the history

#### Scenario: Generation replacement
- **WHEN** an accepted new-generation offer disposes of the previous task record
- **THEN** the previous generation's history is disposed of with it
- **AND** the new history has no inherited entries
