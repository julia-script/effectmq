## MODIFIED Requirements

### Requirement: onFailurePolicy placement

After retries are exhausted or skipped, the engine SHALL settle the task as failed and apply `onFailurePolicy`: `delete` removes the task from execution indexes and deletes its record once no explicit result-retention holds remain; `mark-as-failure` places it in the failed index and retains its record according to configured retention; `keep` retains the record outside terminal indexes according to configured retention. Active holds SHALL affect record disposal only and SHALL NOT hide the terminal execution state.

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

On valid success acknowledgement, the engine SHALL settle the task as succeeded and apply `onSuccessPolicy`: `delete` removes the task from execution indexes and deletes its record once no explicit result-retention holds remain; `mark-as-success` places it in the success index and retains its record according to configured retention; `keep` retains the record outside terminal indexes according to configured retention. Active holds SHALL affect record disposal only and SHALL NOT hide the terminal execution state.

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

The success and failed indexes SHALL contain only terminal tasks. A terminal task MAY have active result-retention holds; index membership records outcome and SHALL NOT imply that the task record is immediately disposable.

#### Scenario: Held terminal task is indexed
- **WHEN** a task with mark-as-success policy succeeds while a result-retention hold is active
- **THEN** it appears in the success index immediately
- **AND** its result remains protected from disposal by the hold
