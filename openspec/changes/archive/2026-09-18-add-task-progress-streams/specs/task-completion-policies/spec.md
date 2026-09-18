# Spec Delta

## ADDED Requirements

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
