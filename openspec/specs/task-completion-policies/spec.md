# task-completion-policies

## Purpose

Defines how the TaskEngine disposes of a task once it is done — covering retry behavior on failure, cancellation handling, and the placement dictated by `onFailurePolicy` and `onSuccessPolicy`. Policies apply when the task *dies* (done and `refCount` 0 — see `task-pinning`); for unpinned tasks that is the moment of completion.

## Requirements

### Requirement: Failure retries until maxRetries

When a task fails via `writeError` with a non-`Canceled` error and its accumulated error count is below `maxRetries`, the engine SHALL re-queue the task to the wait list for another attempt rather than applying the failure policy.

#### Scenario: Retry on first failure with retries remaining
- **WHEN** a task with `maxRetries` of 2 is taken and `writeError` is called once
- **THEN** the task returns to the `wait` list and its `errors` array has one entry

#### Scenario: Failure policy applies once retries are exhausted
- **WHEN** the same task accumulates failures equal to `maxRetries`
- **THEN** the task leaves the `active`/`wait` lists and is placed according to its `onFailurePolicy`

### Requirement: Canceled errors skip retries

When a task fails with an error tagged `~effectmq/Error/Canceled`, the engine SHALL apply the failure policy immediately without consuming remaining retries.

#### Scenario: Canceled task is not retried
- **WHEN** a task with `maxRetries` greater than 0 receives a `writeError` carrying a `_tag` of `~effectmq/Error/Canceled`
- **THEN** the task does not return to the `wait` list and the `onFailurePolicy` is applied immediately

### Requirement: onFailurePolicy placement

After retries are exhausted (or skipped), the engine SHALL apply `onFailurePolicy` when the task dies (immediately if `refCount` is 0, otherwise deferred until `refCount` reaches 0): `delete` removes the task entirely, `mark-as-failure` adds it to the failed list, and `keep` removes it from all lists while preserving the task hash. While a terminally-failed task remains pinned (`refCount > 0`), it SHALL appear in no list and its record SHALL be retained regardless of policy.

#### Scenario: delete failure policy
- **WHEN** a terminally-failed unpinned task has `onFailurePolicy` of `delete`
- **THEN** the task appears in no list and `getTask` returns null

#### Scenario: mark-as-failure failure policy
- **WHEN** a terminally-failed unpinned task has `onFailurePolicy` of `mark-as-failure`
- **THEN** the task appears in the `failed` list and `getTask` still returns the task with its errors

#### Scenario: keep failure policy
- **WHEN** a terminally-failed unpinned task has `onFailurePolicy` of `keep`
- **THEN** the task appears in no list but `getTask` still returns the task with its errors

#### Scenario: pinned terminal failure defers the policy
- **WHEN** a task with `refCount` of 1 terminally fails with `onFailurePolicy` of `delete`
- **THEN** the task appears in no list, `getTask` still returns the task with its errors, and the record is deleted only when its `refCount` reaches 0

### Requirement: onSuccessPolicy placement

On `writeSuccess`, the engine SHALL apply `onSuccessPolicy` when the task dies (immediately if `refCount` is 0, otherwise deferred until `refCount` reaches 0): `delete` removes the task entirely, `mark-as-success` adds it to the success list, and `keep` removes it from all lists while preserving the task hash. While a succeeded task remains pinned (`refCount > 0`), it SHALL appear in no list and its record SHALL be retained regardless of policy.

#### Scenario: mark-as-success policy
- **WHEN** a taken unpinned task with `onSuccessPolicy` of `mark-as-success` receives `writeSuccess`
- **THEN** the task appears in the `success` list and `getTask` still returns the task

#### Scenario: keep success policy
- **WHEN** a taken unpinned task with `onSuccessPolicy` of `keep` receives `writeSuccess`
- **THEN** the task appears in no list but `getTask` still returns the task

#### Scenario: pinned success defers the policy
- **WHEN** a task with `refCount` of 1 receives `writeSuccess` with `onSuccessPolicy` of `delete`
- **THEN** `getTask` still returns the task with its result until its `refCount` reaches 0, at which point the record is deleted

### Requirement: Success and failed lists contain only dead tasks

The engine SHALL only place tasks in the `success` or `failed` lists at death. A record in these lists SHALL hold no refs, so removing it requires no ref bookkeeping.

#### Scenario: Pinned task enters the list only at death
- **WHEN** a pinned task completes with `mark-as-success` and later its last holder dies
- **THEN** the task appears in the `success` list only after the holder's death, never before
