## ADDED Requirements

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

After retries are exhausted (or skipped), the engine SHALL place the task according to `onFailurePolicy`: `delete` removes the task entirely, `mark-as-failure` adds it to the failed list, and `keep` removes it from all lists while preserving the task hash.

#### Scenario: delete failure policy
- **WHEN** a terminally-failed task has `onFailurePolicy` of `delete`
- **THEN** the task appears in no list and `getTask` returns null

#### Scenario: mark-as-failure failure policy
- **WHEN** a terminally-failed task has `onFailurePolicy` of `mark-as-failure`
- **THEN** the task appears in the `failed` list and `getTask` still returns the task with its errors

#### Scenario: keep failure policy
- **WHEN** a terminally-failed task has `onFailurePolicy` of `keep`
- **THEN** the task appears in no list but `getTask` still returns the task with its errors

### Requirement: onSuccessPolicy placement

On `writeSuccess`, the engine SHALL place the task according to `onSuccessPolicy`: `delete` removes the task entirely, `mark-as-success` adds it to the success list, and `keep` removes it from all lists while preserving the task hash.

#### Scenario: mark-as-success policy
- **WHEN** a taken task with `onSuccessPolicy` of `mark-as-success` receives `writeSuccess`
- **THEN** the task appears in the `success` list and `getTask` still returns the task

#### Scenario: keep success policy
- **WHEN** a taken task with `onSuccessPolicy` of `keep` receives `writeSuccess`
- **THEN** the task appears in no list but `getTask` still returns the task
