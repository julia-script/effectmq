## ADDED Requirements

### Requirement: complete runs the handler and reports success

When a task is offered and `complete` is invoked with a handler that returns a success value, the combinator SHALL take the task, run the handler, persist the success via the engine according to the queue's `onSuccessPolicy`, and return `true`.

#### Scenario: handler succeeds
- **WHEN** a payload is offered to a queue and `complete` runs with a handler that returns a success value
- **THEN** `complete` resolves to `true` and the task is no longer on the wait or active lists

#### Scenario: typed payload is delivered to the handler
- **WHEN** the handler inspects the task passed to it
- **THEN** the task's `payload` is the decoded typed object that was offered (not the raw JSON string)

### Requirement: complete routes handler failure to the engine

When the handler returns a typed failure, `complete` SHALL persist the failure via the engine according to the queue's `onFailurePolicy` and return `false`.

#### Scenario: handler fails
- **WHEN** `complete` runs with a handler that fails with the queue's error type
- **THEN** `complete` resolves to `false` and the task is routed per `onFailurePolicy` (e.g. appears on the failed list when `mark-as-failure`)

### Requirement: offer enqueues a task with a deterministic id

When a payload is offered to a queue whose task definition has a deterministic `idempotencyKey`, the engine SHALL store a task whose id equals that key and place it on the wait list (immediate) or scheduled list (delayed).

#### Scenario: offer places task on wait list
- **WHEN** a payload is offered with no delay
- **THEN** a task with the deterministic id appears on the queue's wait list
