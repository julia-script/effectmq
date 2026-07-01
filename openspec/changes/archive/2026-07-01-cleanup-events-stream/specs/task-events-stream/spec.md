## ADDED Requirements

### Requirement: Engine publishes task-lifecycle events

The TaskEngine SHALL publish an event to a per-queue Redis Stream (`<prefix>:<name>:events`) whenever a task's lifecycle state changes. The emitted event types SHALL be: `task.created`, `task.updated`, `task.failed`, `task.completed`, and `task.moved`. Each event SHALL carry the task id and a tag-specific payload.

#### Scenario: Task created emits task.created

- **WHEN** a task is created that did not previously exist
- **THEN** a `task.created` event is published carrying the new task

#### Scenario: Existing task re-created emits task.updated

- **WHEN** a create is issued for a task id that already exists
- **THEN** a `task.updated` event is published carrying both the existing and the new task

#### Scenario: Task moved between lists emits task.moved

- **WHEN** a task is moved between the wait/scheduled/active/failed/success lists (or removed)
- **THEN** a `task.moved` event is published carrying `from` and `to` list names

#### Scenario: Task failure emits task.failed

- **WHEN** a handler reports a failure
- **THEN** a `task.failed` event is published carrying the error, `maxRetries`, `retryCount`, failure `policy`, and `willRetry`

#### Scenario: Task success emits task.completed

- **WHEN** a handler reports success
- **THEN** a `task.completed` event is published carrying the success value and success `policy`

### Requirement: Engine exposes an event stream

The TaskEngine SHALL expose a `stream(name, options?)` method that returns an Effect `Stream` of decoded events read from the queue's Redis Stream via `XREAD`. The stream SHALL start from an optional `cursor` (defaulting to the current time) and SHALL poll at a configurable interval, advancing the cursor past events it has yielded.

#### Scenario: Stream yields published events

- **WHEN** a consumer runs the engine stream for a queue and an event is published
- **THEN** the consumer receives the decoded event

#### Scenario: Stream resumes from a cursor

- **WHEN** a consumer opens the stream with a cursor of a prior event id
- **THEN** only events after that cursor are delivered

### Requirement: TaskQueue stream decodes payloads against queue schemas

`TaskQueue.stream(queue)` SHALL wrap the engine stream and decode each event's task-shaped payload against the queue's payload/success/error schemas, so consumers receive typed tasks, success values, and errors rather than raw stored strings.

#### Scenario: Typed task in created event

- **WHEN** a `task.created` event is streamed for a typed queue
- **THEN** `payload.newTask` is decoded to the queue's task type

#### Scenario: Typed error in failed event

- **WHEN** a `task.failed` event is streamed for a typed queue
- **THEN** `payload.error` is decoded against the queue's error schema

### Requirement: wait and execute await a task's terminal event

`TaskQueue.wait(queue, taskId)` SHALL await the task's terminal event and resolve with its typed success value or fail with its typed error. `TaskQueue.execute(queue, payload, options?)` SHALL offer the task and then await its terminal event as a single call.

#### Scenario: wait resolves on completion

- **WHEN** `wait` is called for a task id that subsequently completes
- **THEN** it resolves with the decoded success value

#### Scenario: wait fails on failure

- **WHEN** `wait` is called for a task id that subsequently fails terminally
- **THEN** it fails with the decoded error

#### Scenario: execute round-trips a task

- **WHEN** `execute` is called and a handler completes the task
- **THEN** it resolves with the handler's success value
