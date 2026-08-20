# task-events-stream

## Purpose

Defines how the TaskEngine publishes task-lifecycle events to a per-queue Redis
Stream and exposes them as a decoded Effect `Stream`, and how TaskQueue builds
typed streaming, `wait`, and `execute` on top of it.

## Requirements

### Requirement: Engine publishes task-lifecycle events

The TaskEngine SHALL publish a versioned event whenever a task's lifecycle
state changes. Events SHALL include a stable event id, task generation
identity, event tag, protocol version, and tag-specific payload. Returning an
unchanged task for a duplicate offer SHALL NOT emit an update event; an explicit
accepted replacement SHALL emit `task.updated`.

#### Scenario: Task created emits task.created
- **WHEN** a new task generation is created
- **THEN** a `task.created` event is published carrying its generation identity and new state

#### Scenario: Duplicate offer returns unchanged
- **WHEN** a create is issued for an existing task without an accepted replacement
- **THEN** the existing task is returned and no lifecycle update event is emitted

#### Scenario: Task state changes
- **WHEN** a task moves between delayed, waiting, leased, retry-scheduled, succeeded, and failed states
- **THEN** a state-change event is published carrying the previous and new states

#### Scenario: Task attempt fails
- **WHEN** an attempt reports a typed handler failure, lease loss, or stall
- **THEN** a failure event identifies the failure kind, attempt, terminal status, and next retry time when present

#### Scenario: Task succeeds
- **WHEN** the current fenced attempt reports success
- **THEN** a terminal completion event is published carrying the encoded success envelope and completion policy

### Requirement: Engine exposes an event stream

The TaskEngine SHALL expose a decoded Effect `Stream` beginning after an
explicit authoritative Redis cursor. When no cursor is supplied, the API SHALL
obtain a server-authoritative start position. It SHALL advance from the last
yielded id and expose the earliest retained cursor and a typed cursor-expired
error.

#### Scenario: Stream yields published events
- **WHEN** a consumer opens a stream and a later event is published
- **THEN** the consumer receives the decoded event exactly once within that stream run

#### Scenario: Stream resumes from a retained cursor
- **WHEN** a consumer opens the stream with a retained prior event id
- **THEN** only retained events after that cursor are delivered

#### Scenario: Cursor has expired
- **WHEN** a consumer resumes from an id older than event retention
- **THEN** the stream fails with a typed cursor-expired error containing the earliest available cursor

### Requirement: TaskQueue stream decodes payloads against queue schemas

`TaskQueue.stream(queue)` SHALL validate the storage protocol and decode task
payloads, successes, and failures against the queue's schemas. Unsupported
versions, corruption, and schema mismatches SHALL remain typed stream failures
and SHALL NOT be normalized into valid-looking events.

#### Scenario: Typed task in created event
- **WHEN** a supported `task.created` event is streamed for a typed queue
- **THEN** its task value is decoded to the queue's task type

#### Scenario: Corrupt failure event
- **WHEN** a failure event contains invalid encoded bytes
- **THEN** the stream fails with a typed decoding error rather than yielding an empty or partial failure

### Requirement: wait and execute await a task's terminal event

`TaskQueue.wait` SHALL accept a task/result handle containing generation identity and an authoritative cursor. It SHALL check durable terminal state, subscribe from the cursor, then recheck state to close the race. It SHALL resolve typed success or fail with typed task failure, task-not-found, result-expired, cursor-expired, timeout, storage, engine, or schema failures. `TaskQueue.execute` SHALL be behaviorally and type-equivalent to offering a task and awaiting the returned handle through this protocol; its public failure and service channels SHALL contain the full union required by both operations.

#### Scenario: Task already completed
- **WHEN** `wait` begins after the retained task has already completed
- **THEN** it resolves immediately from durable terminal state

#### Scenario: Completion races subscription
- **WHEN** completion occurs between the initial state read and stream subscription
- **THEN** the subscription or recheck observes the same terminal generation and `wait` resolves

#### Scenario: Result expired
- **WHEN** terminal metadata exists but its result retention has expired
- **THEN** `wait` fails with a typed result-expired error

#### Scenario: Execute round-trips a task
- **WHEN** `execute` offers a task and a managed worker completes it
- **THEN** it resolves with the decoded success for the offered generation

#### Scenario: Execute observes a terminal task failure
- **WHEN** the offered generation reaches terminal failure
- **THEN** `execute` fails with the same typed task-failure wrapper that `wait` returns

#### Scenario: Execute requires schema services
- **WHEN** offering or awaiting requires payload, success, or failure schema services
- **THEN** the generated `execute` type requires the complete union of those services
