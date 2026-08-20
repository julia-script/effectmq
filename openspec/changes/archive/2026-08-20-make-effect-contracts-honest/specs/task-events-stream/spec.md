## MODIFIED Requirements

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
