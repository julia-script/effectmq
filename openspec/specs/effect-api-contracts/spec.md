# effect-api-contracts

## Purpose

Defines public Effect APIs whose declared success, failure, and service channels exactly describe every outcome and dependency observable by callers.

## Requirements

### Requirement: Public Effect signatures are complete
Every public Effect-returning API SHALL expose the union of all failures it can return and all services it can request, including failures and services introduced by delegated queue, engine, storage, and schema operations. Public declarations SHALL NOT widen a failure channel to `any`, erase a required service, or claim an infallible channel for a recoverable failure.

#### Scenario: Completion uses schema services
- **WHEN** a completion handler requires an environment and its payload, success, and failure schemas require encoding or decoding services
- **THEN** the completion API's public type requires the handler environment, engine service, and every schema service it uses
- **AND** its failure type includes every recoverable engine and schema failure it can return

#### Scenario: One-item completion is inspected by a consumer
- **WHEN** a consumer inspects the generated declaration for the one-item completion API
- **THEN** its failure channel is an exact named union rather than `any`

### Requirement: Task decoding preserves all schema requirements
Decoding a stored task SHALL expose the decoding services required by the payload, success, and failure schemas and SHALL retain all corresponding schema failures in the typed channel.

#### Scenario: Payload decoder requires a service
- **WHEN** a task payload schema depends on a decoding service
- **THEN** the stored-task decoder cannot be executed until that service is provided

### Requirement: Definition constructors are pure
Public construction of task, queue, worker, and scheduler definitions SHALL synchronously return reusable descriptions. Definition constructors SHALL NOT return an Effect, require runtime services, throw for invalid definition configuration, or perform runtime invariant validation.

#### Scenario: Definitions live at module scope
- **WHEN** a caller constructs a task, binds it to a queue, and creates a worker or scheduler
- **THEN** every definition is available without entering an Effect or providing a runtime

#### Scenario: Invalid task configuration is first consumed
- **WHEN** a pure task definition contains an invalid retry, storage, or retention value
- **THEN** construction still returns the definition without throwing
- **AND** the first queue or worker operation that consumes the invalid value terminates with a defect before external work begins
- **AND** the programmer invariant does not appear in the operation's typed error channel

### Requirement: Runtime infrastructure configuration is typed
Public construction of runtime infrastructure, including engine layers, SHALL validate caller-supplied operational configuration in an Effect and fail with a semantic configuration error. It SHALL NOT throw synchronously or terminate with a defect for predictably invalid external or runtime configuration.

#### Scenario: Invalid engine limit
- **WHEN** an engine layer is built with an invalid size or batch limit
- **THEN** layer construction fails in its typed error channel before Redis work begins

### Requirement: Expected failures have semantic identities
Every expected public failure SHALL have a stable tagged identity and structured fields sufficient for programmatic recovery. Human-readable messages and nested causes SHALL provide diagnostics only and SHALL NOT determine control flow.

#### Scenario: Recovery branches on a failure
- **WHEN** an operation reaches a relationship limit or an indeterminate transport outcome
- **THEN** the caller can distinguish the condition by tag and structured reason without parsing a message or nested cause text

### Requirement: Public operation inputs are obtainable
Every public operation that accepts an opaque library-owned value SHALL be paired with a public operation that produces that value in the same abstraction. A high-level API SHALL NOT expose only the continuation half of a lower-level lifecycle.

#### Scenario: Caller manages a task attempt
- **WHEN** a caller needs to acquire, extend, or release a low-level task attempt
- **THEN** those operations are available together through the TaskEngine lifecycle API
- **AND** the high-level typed queue API does not require an attempt value it cannot produce
