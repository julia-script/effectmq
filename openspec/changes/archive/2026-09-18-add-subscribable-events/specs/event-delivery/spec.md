## Purpose

Defines typed durable event emission and independent, recoverable deliveries to
the subscriptions registered at emission time.

## ADDED Requirements

### Requirement: Emission captures a fixed recipient set
Emission SHALL atomically persist its payload and capture all active registered
subscriptions. Later subscriptions SHALL NOT acquire that event. Payloads SHALL
be validated and encoded using the queue schema before persistence. Invalid
configuration SHALL fail before mutation, and conflicting queue definitions
SHALL be rejected rather than silently changing persisted policies.

#### Scenario: Late subscriber
- **WHEN** a subscription is registered after an event is emitted
- **THEN** it receives only subsequent events

#### Scenario: Concurrent registration and emission
- **WHEN** registration races emission
- **THEN** the subscription is either included once or excluded according to their atomic ordering

### Requirement: Each subscription acknowledges independently
An acknowledgement SHALL resolve only its own event/subscription obligation.
Repeated acknowledgements SHALL be harmless and SHALL NOT decrement another
recipient's obligation. Delivery SHALL be at-least-once until acknowledgement,
removal, or event expiry; handler side effects are not guaranteed exactly once.

#### Scenario: Independent progress
- **WHEN** one of two subscriptions acknowledges an event
- **THEN** the event stays active for the other subscription and the first receives no further delivery

### Requirement: Delivery ownership is fenced and recoverable
A delivery attempt SHALL have an expiring ownership token. A subscription's
workers SHALL NOT concurrently acquire a currently leased delivery. Expired or
released deliveries SHALL become available again with a new token; stale tokens
SHALL NOT acknowledge, renew, or release a newer attempt.

#### Scenario: Worker crash and late acknowledgement
- **WHEN** an attempt expires and another worker acquires the same delivery
- **THEN** the old attempt cannot acknowledge the new attempt's obligation

#### Scenario: Managed handler
- **WHEN** a managed handler succeeds
- **THEN** its delivery is acknowledged while ownership renewal protects the running handler
- **AND** renewal failure interrupts managed work without reporting success

### Requirement: Stored data errors remain visible
Unsupported record versions, corrupt storage, schema mismatches, and Redis
failures SHALL remain typed failures rather than appearing as an empty queue or
successful acknowledgement.

#### Scenario: Invalid stored payload
- **WHEN** a stored event cannot be decoded against the queue schema
- **THEN** reading or acquiring it fails explicitly
