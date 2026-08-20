## ADDED Requirements

### Requirement: Redis connection lifecycle is scope-safe
Every Redis connection used by the library SHALL be acquired and released within an Effect scope. Connection and close failures SHALL retain their semantic typed identity, while defects and interruption SHALL propagate unchanged.

#### Scenario: Pool scope closes normally
- **WHEN** the Redis pool scope closes after successful use
- **THEN** each owned client is closed exactly once

#### Scenario: Readiness command defects
- **WHEN** a readiness command terminates with a defect rather than an expected connection failure
- **THEN** the readiness operation preserves the defect instead of reporting the connection as merely unready

### Requirement: Redis replies are validated before use
Every Redis reply crossing into queue logic SHALL be validated against its expected scalar, tuple, collection, or stream shape. An unexpected reply SHALL fail with a typed invalid-reply error containing operation context and SHALL NOT be coerced or accepted through a type assertion.

#### Scenario: Stream reply has an invalid tuple
- **WHEN** a stream read returns an entry with a malformed field/value tuple
- **THEN** decoding fails with an invalid-reply error before an event is constructed

#### Scenario: Text conversion receives an unsupported value
- **WHEN** a Redis result cannot be converted using the explicitly supported text representations
- **THEN** the adapter fails with an invalid-reply error rather than stringifying the value implicitly
