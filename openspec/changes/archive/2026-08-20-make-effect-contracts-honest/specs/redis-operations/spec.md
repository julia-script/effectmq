## ADDED Requirements

### Requirement: Redis failures are translated semantically
The Redis boundary SHALL translate expected transport, script, protocol, relationship-limit, and indeterminate-commit conditions into stable tagged errors with structured reasons. Queue policy SHALL branch only on those semantic errors and SHALL NOT inspect Redis messages or recursively stringify causes.

#### Scenario: Relationship limit is reached
- **WHEN** an atomic storage operation rejects a relationship because its configured limit is reached
- **THEN** the queue receives a relationship-limit failure with structured limit context

#### Scenario: Commit outcome is unknown
- **WHEN** the connection is lost after a mutating command may have reached Redis
- **THEN** the operation fails with an indeterminate-write error carrying the operation and retry identity

#### Scenario: Redis diagnostic wording changes
- **WHEN** a Redis client changes the human-readable wording or cause nesting of an equivalent failure
- **THEN** the queue's recovery decision remains unchanged
