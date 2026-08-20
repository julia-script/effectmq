## Purpose

Defines a durable, lossless, versioned storage contract for typed task values and metadata across compatible releases and rolling deployments.

## ADDED Requirements

### Requirement: User values round-trip losslessly
Payloads, successes, and typed failures SHALL remain opaque to Redis-side logic and SHALL decode to the same supported value, including nested nulls, empty arrays/maps, Unicode, binary values, and safe numeric values.

#### Scenario: Typed failure contains nested nulls
- **WHEN** a typed failure containing nested null values is stored and read
- **THEN** the decoded failure is deeply equal to the encoded failure

### Requirement: Stored records carry a protocol version
Every task generation and event record SHALL identify its storage protocol version and task-schema identity. Readers SHALL reject unsupported versions with a typed compatibility error rather than mis-decoding them.

#### Scenario: New reader encounters an unsupported record
- **WHEN** a reader encounters a record with an unsupported protocol version
- **THEN** it fails with an error that identifies the encountered and supported versions

### Requirement: Corruption is never normalized into valid empty data
Malformed bytes, invalid field types, and structurally invalid collections SHALL fail decoding in the typed error channel. Only explicitly documented canonical representations may normalize to an equivalent value.

#### Scenario: Error history decodes to a map
- **WHEN** the stored error-history field contains a non-list value
- **THEN** decoding fails instead of returning an empty history

### Requirement: Rolling compatibility is declared
Each release SHALL declare which protocol versions it can read and write. A rolling upgrade SHALL either preserve a mutually readable write version or require an explicit migration before mixed-version workers start.

#### Scenario: Mixed-version deployment
- **WHEN** old and new supported workers run concurrently
- **THEN** every record they exchange uses a protocol version readable by both

### Requirement: Encoded values are size bounded
The producer and worker APIs SHALL reject payloads, results, failures, error histories, and relationship sets that exceed configured encoded-size or count limits before an unbounded Redis operation occurs.

#### Scenario: Oversized result
- **WHEN** a handler returns a result larger than the configured maximum
- **THEN** acknowledgement fails with a typed size-limit error
- **AND** the task follows the configured terminal handling policy
