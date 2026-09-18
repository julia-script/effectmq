# storage-protocol

## Purpose

Defines a durable, lossless, versioned storage contract for typed task values
and metadata across compatible releases and rolling deployments.

## Requirements

### Requirement: User values round-trip losslessly

Payloads, successes, typed failures, and custom progress values SHALL remain
opaque to Redis-side logic and SHALL decode to the same supported value,
including nested nulls, empty arrays/maps, Unicode, binary values, and safe
numeric values. Progress values SHALL use a distinct value kind and the task's
schema identity within the versioned storage contract.

#### Scenario: Typed failure contains nested nulls
- **WHEN** a typed failure containing nested null values is stored and read
- **THEN** the decoded failure is deeply equal to the encoded failure

#### Scenario: Progress contains non-JSON values
- **WHEN** custom progress containing nested nulls, binary data, and empty collections is stored and read
- **THEN** it round-trips without losing those values

#### Scenario: Progress schema identity mismatch
- **WHEN** a reader uses a task schema identity different from the stored progress identity
- **THEN** it fails with a typed schema-identity error rather than interpreting the bytes against a different schema

### Requirement: Stored records carry a protocol version

Every task generation and event record SHALL identify its storage protocol
version and task-schema identity. Readers SHALL reject unsupported versions
with a typed compatibility error rather than mis-decoding them.

#### Scenario: New reader encounters an unsupported record
- **WHEN** a reader encounters a record with an unsupported protocol version
- **THEN** it fails with an error that identifies the encountered and supported versions

### Requirement: Corruption is never normalized into valid empty data

Malformed bytes, invalid field types, structurally invalid collections,
serializer exceptions, and invalid byte conversions SHALL fail decoding in the
typed error channel. These failures SHALL NOT escape as defects or be
normalized into valid-looking data. Only explicitly documented canonical
representations may normalize to an equivalent value.

#### Scenario: Error history decodes to a map
- **WHEN** the stored error-history field contains a non-list value
- **THEN** decoding fails instead of returning an empty history

#### Scenario: MessagePack input is truncated
- **WHEN** stored MessagePack bytes end before a declared value is complete
- **THEN** decoding fails with a typed storage-decoding error
- **AND** no synchronous serializer exception escapes the Effect

#### Scenario: Encoded input is not a supported byte representation
- **WHEN** an external value cannot be converted to the required byte representation
- **THEN** conversion fails with a typed storage-decoding error rather than a defect

### Requirement: Rolling compatibility is declared

Each release SHALL declare which protocol versions it can read and write. A
rolling upgrade SHALL either preserve a mutually readable write version or
require an explicit migration before mixed-version workers start.

#### Scenario: Mixed-version deployment
- **WHEN** old and new supported workers run concurrently
- **THEN** every record they exchange uses a protocol version readable by both

### Requirement: Encoded values are size bounded

The producer and worker APIs SHALL reject payloads, results, failures, custom
progress values, error histories, and relationship sets that exceed configured
encoded-size or count limits before an unbounded Redis operation occurs.
Custom progress SHALL respect the task's encoded-value byte limit. This
per-value limit SHALL apply even when the history has no entry-count limit.

#### Scenario: Oversized result
- **WHEN** a handler returns a result larger than the configured maximum
- **THEN** acknowledgement fails with a typed size-limit error
- **AND** the task follows the configured terminal handling policy

#### Scenario: Oversized progress with unlimited history
- **WHEN** a task has no history count limit but emits progress larger than its encoded-value limit
- **THEN** the append fails with a typed size-limit error and does not store that progress value

### Requirement: Untrusted keyed data is prototype safe

Collections populated from externally controlled keys SHALL use a representation that cannot mutate or inherit JavaScript object prototypes. Key values such as `__proto__`, `constructor`, and `prototype` SHALL be preserved as ordinary data or rejected by an explicit schema rule.

#### Scenario: External key is __proto__
- **WHEN** a Redis field or decoded record contains the key `__proto__`
- **THEN** processing does not alter the collection's prototype
- **AND** the key is handled according to the collection's documented data semantics

### Requirement: History count limits are opt-in and trim oldest entries

Task history SHALL have no entry-count limit by default. A definition SHALL
allow an explicit positive integer entry-count limit or null for no limit.
The effective limit SHALL be persisted at generation creation. Both custom
progress and lifecycle entries SHALL count toward the same limit. Each append
to a limited history SHALL retain the newest entries and remove enough oldest
entries that the count is at most the configured limit when it returns.
Reaching the limit SHALL NOT reject otherwise valid progress or prevent a
task's lifecycle transition. Unlimited history SHALL NOT be implicitly capped
by the queue-wide lifecycle event limit or time-based event retention.

#### Scenario: Default history grows
- **WHEN** an enabled task emits more entries than the queue-wide lifecycle event limit without configuring a history limit
- **THEN** its per-task history keeps those entries until task-record disposal

#### Scenario: Configured limit is reached
- **WHEN** a valid entry is appended to a history already at its explicit count limit
- **THEN** the new entry is retained, the oldest entry is removed, and the count stays within the limit
- **AND** readers can detect the truncation

#### Scenario: Lifecycle transition at capacity
- **WHEN** a retained enabled task settles with its history at the configured count limit
- **THEN** settlement succeeds and its latest lifecycle entries replace the oldest entries according to the same limit

#### Scenario: Invalid count limit
- **WHEN** an operation first consumes a definition with a history limit that is zero, negative, fractional, non-finite, or outside the supported safe-integer range
- **THEN** definition-invariant validation rejects it before external work

#### Scenario: Duplicate offer supplies a different limit
- **WHEN** an offer returns an existing generation and its definition now supplies a different history limit
- **THEN** the existing generation retains the limit persisted when it was created
