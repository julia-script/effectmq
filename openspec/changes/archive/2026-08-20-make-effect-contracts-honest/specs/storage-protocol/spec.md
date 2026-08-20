## MODIFIED Requirements

### Requirement: Corruption is never normalized into valid empty data
Malformed bytes, invalid field types, structurally invalid collections, serializer exceptions, and invalid byte conversions SHALL fail decoding in the typed error channel. These failures SHALL NOT escape as defects or be normalized into valid-looking data. Only explicitly documented canonical representations may normalize to an equivalent value.

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
