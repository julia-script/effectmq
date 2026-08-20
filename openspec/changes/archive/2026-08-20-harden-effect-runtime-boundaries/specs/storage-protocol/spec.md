## ADDED Requirements

### Requirement: Untrusted keyed data is prototype safe
Collections populated from externally controlled keys SHALL use a representation that cannot mutate or inherit JavaScript object prototypes. Key values such as `__proto__`, `constructor`, and `prototype` SHALL be preserved as ordinary data or rejected by an explicit schema rule.

#### Scenario: External key is __proto__
- **WHEN** a Redis field or decoded record contains the key `__proto__`
- **THEN** processing does not alter the collection's prototype
- **AND** the key is handled according to the collection's documented data semantics
