## ADDED Requirements

### Requirement: Invalid schedule definitions fail predictably
Schedule construction SHALL validate missed-tick policy and backfill bounds before materialization. Predictably invalid definitions SHALL fail with a typed scheduler-configuration error and SHALL NOT throw or die.

#### Scenario: Maximum backfill is invalid
- **WHEN** a schedule declares a maximum backfill outside the supported range
- **THEN** construction fails with a scheduler-configuration error identifying the field and accepted range
- **AND** no due tick is evaluated or offered
