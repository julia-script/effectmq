## Purpose

Defines application event completion, optional delivery deadlines, and deletion
or archival after events become terminal.

## ADDED Requirements

### Requirement: All resolved obligations complete an event
An event SHALL complete when every captured subscription has acknowledged or
been explicitly removed. An emission with zero subscriptions SHALL complete
immediately. Completed events SHALL no longer be delivered.

#### Scenario: Empty recipient set
- **WHEN** an event is emitted without registered subscriptions
- **THEN** it completes immediately and follows its queue's retention policy

### Requirement: Expiration is optional and independent of retention
Events SHALL support an optional deadline, including a queue default and a
per-emission override that can disable expiration. Without a deadline an event
SHALL remain active indefinitely while obligations remain. Expiration SHALL
settle an unfinished event as expired and prevent further delivery or valid
acknowledgement. Expiration SHALL use authoritative storage time, including
when racing an acknowledgement.

#### Scenario: Indefinite wait
- **WHEN** an event has no deadline and a subscription remains disconnected
- **THEN** elapsed time alone does not settle or delete the event

#### Scenario: Expired delivery
- **WHEN** the deadline has passed with an obligation outstanding
- **THEN** inspection, acquisition, acknowledgement, or maintenance observes an expired terminal event

### Requirement: Terminal events follow queue retention policy
Queues SHALL select full event deletion or archival on settlement. Archived
records SHALL retain the payload, completion or expiration outcome, and recipient
statuses. Archive retention SHALL be independent of the delivery deadline and
support indefinite retention or a finite duration. Archive inspection and listing
SHALL be available. Maintenance SHALL eventually remove finite-retention archives
and all event-specific delivery indexes for deleted events.

#### Scenario: Delete after final acknowledgement
- **WHEN** the last obligation is resolved for a delete-policy event
- **THEN** its event record and delivery indexes are removed

#### Scenario: Inspect archived expiration
- **WHEN** an archive-policy event expires
- **THEN** it remains inspectable with expired outcome and outstanding recipient statuses until archive retention ends
