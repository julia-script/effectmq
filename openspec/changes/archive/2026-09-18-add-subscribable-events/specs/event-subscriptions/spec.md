## Purpose

Defines durable named recipients for application events, including registration,
worker sharing, disconnect persistence, and explicit removal.

## ADDED Requirements

### Requirement: Subscription registration is durable and idempotent
A subscription SHALL be identified by queue and name. Repeated registration
SHALL return the same active generation without resetting deliveries. Multiple
workers using that subscription SHALL share its deliveries. Disconnecting a
worker SHALL NOT remove the subscription or waive its obligations.

#### Scenario: Concurrent registration
- **WHEN** two workers register the same queue and subscription name
- **THEN** both receive the same generation and one recipient obligation is created per subsequent event

#### Scenario: Restart
- **WHEN** a worker reconnects using an existing subscription name
- **THEN** its outstanding deliveries remain available

#### Scenario: Queue isolation
- **WHEN** the same subscription name is registered in different queues
- **THEN** each subscription has independent deliveries and identity

### Requirement: Explicit removal waives outstanding obligations
Removal SHALL exclude the subscription from subsequent emissions and prevent
further delivery to its generation. Outstanding obligations SHALL be recorded
as waived rather than acknowledged. Settlement of a large backlog SHALL be
resumable through bounded maintenance; inspecting an event SHALL reflect its
resolved obligations immediately.

#### Scenario: Removal completes an event
- **WHEN** the only remaining recipient subscription is removed
- **THEN** inspecting or maintaining the event settles it as completed according to queue retention policy

### Requirement: Reusing a removed name creates a new generation
Registering a removed name SHALL create a distinct generation that receives
only future events. Stale removal or acknowledgement requests SHALL NOT mutate
the replacement subscription or its deliveries.

#### Scenario: Old worker reconnects after recreation
- **WHEN** a subscription is removed and recreated with the same name
- **THEN** the old handle cannot remove the new subscription or acknowledge its events
