## Purpose

Defines recurring scheduling as durable creation of idempotently identified queue tasks so process failure cannot silently lose an already-consumed tick.

## ADDED Requirements

### Requirement: Every due tick maps to one task identity
A scheduled definition SHALL derive a deterministic task identity from the schedule name and nominal tick time and SHALL offer that task idempotently.

#### Scenario: Two scheduler processes observe the same tick
- **WHEN** two processes attempt to materialize the same due tick
- **THEN** exactly one task generation exists for that schedule and tick identity

### Requirement: Tick execution uses queue delivery semantics
Scheduled handlers SHALL run through the managed queue worker and SHALL inherit its at-least-once delivery, lease, retry, retention, and observability behavior.

#### Scenario: Scheduler process dies after offering
- **WHEN** the scheduler process dies after the tick task is stored but before a worker executes it
- **THEN** the task remains eligible for normal queue processing

### Requirement: Missed-tick policy is explicit
A schedule SHALL declare whether startup skips, coalesces, or backfills ticks missed while no scheduler was running, including a maximum backfill count.

#### Scenario: Coalescing missed ticks
- **WHEN** a coalescing schedule restarts after multiple nominal ticks were missed
- **THEN** it creates one task representing the documented coalesced interval

### Requirement: Scheduler API does not claim exactly-once execution
Scheduler documentation SHALL distinguish idempotent tick creation from at-least-once handler execution.

#### Scenario: Tick task is retried
- **WHEN** the worker loses its lease after beginning a scheduled handler
- **THEN** the same tick task may execute again under normal retry semantics
