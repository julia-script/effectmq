# task-relationships

## Purpose

Separates task creator provenance, explicit result retention, and execution
dependency so nested offers do not silently acquire workflow-like lifecycle
semantics.

## Requirements

### Requirement: Creator provenance is automatic and informational

A task offered from a managed task handler SHALL record the running task as its
creator. Creator provenance SHALL NOT retain results, order execution,
propagate cancellation, or propagate failure.

#### Scenario: Nested offer records its creator
- **WHEN** task A's handler offers task B
- **THEN** B records A as its creator
- **AND** A's completion has no effect on B's execution

### Requirement: Result retention is explicit

A producer SHALL explicitly request a result-retention hold when it needs a
terminal task record to remain readable until a named holder settles. Nested
offers SHALL NOT create such a hold by default.

#### Scenario: Nested offer without retention
- **WHEN** task A offers task B without a retention option
- **THEN** B has creator provenance for A but no retention hold owned by A

#### Scenario: Explicit retention
- **WHEN** task A offers task B with a result-retention hold owned by A
- **THEN** B's terminal result remains readable until A settles or explicitly releases the hold

### Requirement: Retention holds are set-idempotent

A retention hold SHALL be identified by holder and retained task generation.
Reacquiring the same hold SHALL have no additional effect, while a different
holder SHALL acquire an independent hold.

#### Scenario: Replay reacquires the same hold
- **WHEN** a replaying holder repeats the same retained offer
- **THEN** exactly one hold exists for that holder and task generation

#### Scenario: Second holder retains an existing task
- **WHEN** another live holder explicitly retains the same task generation
- **THEN** both holders independently prevent result disposal

### Requirement: Retention holders must be live

A retention hold SHALL be acquired only for an existing holder that has not
settled. Failure to validate any requested holder SHALL leave the retained task
and all relationships unchanged.

#### Scenario: Settled holder requests retention
- **WHEN** a retained offer names a holder that has already settled
- **THEN** the offer fails without acquiring a hold

### Requirement: Removal respects active holds

Removing a task generation with active retention holds SHALL fail unless an
explicit administrative force mode is used. Settling or removing a holder SHALL
release every hold owned by that holder in bounded, resumable batches.

#### Scenario: Remove retained result
- **WHEN** a terminal task has an active retention hold and ordinary removal is requested
- **THEN** removal fails and the result remains readable

#### Scenario: Holder removal releases retention
- **WHEN** a holder is removed before normal settlement
- **THEN** its retention holds are eventually released exactly once

### Requirement: Retention does not create execution dependency

The library SHALL NOT delay a holder's completion, cancel a retained task, or
translate a retained task's failure solely because a retention hold exists.

#### Scenario: Holder settles before retained task
- **WHEN** holder A settles while retained task B is still runnable
- **THEN** A's hold is released and B continues independently

### Requirement: Child terminology is reserved

Public documentation SHALL use creator, spawned task, retention hold, and
execution dependency for their distinct meanings. It SHALL use child task only
for a future relationship that actually defines structured lifecycle behavior.

#### Scenario: API documentation for nested offer
- **WHEN** a user reads the nested-offer documentation
- **THEN** it describes provenance and optional retention without promising parent/child cancellation, joining, or failure propagation
