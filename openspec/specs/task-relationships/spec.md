# task-relationships

## Purpose

Separates creator provenance, explicit result retention, and execution
dependency so nested offers do not silently acquire workflow-like semantics.

## Requirements

### Requirement: Creator provenance is informational

A task offered from a managed handler SHALL record the running task as its
creator. Provenance SHALL NOT retain results, order execution, join, cancel, or
propagate failure.

#### Scenario: Nested offer
- **WHEN** task A's handler offers task B
- **THEN** B records A as creator and both execute independently

### Requirement: Result retention is explicit and set-idempotent

A producer SHALL explicitly request a result-retention hold. A hold is
identified by holder generation and retained generation; replaying it has no
additional effect while a different holder acquires independently.

#### Scenario: Two holders
- **WHEN** two live task generations retain the same terminal generation
- **THEN** each relationship independently prevents result disposal

### Requirement: Retention holders are live

A hold SHALL be acquired only for an existing unsettled holder. Failed
validation SHALL leave the task and all relationships unchanged.

#### Scenario: Settled holder
- **WHEN** a settled holder requests retention
- **THEN** the offer fails without acquiring a hold

### Requirement: Removal respects active holds

Ordinary removal SHALL reject active incoming holds. Holder settlement or
removal SHALL release owned holds in bounded resumable batches. A separately
named administrative force-removal operation MAY revoke relationships.

#### Scenario: Retained terminal result
- **WHEN** ordinary removal targets a terminal generation with an active hold
- **THEN** removal fails and the result remains readable

### Requirement: Retention does not create execution dependency

The engine SHALL NOT delay holder settlement, cancel retained work, join tasks,
or translate failures solely because a retention relationship exists.

#### Scenario: Holder settles first
- **WHEN** a holder settles while retained work remains runnable
- **THEN** its hold releases and the retained task continues independently
