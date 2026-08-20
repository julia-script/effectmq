## Purpose

Defines the compatibility, verification, packaging, documentation, security, and publication evidence required before effectmq is presented as production-ready.

## ADDED Requirements

### Requirement: Compatibility matrix is tested
The release SHALL publish and continuously test its supported Node.js, Redis, Effect, RESP, and Redis-client versions and topologies.

#### Scenario: Supported matrix entry
- **WHEN** a version combination is listed as supported
- **THEN** its build, consumer typecheck, integration suite, and fault smoke tests pass in CI

### Requirement: Candidate uses the current Effect beta baseline
Immediately before cutting the candidate, the release SHALL resolve npm's
`beta` dist-tag for `effect` and every direct `@effect/*` dependency, record the
exact resolved versions, update the peer minimum and development dependencies,
and rerun all release gates. A newer `rc` dist-tag SHALL NOT replace the beta
baseline unless the release plan is explicitly changed.

#### Scenario: Effect beta has advanced
- **WHEN** npm's `beta` dist-tag differs from the versions in the candidate manifest
- **THEN** the manifest, lockfile, documentation, and packed-consumer fixture are updated to the resolved beta versions
- **AND** the full correctness, compatibility, package, benchmark, and soak gates pass again

### Requirement: Correctness and fault gates pass
Release CI SHALL pass reference-state-model/property tests, crash and lease-loss tests, Redis restart and Sentinel failover tests, script-cache-loss tests, clock-skew tests, and mixed-version upgrade/rollback tests.

#### Scenario: Release commit is published
- **WHEN** a package version is eligible for publication
- **THEN** every required correctness and fault job has succeeded for that exact commit

### Requirement: Performance bounds are published
The release SHALL publish reproducible throughput and p50/p95/p99 latency measurements across documented backlog, payload, concurrency, and expiry-sweep sizes, including the maximum configured atomic batch.

#### Scenario: Adversarial backlog benchmark
- **WHEN** the benchmark runs with a large due-task and expired-lease backlog
- **THEN** no single atomic queue operation exceeds the documented work bound

### Requirement: Packed package is consumer-tested
CI SHALL build and pack the package, assert the intended file list and generated-artifact drift, and install the tarball into representative ESM consumer projects that exercise every public export.

#### Scenario: Experimental source is present in the tarball
- **WHEN** the packed file list includes scratchpad, test, or unintended generated declarations
- **THEN** the release gate fails

### Requirement: Release documentation is complete
The release SHALL include accurate delivery guarantees, task-relationship language, supported topology and compatibility tables, upgrade/rollback and operations guides, security policy, contribution guide, and pre-1.0 compatibility policy.

#### Scenario: Exactly-once wording remains
- **WHEN** release documentation validation finds an exactly-once handler-execution claim
- **THEN** the release gate fails

### Requirement: Publication is provenance-bearing and gated
Publishing SHALL depend on the successful release gate for the exact commit and SHALL use npm trusted publishing with generated provenance rather than a long-lived publication token.

#### Scenario: Release gate has not passed
- **WHEN** the publication workflow runs for a commit without a successful release gate
- **THEN** no npm package is published
