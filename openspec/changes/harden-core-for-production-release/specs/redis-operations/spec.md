## Purpose

Defines the supported Redis deployment contract and operational bounds needed to run the queue safely through restarts, failover, backlog spikes, and retention cleanup.

## ADDED Requirements

### Requirement: Supported Redis topologies are explicit
The release SHALL support standalone Redis and Sentinel-managed non-sharded primary/replica deployments. Redis Cluster SHALL be documented as unsupported.

#### Scenario: Cluster configuration is supplied
- **WHEN** a user attempts to configure Redis Cluster
- **THEN** startup fails with an unsupported-topology error before processing tasks

### Requirement: Application scripts recover after cache loss
Queue scripts SHALL be content-addressed, loaded into the application script cache, invoked by digest, and transparently reloaded once when Redis reports a missing script.

#### Scenario: Redis script cache is flushed
- **WHEN** the script cache is cleared between queue operations
- **THEN** the next operation reloads the expected script and retries without duplicating the transition

### Requirement: Mixed application versions do not replace each other's code
Starting one library version SHALL NOT globally replace the Redis-side implementation used by another running version.

#### Scenario: Rolling application upgrade
- **WHEN** old and new workers start in either order against the same supported Redis deployment
- **THEN** each invokes the script version matching its own storage protocol

### Requirement: Maintenance work is bounded
Lease recovery, delayed promotion, retention trimming, relationship release, and list inspection SHALL process configurable bounded batches and SHALL expose a continuation until work is complete.

#### Scenario: Thousands of leases expire together
- **WHEN** more leases expire than the configured sweep batch size
- **THEN** one atomic operation processes at most the configured batch
- **AND** remaining work stays discoverable for subsequent sweeps

### Requirement: Retention is configurable and observable
Task records, terminal results, dead-letter entries, and lifecycle events SHALL have configurable age/count retention. Health and metrics SHALL expose queue depth, oldest age, sweep lag, Redis errors, and retention failures.

#### Scenario: Event stream reaches its configured limit
- **WHEN** appending an event would exceed the configured retention window
- **THEN** old events are trimmed within the documented approximation
- **AND** consumers can determine the earliest resumable cursor

### Requirement: Deployment safety requirements are documented
The production guide SHALL define persistence, `noeviction`, backup/restore, replication data-loss windows, ACLs, TLS, timeouts, reconnection, graceful shutdown, and indeterminate-write behavior.

#### Scenario: Producer loses connection after sending an offer
- **WHEN** the producer cannot determine whether Redis committed the offer
- **THEN** the API returns an indeterminate-write error that instructs retry with the same idempotency identity
