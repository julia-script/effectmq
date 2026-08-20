## 1. Establish the Safety Baseline

- [x] 1.1 Split the current serialization work from Redis Functions so binary codec changes can be reviewed and tested independently
- [x] 1.2 Restore real packaged `.lua` sources and replace `FUNCTION LOAD REPLACE`/`FCALL` with a content-addressed binary-safe `SCRIPT LOAD`/`EVALSHA` runner
- [x] 1.3 Add `NOSCRIPT` cache-flush and mixed-library-version tests proving each process reloads only its own script content
- [x] 1.4 Add a failing regression test for a same-state waiting transition and lease renewal preserving state-index membership
- [x] 1.5 Repair the Lua transition primitive and verify every task occupies exactly one execution state after each transition
- [x] 1.6 Build a small in-memory reference state model for offer, acquire, renew, fail, succeed, expire, retain, release, and remove operations

## 2. Define Task Identity and Public Outcomes

- [x] 2.1 Add queue/id/generation identity types and persist a generation on every task and lifecycle event
- [x] 2.2 Introduce a schema-aware `TaskHandle<Success, Failure>` carrying generation identity and its authoritative event cursor
- [x] 2.3 Change `TaskQueue.offer` to return an explicit created-or-existing outcome with a task handle
- [x] 2.4 Make duplicate offers return the existing generation unchanged in delayed, waiting, leased, retry, and terminal states
- [x] 2.5 Add the explicit new-generation mode and prove no payload, ownership, history, relationship, or outcome fields leak from the prior generation
- [x] 2.6 Add typed indeterminate-write behavior and document retrying an offer with the same idempotency identity

## 3. Fence and Supervise Attempts

- [x] 3.1 Replace worker-wide lock identity with a unique opaque token generated for each acquired attempt
- [x] 3.2 Require the exact current token for renew, success, typed failure, and release transitions and return typed `LeaseLost` for stale tokens
- [x] 3.3 Track attempt number, handler-failure count, and stalled-attempt count as separate protocol fields
- [x] 3.4 Use Redis server time and a score-ordered active index for lease acquisition, renewal, and expiry recovery
- [x] 3.5 Implement finite default `maxStalledCount` handling and terminal built-in stalled failures without invoking the user error schedule
- [x] 3.6 Refactor managed processing into a scoped handler/heartbeat race that interrupts the handler when ownership cannot be established safely
- [x] 3.7 Add bounded heartbeat transport retry, graceful worker drain, and separate producer, worker, and maintenance Redis connections
- [x] 3.8 Test late success/failure acknowledgements, heartbeat interruption, process crash, event-loop pause, Redis restart, and clock-skew scenarios

## 4. Separate Task Relationships

- [x] 4.1 Replace implicit parent/child fields with immutable informational creator provenance captured from managed task context
- [x] 4.2 Remove `detached`, implicit `heldBy`, pin counts, and alive/done/dead lifecycle behavior from public and internal APIs
- [x] 4.3 Add the affirmative `retainResultUntil: "current-task-settles"` offer option and reject it outside a live managed holder context
- [x] 4.4 Store holds as set-idempotent holder-generation/retained-generation relationships and allow independent holders to retain an existing generation
- [x] 4.5 Make terminal settlement visible immediately while active holds postpone only record disposal
- [x] 4.6 Release a settled or removed holder's relationships through bounded durable continuation batches
- [x] 4.7 Make ordinary removal reject active holds and add a separately named administrative force-removal path
- [x] 4.8 Add replay, multiple-holder, settled-holder, holder-removal, and retained-task-removal tests proving no join, cancellation, or failure propagation

## 5. Version and Harden the Storage Boundary

- [x] 5.1 Define the v1 storage envelope, supported value domain, protocol version, queue schema identity, and canonical built-in error tags
- [x] 5.2 Implement one codec boundary for opaque task payloads, successes, typed failures, and binary Lua arguments/replies
- [x] 5.3 Preserve nested nulls, empty collections, Unicode, binary data, safe numeric values, and typed failures in round-trip tests
- [x] 5.4 Replace normalization fallbacks with typed corruption, schema-mismatch, and unsupported-protocol errors
- [x] 5.5 Enforce configurable encoded-size and count limits for payloads, outcomes, error history, and relationships
- [x] 5.6 Add committed golden byte fixtures and old-reader/new-writer compatibility tests for every declared rollable protocol pair
- [x] 5.7 Move production writes into the v1 key namespace and add a read-only pre-release data inspection/drain command

## 6. Make Waiting, Events, and Retention Reliable

- [x] 6.1 Publish versioned lifecycle events with stable event id, task generation, previous/new state, attempt data, and tag-specific opaque values
- [x] 6.2 Return the authoritative Redis event cursor atomically with offer and expose the earliest retained cursor
- [x] 6.3 Implement `TaskQueue.wait(handle)` as durable-state read, cursor subscription, and post-subscription recheck for the exact generation
- [x] 6.4 Return distinct typed task failure, task-not-found, result-expired, cursor-expired, protocol/schema, and caller-timeout outcomes
- [x] 6.5 Route `TaskQueue.execute` through the same offer-and-wait handle protocol
- [x] 6.6 Add configurable task-record, result, terminal-index, dead-letter, and event retention with approximate bounded stream trimming
- [x] 6.7 Test completion before wait, completion during subscription, event trimming, result expiry, generation replacement, reconnect, and corrupt events

## 7. Bound Maintenance and Inspection

- [x] 7.1 Convert expired-lease recovery and delayed-task promotion to score-ordered due queries with configurable batch limits
- [x] 7.2 Convert relationship release, terminal disposal, dead-letter cleanup, and retention trimming to bounded atomic batches with durable continuations
- [x] 7.3 Replace unbounded task/list inspection APIs with capped cursor pagination and documented ordering
- [x] 7.4 Ensure payload, history, result, relationship, and cascade limits are checked before invoking an unbounded Redis operation
- [x] 7.5 Add metrics and structured logs for depth, oldest age, sweep lag, due backlog, expired leases, ownership loss, retention failure, reload, and reconnect
- [x] 7.6 Benchmark adversarial due, expiry, retention, and relationship backlogs and assert that one script never exceeds its configured work bound

## 8. Make Scheduling Durable

- [x] 8.1 Redesign schedule definitions around stable name, rule, timezone, target queue, payload constructor, and missed-tick policy
- [x] 8.2 Derive a deterministic task id from schedule name and nominal tick time and materialize each due tick with idempotent `offer`
- [x] 8.3 Implement skip, coalesce, and bounded-backfill policies for missed ticks
- [x] 8.4 Run scheduled work exclusively through managed queue workers and remove direct/exactly-once handler-execution claims
- [x] 8.5 Test competing schedulers, crash before/after offer, scheduler downtime, timezone boundaries, retry, and duplicate handler execution

## 9. Complete the Redis Operations Contract

- [x] 9.1 Validate startup topology and fail fast with a typed unsupported-topology error for Redis Cluster
- [x] 9.2 Add and integration-test Sentinel discovery, primary failover, reconnection, and post-failover script reload behavior
- [x] 9.3 Install Node Redis error listeners before connection use and expose readiness, connection, and command health without secret leakage
- [x] 9.4 Provide TLS, ACL, timeout, reconnect-backoff, graceful-shutdown, and bounded pool configuration through scoped Effect Layers
- [x] 9.5 Write the operations runbook covering persistence, `noeviction`, backup/restore, replication loss windows, ACLs, TLS, capacity, failover, and indeterminate writes

## 10. Prove and Ship the Release

- [x] 10.1 Run generated property sequences against the reference model and Redis, checking one-state, one-owner, terminal, relationship, and bounded-work invariants after every operation
- [x] 10.2 Add deterministic fault injection at offer, acquire, heartbeat, acknowledgement, event, cleanup, reconnect, restart, and Sentinel failover boundaries
- [x] 10.3 Define and test the supported Node.js, Redis, Effect, RESP, Redis-client, standalone, and Sentinel compatibility matrix
- [x] 10.4 Publish reproducible throughput and p50/p95/p99 latency benchmarks across payload, concurrency, backlog, and sweep-batch sizes
- [x] 10.5 Update API reference, delivery-guarantee guide, idempotency guide, task-relationship explanation, scheduler guide, support policy, upgrade/rollback guide, security policy, and contribution guide
- [x] 10.6 Remove exactly-once, parent/child, death, and pinning language that conflicts with the production domain model
- [x] 10.7 Make formatting, typechecking, linting, Lua validation, unit/integration/fault tests, generated-artifact drift, and documentation checks required CI gates
- [x] 10.8 Build and pack the tarball, reject scratch/test/unintended files, and install it into representative ESM consumers that exercise every public export
- [x] 10.9 Add a release-candidate soak using the maximum supported batch limits and verify metrics, memory, Redis latency, and shutdown behavior
- [x] 10.10 Resolve npm's current `beta` dist-tag and update `effect`, every direct `@effect/*` dependency, the peer minimum, lockfile, documentation, and packed-consumer fixture; rerun all release gates
- [ ] 10.11 Configure protected npm trusted publishing with provenance and require the exact-commit release gate before publication
- [ ] 10.12 Publish the compatibility and performance evidence, cut the release candidate, complete the rollback rehearsal, and only then mark the release production-ready
