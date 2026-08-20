## Why

`effectmq` has a strong Effect-native core, but its current task state machine, implicit child-task retention, lease ownership, Redis maintenance work, storage format, and release process do not yet support an honest production-readiness claim. The next release should freeze a small, explicit correctness contract before adding more queue features or stabilizing the public API.

## What Changes

- **BREAKING**: define delivery as at-least-once and remove all exactly-once claims; handlers must be safe to repeat.
- **BREAKING**: replace process-wide worker ownership with a unique fenced lease per task attempt; a lost lease interrupts the handler and rejects all late acknowledgements.
- Fix the task state machine so every task has one valid execution state, same-state transitions are lossless, duplicate offers are state-aware, and stalled attempts consume a bounded failure budget.
- **BREAKING**: stop treating every task offered inside a handler as an implicit lifecycle child. Keep automatic creator provenance, but make result retention an explicit relationship with no implied ordering, cancellation, or failure propagation.
- Add a replay-safe result handle: awaiting checks durable terminal state, subscribes from an authoritative Redis cursor, and rechecks to close the race. Define result-expired, task-not-found, timeout, and terminal-failure outcomes.
- Keep Lua in real `.lua` files, but retain a binary-safe `SCRIPT LOAD`/`EVALSHA` runner with `NOSCRIPT` recovery rather than loading an unversioned Redis Function library from every worker startup.
- Introduce a lossless, versioned storage/codec envelope with migration and mixed-version rules; user payloads, successes, and failures remain opaque to Lua.
- Bound every atomic Redis operation, lease/delay sweep, reference-release cascade, list query, and event stream; add configurable task/result/event retention.
- Explicitly support standalone Redis and Sentinel-managed non-sharded deployments for this release; Redis Cluster remains unsupported.
- Make scheduler semantics honest and durable by representing each tick as an idempotently keyed queued task instead of advancing schedule state before running an untracked handler.
- Add production release gates: state-model and fault tests, Redis restart/failover and mixed-version tests, benchmarks, a Node/Redis/Effect compatibility matrix, operational documentation, packed-consumer smoke tests, provenance-bearing npm publication, and a clean generated artifact.
- Resolve the npm `beta` dist-tag for `effect` and every direct `@effect/*` dependency immediately before the candidate, record the exact baseline, and rerun the full release suite against it.

## Capabilities

### New Capabilities

- `task-delivery-safety`: At-least-once delivery, task-state invariants, fenced per-attempt leases, supervised heartbeats, bounded stalled recovery, and state-aware duplicate offers.
- `task-relationships`: Creator provenance, explicit result-retention holds, replay-safe result handles, and the deliberate absence of implicit child execution semantics.
- `storage-protocol`: A lossless, versioned codec envelope and its compatibility, migration, corruption, and size-limit behavior.
- `redis-operations`: Script loading/recovery, supported Redis topologies, bounded maintenance, retention, health, and operational safety requirements.
- `scheduler-delivery`: Durable, idempotent scheduled-task creation with at-least-once queue execution.
- `production-release`: Compatibility, verification, packaging, documentation, security, and publication gates for the production release.

### Modified Capabilities

- `task-pinning`: Replace automatic handler-context pinning and the alive/done/dead disposal graph with explicit result-retention holds; provenance no longer implies retention.
- `task-events-stream`: Add bounded retention, authoritative cursors, replay windows, and race-safe terminal-result waiting.
- `task-retry-policy`: Count lease loss/stalls within a bounded attempt policy and distinguish handler failure from ownership loss.
- `task-completion-policies`: Apply disposal policies after terminal settlement subject only to explicit result-retention holds, with named result expiry behavior.

## Impact

- Public APIs: `TaskQueue.offer`, `complete`, `wait`, and `execute`; a new task/result handle and lease-loss error; `detached` is removed or replaced by an affirmative retention option; scheduler construction becomes queue/task based.
- Engine and storage: `TaskEngine`, Lua scripts, Redis keys, state fields, lease tokens, event streams, task/result retention, codec envelopes, and migration/version checks.
- Redis boundary: `RedisPool` and `NodeRedisPool` gain a binary-safe cached script runner and Sentinel-aware configuration; Redis Functions and Redis Cluster are not part of this release contract.
- Tests and tooling: reference-model/property tests, fault injection, local and containerized Redis matrices, mixed-version fixtures, benchmarks, package-consumer tests, generation drift checks, and Effect-native test Layers.
- Documentation and release: README delivery claims, deployment/runbooks, support policy, package metadata, CI/release workflow, npm trusted publishing, and a domain glossary.
