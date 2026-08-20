---
"@effectmq/core": minor
---

Harden the queue protocol and runtime for a first production candidate.

This is a breaking pre-1.0 storage and API release. Existing pre-v1 Redis data
must be drained before upgrading; the package includes a read-only inspector.

- Add generation-specific task handles, explicit created/existing offer
  outcomes, safe duplicate behavior, and deliberate new-generation offers.
- Fence every attempt with a unique lease token, separate handler failures from
  stalled attempts, use Redis server time, supervise heartbeats, and add scoped
  bounded-concurrency workers with graceful drain.
- Replace implicit task lifecycle relationships with informational creator
  provenance and explicit set-idempotent result-retention holds.
- Add the versioned, bounded MessagePack storage protocol with typed corruption,
  compatibility, schema, size, and count failures.
- Load real packaged Lua through content-addressed `SCRIPT LOAD` / `EVALSHA`
  with binary-safe arguments and `NOSCRIPT` recovery. Redis Functions are not
  used and mixed application versions do not replace one another's scripts.
- Make events, handle-based waiting, retention, inspection, and maintenance
  bounded and race-safe, with metrics for queue and Redis health.
- Redesign scheduling as durable idempotent tick-task materialization with
  skip, coalesce, and bounded-backfill policies. Handler execution remains
  at-least-once.
- Support standalone Redis and Sentinel with explicit TLS/ACL/timeout/pool
  configuration and fail startup for Redis Cluster.
- Add compatibility, property, fault, restart/failover, package-consumer,
  benchmark, soak, documentation, and provenance-bearing release gates.
- Refresh `effect` and direct `@effect/*` development dependencies to the npm
  `beta` baseline resolved for the candidate (`4.0.0-beta.107`).
- Require Node.js 22.19 or newer, matching the runtime floor of the verified
  Effect beta platform stack; Node.js 20 is not part of the support contract.
