# Contributing

Open an issue or design discussion before a large protocol or public API change.
Correctness changes should include the smallest failing test first and preserve
the v1 storage, generation identity, lease fencing, bounded-work, and
at-least-once contracts.

Install the pinned toolchain and run the release gates:

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test
pnpm build
pnpm verify:package
```

Integration tests use Docker by default. The restart and Sentinel fault tests
use local `redis-server` processes and are enabled with:

```sh
EFFECTMQ_TEST_REDIS=local EFFECTMQ_TEST_SENTINEL=local pnpm test
```

Effectful tests use `@effect/vitest`: use `it.effect` for isolated Effects and
suite-scoped Layers for shared services. Do not call `Effect.runPromise` from a
test or keep a module-level `ManagedRuntime`. Test resources must be acquired
with `Effect.acquireRelease`; the lifecycle gate fails on leaked tracked
resources or a finalizer timeout. Run `pnpm typecheck:test` when changing any
test or `src/testing` support file.

Use `TestClock` plus `Deferred`/latches for unit-level time and concurrency.
Only Redis-owned TTL/restart/failover tests use real time; label those suites
and prefer bounded polling with diagnostic timeouts over fixed sleeps.

Production modules import supported narrow `effect/*` subpaths. Public
Effect-returning functions pin exact success, error, and service channels and
use `Effect.fnUntraced` for reusable generator implementations. Services are
`Context.Service` classes with `@effectmq/core/<Service>` identifiers; optional
fiber-local values are `Context.Reference`s. Use `TaskEngine.layer()` when the
application supplies `RedisPool`. Use `NodeLive.layer()` for the complete Node
graph.

Edit `src/lua/taskEngine.lua`, then run `pnpm gen:lua`; never hand-edit the
generated TypeScript module. CI rejects generated drift. Add committed golden
fixtures for any declared storage compatibility pair and property/fault tests
for state-machine changes.

Document user-visible behavior in the appropriate guide and update the support
matrix for dependency/platform changes. Add a Changeset for publishable
changes. Keep commits focused; do not include local databases, benchmark
scratch files, test output, editor metadata, secrets, certificates, or packed
tarballs.
