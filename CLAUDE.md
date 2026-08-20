# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`@effectmq/core` — a Redis-backed task queue built on **Effect 4 beta** (pinned to `effect@4.0.0-beta.107`; it does not work with stable Effect 3.x). Typed payloads/results/errors via schemas, with retries, delays, idempotency, and cron schedules. Single package, pnpm, ESM (`"type": "module"`, `nodenext` resolution — internal imports use `.js` extensions).

## Commands

```bash
pnpm install                      # install (pnpm@10.x, see packageManager)
pnpm build                        # tsc -> dist/ (tests and src/testing are excluded from the build)
pnpm typecheck                    # strict production + complete test typecheck
pnpm typecheck:src                # production sources only
pnpm typecheck:test               # all tests and src/testing support
pnpm lint                         # Biome lint across the repository
pnpm check                        # formatting, lint, types, architecture, Lua, docs, release
pnpm lint:fix                     # biome check --write --unsafe
pnpm test                         # vitest run (integration tests, needs Docker — see below)
pnpm test:watch                   # vitest watch mode
pnpm vitest run src/TaskQueue.test.ts        # single test file
pnpm vitest run -t "test name substring"     # single test by name
```

### Tests need Docker

Effectful tests use `@effect/vitest`. Redis integration suites install the suite-scoped `TestLayer` from `src/testing/redisLayer.ts`; it acquires and releases containers, clients, child processes, listeners, and temporary directories through Effect scopes. Never add a module-level `ManagedRuntime`, direct `Effect.runPromise` test runner, or top-level resource warming. Container acquisition has an explicit 60s hook timeout and normal tests have a 30s timeout (`vitest.config.ts`). The root `docker-compose.yml` Redis is for manual/local experimentation only — tests don't use it.

Use `it.effect` for Effectful unit tests and `layer(...)(..., (it) => ...)` for suites with dependencies. Unit-level timing uses `TestClock` and a `Deferred`/latch before advancing time. Tests that exercise Redis TTL, restart, or Sentinel failover are explicitly labeled “real Redis time,” exclude Effect test services, and use bounded polling with diagnostic timeouts rather than fixed sleeps. Pure value/schema tests remain ordinary Vitest tests with explicit assertions.

### Releases

Changesets-based: run `pnpm changeset` to record a change; the release workflow versions and publishes (`pnpm release` = build + `changeset publish`). Add a changeset for any user-facing change.

## Architecture

Flat `src/` with a strict layering, top to bottom:

- **`TaskQueue.ts`** — the high-level API users live in: `make` (bind a queue name to a task definition), `offer` (enqueue), `complete` (take → run handler → report outcome, applying the task's retry schedule on failure), `stream` (typed lifecycle events), `wait` / `execute` (await a task's terminal result). Decodes engine tasks/events against the task's schemas.
- **`Scheduler.ts`** — cron-driven durable task materialization. Competing processes idempotently offer the same tick task; managed workers execute it with at-least-once delivery.
- **`Task.ts`** — `Task.make`: the schema-bearing task definition (payload/success/error schemas, `idempotencyKey` — which *is* the task id, so same key = same task — `retry` as an Effect `Schedule`, `maxRetries` default 5, `null` = unbounded).
- **`TaskEngine.ts`** (the big one, ~1000 lines) — the low-level `Context.Service` implementing queue primitives through the atomic script generated from `src/lua/taskEngine.lua`: create/take/writeSuccess/writeError, lock extend/remove, delayed + cron schedule state. Tasks move between Redis indexes named `wait`, `scheduled`, `active`, `failed`, and `success`. Every state change publishes to a per-queue Redis Stream (`<prefix>:<name>:events`, via `XADD`); `stream` uses a blocking `XREAD` with a default two-second poll interval. Consumers rarely call the engine directly — go through `TaskQueue`/`Scheduler`.
- **`RedisPool.ts`** — the minimal service the engine depends on: just `send` + `eval`. Any Redis client can implement it.
- **`NodeRedisPool.ts`** — the bundled `RedisPool` implementation using node-redis `createClientPool` (lazy connect, closed on layer scope end). Tests provide `RedisPool` via ioredis + testcontainers instead (`src/testing/redisLayer.ts`) — proof the service boundary works.
- **`TaskRecord.ts`** — public typed task record schemas and storage codecs.
- **`TaskEvent.ts`** — public queue lifecycle event schemas.
- **`EngineRecord.ts` / `MessagePack.ts` / `RetrySchedule.ts`** — internal Redis record, binary codec, and retry-schedule concepts.

Wiring: `TaskEngine.layer()` is the complete zero-requirement Node live graph and intentionally retains Redis operational services. `TaskEngine.layerNoDeps()` is the custom-client layer that requires `RedisPool`.

`Worker` provides built-in bounded local concurrency, lease supervision,
maintenance, and graceful draining. It does not provide distributed/global
concurrency or rate limiting; compose those policies explicitly at the
application boundary.

## Conventions

- **Effect 4 beta idioms**: `Context.Service` classes for services, `Schema.TaggedError` for schema-backed errors, `Effect.fnUntraced` for functions, `Data.TaggedError` for engine errors, imports from `effect/unstable/*` where needed (e.g. `effect/unstable/persistence/Redis`). Match these when adding code.
- Service identifiers use `@effectmq/core/<Service>`; nominal type IDs and built-in error tags use the `~effectmq/...` namespace.
- `effect` is a **peerDependency** (`>=4.0.0-beta.107`) and devDependency. `@effect/platform-node` is a runtime dependency for the standard live graph; all Effect packages use the same beta baseline.
- Public API (everything re-exported from `src/index.ts` as namespace exports) carries TSDoc, including `@module` headers per file. Keep new exports documented.
- Formatting/linting is Biome (2-space indent); config in `biome.json`.

## openspec/

Spec-driven change tracking lives in `openspec/`: `specs/` holds the current capability specs (e.g. `task-retry-policy`, `task-completion-policies`, `task-events-stream`); `changes/` holds in-flight change proposals (`proposal.md`, `design.md`, `tasks.md`, delta specs), archived to `changes/archive/<date>-<name>/` when done. For behavior changes to a spec'd capability, check the relevant spec and keep it in sync.
