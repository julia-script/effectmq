# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`@effectmq/core` — a Redis-backed task queue built on **Effect 4 beta** (pinned to `effect@4.0.0-beta.102`; it does not work with stable Effect 3.x). Typed payloads/results/errors via schemas, with retries, delays, idempotency, and cron schedules. Single package, pnpm, ESM (`"type": "module"`, `nodenext` resolution — internal imports use `.js` extensions).

## Commands

```bash
pnpm install                      # install (pnpm@10.x, see packageManager)
pnpm build                        # tsc -> dist/ (tests and src/testing are excluded from the build)
pnpm exec tsc --noEmit            # typecheck (what CI runs)
pnpm exec biome check src/        # lint (what CI runs)
pnpm lint:fix                     # biome check --write --unsafe
pnpm test                         # vitest run (integration tests, needs Docker — see below)
pnpm test:watch                   # vitest watch mode
pnpm vitest run src/TaskQueue.test.ts        # single test file
pnpm vitest run -t "test name substring"     # single test by name
```

Note: the `pnpm lint` script runs `turbo run lint`, but turbo is not a dependency — it's stale. Use `pnpm exec biome check src/` (matches CI).

### Tests need Docker

Tests use `@testcontainers/redis` to spin up a real Redis container per vitest worker; `src/testing/redisLayer.ts` builds a shared `TestRuntime` (ManagedRuntime) at module import time so the container boot doesn't eat the first test's timeout. Test timeout is 30s (`vitest.config.ts`). The root `docker-compose.yml` Redis is for manual/local experimentation only — tests don't use it.

### Releases

Changesets-based: run `pnpm changeset` to record a change; the release workflow versions and publishes (`pnpm release` = build + `changeset publish`). Add a changeset for any user-facing change.

## Architecture

Flat `src/` with a strict layering, top to bottom:

- **`TaskQueue.ts`** — the high-level API users live in: `make` (bind a queue name to a task definition), `offer` (enqueue), `complete` (take → run handler → report outcome, applying the task's retry schedule on failure), `stream` (typed lifecycle events), `wait` / `execute` (await a task's terminal result). Decodes engine tasks/events against the task's schemas.
- **`Scheduler.ts`** — cron-driven recurring work. Uses the engine's `setSchedule`/`consumeSchedule` so multiple processes running the same named scheduler fire once per tick collectively.
- **`Task.ts`** — `Task.make`: the schema-bearing task definition (payload/success/error schemas, `idempotencyKey` — which *is* the task id, so same key = same task — `retry` as an Effect `Schedule`, `maxRetries` default 5, `null` = unbounded).
- **`TaskEngine.ts`** (the big one, ~1000 lines) — the low-level `Context.Service` implementing queue primitives as **atomic Lua scripts** (inline `/*lua*/` strings, built by `buildScripts`): create/take/writeSuccess/writeError, lock extend/remove, delayed + cron schedule state. Tasks move between Redis lists: `wait`, `scheduled`, `active`, `failed`, `success`. Every state change publishes to a per-queue Redis Stream (`<prefix>:<name>:events`, via `XADD`); `stream` polls it with `XREAD` (default 1s). Consumers rarely call the engine directly — go through `TaskQueue`/`Scheduler`.
- **`RedisPool.ts`** — the minimal service the engine depends on: just `send` + `eval`. Any Redis client can implement it.
- **`NodeRedisPool.ts`** — the bundled `RedisPool` implementation using node-redis `createClientPool` (lazy connect, closed on layer scope end). Tests provide `RedisPool` via ioredis + testcontainers instead (`src/testing/redisLayer.ts`) — proof the service boundary works.
- **`Schemas.ts`** — shared task model: completion policies (`delete` | `keep` | `mark-as-success` | `mark-as-failure`), built-in `Stalled`/`Canceled` tagged errors, encode/decode between engine (Redis hash) representation and typed tasks.

Wiring: `TaskEngine.layer()` requires `RedisPool`; the standard app layer is `Layer.provideMerge(TaskEngine.layer(), NodeRedisPool.layer())`.

The library deliberately has **no built-in concurrency/rate limiting** — one `complete` processes one task, and callers compose concurrency from Effect primitives (fibers, semaphores, schedules). Don't add worker-pool machinery.

## Conventions

- **Effect 4 beta idioms**: `Context.Service` classes for services, `Schema.TaggedErrorClass` for errors, `Effect.fnUntraced` for functions, `Data.TaggedError` for engine errors, imports from `effect/unstable/*` where needed (e.g. `effect/unstable/persistence/Redis`). Match these when adding code.
- Type IDs are string constants like `"~effectmq/TaskEngine"`; built-in error tags use the `~effectmq/Error/...` namespace.
- `effect` is a **peerDependency** (`>=4.0.0-beta.102`) and devDependency, never a hard dependency. The only runtime dependency is `redis`.
- Public API (everything re-exported from `src/index.ts` as namespace exports) carries TSDoc, including `@module` headers per file. Keep new exports documented.
- Formatting/linting is Biome (2-space indent); config in `biome.json`.

## openspec/

Spec-driven change tracking lives in `openspec/`: `specs/` holds the current capability specs (e.g. `task-retry-policy`, `task-completion-policies`, `task-events-stream`); `changes/` holds in-flight change proposals (`proposal.md`, `design.md`, `tasks.md`, delta specs), archived to `changes/archive/<date>-<name>/` when done. For behavior changes to a spec'd capability, check the relevant spec and keep it in sync.
