## Why

A Redis Stream–backed event system was added to the engine (task lifecycle events plus a typed `TaskQueue.stream` and the `wait`/`execute` APIs built on it), but it landed as a working spike: dead commented-out code, duplicated schemas (`IncomingRedisEventSchema` vs `IncomingRedisEventSchema2`), stale doc comments, typos (`pool`/`poolInterval` for `poll`), and no test coverage for the new behavior. This change locks the feature down: document it, remove the cruft, and cover it with tests before publish.

## What Changes

- Document the task-events-stream capability: events emitted (`task.created`, `task.updated`, `task.failed`, `task.completed`, `task.moved`), the engine `stream` API, and the typed `TaskQueue.stream` / `wait` / `execute` APIs.
- Remove dead code: commented-out blocks in `Schemas.ts` (`decodeTask` draft, `parseTask` body, `IncomingRedisEventSchema2`, stale Lua comments) and `TaskEngine.ts`.
- Deduplicate the Redis-event decode path — keep one `IncomingRedisEventSchema`, delete the `2` variant.
- Fix comments and naming: restore the module doc comment on `TaskEngine.ts`, correct `pool`→`poll` typos (`poolInterval`, "Failed to pool stream"), and update stale comments in the Lua `importMap` (`removeFromAllLists`→`removeFromCurrentLists`, `moveToList`).
- Add tests covering event emission, stream decoding, and `wait`/`execute` round-trips.
- Update `README.md` with a streaming/events section.

## Capabilities

### New Capabilities
- `task-events-stream`: The engine publishes task-lifecycle events to a per-queue Redis Stream and exposes them as a typed Effect `Stream`; `TaskQueue.stream` decodes payloads against the queue's schemas and `wait`/`execute` await a task's terminal event.

### Modified Capabilities
<!-- No requirement-level changes to existing specs; completion-policy behavior is unchanged, only now emits events. -->

## Impact

- Code: `src/Schemas.ts`, `src/TaskEngine.ts`, `src/TaskQueue.ts`, Lua `importMap` scripts.
- Tests: `src/TaskQueue.test.ts` (and/or a new events test file).
- Docs: `README.md`.
- No public API removals; `stream`/`wait`/`execute` are new additions. No dependency changes.
