## Context

The events/stream feature is already implemented and working across `Schemas.ts`, `TaskEngine.ts` (Lua `importMap` + `stream`), and `TaskQueue.ts` (`stream`/`wait`/`execute`). This change is primarily cleanup and hardening: the design here documents the decisions the spike already committed to (so future edits don't re-litigate them) and pins down the cleanup so it doesn't change behavior.

Current state / cruft to remove:
- `Schemas.ts`: commented-out `decodeTask` draft, `import type * as Task`, `IncomingRedisEventSchema2` (unused duplicate of `IncomingRedisEventSchema`), stray commented schema lines.
- `TaskEngine.ts`: module doc comment deleted, commented-out `parseTask` body, `pool`/`poolInterval` typos, stale `removeFromCurrentLists`/`moveToList` comments in the Lua strings.

## Goals / Non-Goals

**Goals:**
- Document how events flow: Redis Stream per queue → engine `XREAD` polling → typed `TaskQueue` decode.
- Remove all dead/commented code and duplicate schemas without changing behavior.
- Fix comments, module docs, and `poll` naming.
- Add tests for event emission, stream decode, and `wait`/`execute`.

**Non-Goals:**
- No change to completion-policy semantics (only that they now emit events).
- No consumer-groups / `XACK` delivery guarantees — polling `XREAD` with a cursor is sufficient for now.
- No new public API surface beyond what already exists.

## Decisions

- **Redis Streams over pub/sub for events.** Streams are durable and cursor-addressable, so a consumer can resume from a known event id (`wait`/`execute` rely on catching a task's terminal event even if it opens slightly late). Pub/sub would drop events for a late subscriber. Trade-off: consumer must poll and manage the cursor.
- **Events published from Lua, in the same atomic script as the state change.** `publishEvent` is called inside `moveToList`, `createTask`, complete/fail scripts, so an event is emitted iff the state transition committed — no separate publish step that could diverge from state.
- **Decode in layers, in TypeScript, not Lua.** Redis returns `XREAD` replies as nested arrays; a single `IncomingRedisEventSchema` (Tuple → entries record → `EventSchema`) turns them into typed events. `TaskQueue.stream` adds a second decode pass to type the task/success/error payloads against the queue schemas. Keeping one `IncomingRedisEventSchema` (drop the `2` variant) avoids two divergent decode paths.
- **`wait`/`execute` = filter the stream for the task's terminal event.** Rather than a bespoke blocking read, both open `TaskQueue.stream`, `filter` to `taskId` + (`task.completed`|`task.failed`), `take(1)`. `execute` offers first, then awaits. Reuses the stream instead of a second mechanism.

## Risks / Trade-offs

- [Polling latency] `XREAD` polls at `pollInterval` (default 1s) → up to ~1s latency on `wait`/`execute`. → Mitigation: interval is configurable; acceptable for a task queue.
- [`execute` race — task completes before the stream is listening] `execute` opens the stream *before* offering, so the terminal event is captured. → Keep that ordering; a test should cover a fast handler.
- [Cleanup changing behavior] Removing commented code is safe, but the `parseTask` rewrite (to `EngineTaskFromRedisEntriesSchema`) and `pool`→`poll` renames touch live paths. → Mitigation: rename the internal `poolInterval` option and error string only; tests must pass unchanged.
