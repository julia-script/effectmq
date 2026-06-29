## Why

`TaskEngine` (the Redis/Lua layer) now has failure and completion-policy coverage, but the higher-level `TaskQueue` API in `src/TaskQueue.ts` — the surface application code actually uses — has no tests. Its centerpiece is the `complete` combinator, which ties the whole lifecycle together (take → lock heartbeat → run handler → succeed/fail). We want that path verified before building on it.

## What Changes

- Add integration tests for the `complete` combinator covering both outcomes: handler returns success (`writeSuccess` + returns `true`) and handler fails (`writeError` + returns `false`).
- Exercise the supporting flow end-to-end through `complete`: `offer` enqueues a task, `complete` takes it, decodes the typed payload, runs the handler, and applies the result.
- Use a deterministic `idempotencyKey` so task ids are stable and assertable.
- Tests only — no production changes expected. Any bug surfaced is reported, not silently worked around.

## Capabilities

### New Capabilities
- `task-queue-completion`: The observable behavior of the `TaskQueue.complete` combinator — taking an offered task, running a typed handler, and routing the handler's success/failure to the engine, including the boolean return contract.

### Modified Capabilities

_None._

## Impact

- New `src/TaskQueue.test.ts`.
- Reuses `src/testing/redisLayer.ts` (`TestRuntime`, `getLists`) and `Task.make`/`TaskQueue.make`; no new dependencies.
- Exercises `TaskQueue.offer`, `TaskQueue.complete`, and indirectly `succeed`/`fail`/`takeUnsafe`/`extendLock` against the existing Redis test layer.
