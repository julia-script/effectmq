## 1. Clean up Schemas.ts

- [x] 1.1 Delete commented-out `decodeTask` draft block and `import type * as Task` line
- [x] 1.2 Remove `IncomingRedisEventSchema2` (unused duplicate) and its stray commented lines
- [x] 1.3 Remove stray commented schema lines in `makeTaskSchema`/`EventSchema` (e.g. `successDecoder`/`errorDecoder`, `existingTask`/`newTask` comments)
- [x] 1.4 Confirm the single `IncomingRedisEventSchema` + `decodeIncomingRedisEventList` are the only decode path referenced by `TaskEngine`

## 2. Clean up TaskEngine.ts

- [x] 2.1 Restore the module doc comment describing the low-level engine
- [x] 2.2 Delete the commented-out old `parseTask` body (keep the `EngineTaskFromRedisEntriesSchema` version)
- [x] 2.3 Rename `pool`→`poll`: `poolInterval`→`pollInterval` and the "Failed to pool stream" error string→"Failed to poll stream"
- [x] 2.4 Update stale Lua comments in `importMap` (`-- removeFromCurrentLists(...)` placeholders in the `addTo*`/`moveToList`/`deleteTask` scripts) to reflect actual behavior or remove them

## 3. Update comments and docs

- [x] 3.1 Add/refresh doc comments for the new engine `stream` method and `TaskQueue.stream`/`wait`/`execute`
- [x] 3.2 Add a "Streaming & events" section to `README.md` documenting event types and `stream`/`wait`/`execute`

## 4. Tests

- [x] 4.1 Test that a created task emits `task.created` and a re-created id emits `task.updated`
- [x] 4.2 Test that failure emits `task.failed` (with `willRetry`) and success emits `task.completed`
- [x] 4.3 Test `TaskQueue.stream` decodes payloads to typed task/success/error
- [x] 4.4 Test `wait` resolves on completion and fails on terminal failure
- [x] 4.5 Test `execute` round-trips (including a fast handler that completes quickly)

## 5. Verify

- [x] 5.1 Run the full test suite (`docker-compose up` Redis if needed) and typecheck; confirm no behavior changed
