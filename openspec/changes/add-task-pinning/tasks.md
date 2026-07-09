# add-task-pinning — Tasks

## 1. Schemas and types

- [x] 1.1 Add `refCount`, `refs`, `createdBy`, terminal-outcome flag, and `dead` flag to the engine task model in `src/Schemas.ts` (`EngineTask`, `EngineTaskFromRedisEntriesSchema` decode/encode; `EngineTaskInsert` gains `heldBy?` and `createdBy?`)
- [x] 1.2 Add a shared task-reference shape (`{prefix, id}`) with JSON encoding used by `heldBy`, `refs`, and `createdBy`

## 2. Engine Lua — acquisition

- [x] 2.1 Extend `CreateOrUpdateTaskScript`: store `createdBy`; when the task is new, validate each `heldBy` holder (hash exists and dead flag unset — error otherwise, creating nothing), increment the new task's `refCount` per holder, append the new task's reference to each holder's `refs`
- [x] 2.2 Skip all ref acquisition when the task already exists (idempotent re-offer), keeping the current update behavior otherwise

## 3. Engine Lua — death

- [ ] 3.1 Add `dieIfDead(prefix, id)` helper: if outcome recorded and `refCount == 0`, apply the outcome policy (delete/keep/mark-as-*), set the dead flag on retained records, then release `refs` via an iterative worklist that decrements each target and re-checks `dieIfDead`
- [ ] 3.2 Route `writeSuccess` through it: record the success outcome, publish `task.completed` at completion, move to no list if pinned, call `dieIfDead`
- [ ] 3.3 Route `failTask`'s terminal branch through it the same way (retries exhausted or `Canceled`)
- [ ] 3.4 Make `RemoveTaskScript` a forced death: release refs with cascade (skipping the done check), then delete the record
- [ ] 3.5 Ensure cross-queue release publishes list-move events to the target task's own queue stream

## 4. TaskQueue API

- [ ] 4.1 Add `heldBy` and `createdBy` to `TaskOptions` in `src/TaskQueue.ts`, encoding task references to `{prefix, id}` and passing them to `engine.createTask`
- [ ] 4.2 Update TSDoc for `offer` and the new fields; note policies now apply at death (identical timing for unpinned tasks)

## 5. Tests

- [ ] 5.1 Acquisition: holder validation (missing holder errors, dead-retained holder errors, done-but-pinned holder accepted), multi-holder counts, cross-queue acquisition
- [x] 5.2 Replay: re-offering an existing task with `heldBy` does not double-pin (in `src/TaskEngine.pinning.test.ts`)
- [ ] 5.3 Lifecycle: pinned task defers each policy (delete/keep/mark-as-*) until last release; unpinned behavior unchanged against existing completion-policy tests
- [ ] 5.4 Cascade: holder death releases children transitively (fan-out and pipeline shapes); `removeTask` on an alive holder force-releases
- [x] 5.5 `createdBy` stored, returned, unaffected by creator deletion (in `src/TaskEngine.pinning.test.ts`)
- [ ] 5.6 Success/failed lists receive pinned tasks only at death

## 6. Docs and release

- [ ] 6.1 Update `README`/module TSDoc where completion policies are described to the disposal-at-death wording
- [ ] 6.2 Add a changeset (minor: new `heldBy`/`createdBy` options; semantics note for pinned tasks)
