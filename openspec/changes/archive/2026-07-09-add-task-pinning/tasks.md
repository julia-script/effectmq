# add-task-pinning — Tasks

## 1. Schemas and types

- [x] 1.1 Add `refCount`, `refs`, `createdBy`, terminal-outcome flag, and `dead` flag to the engine task model in `src/Schemas.ts` (`EngineTask`, `EngineTaskFromRedisEntriesSchema` decode/encode; `EngineTaskInsert` gains `heldBy?` and `createdBy?`)
- [x] 1.2 Add a shared task-reference shape (`{prefix, id}`) with JSON encoding used by `heldBy`, `refs`, and `createdBy`

## 2. Engine Lua — acquisition

- [x] 2.1 Extend `CreateOrUpdateTaskScript`: store `createdBy`; when the task is new, validate each `heldBy` holder (hash exists and dead flag unset — error otherwise, creating nothing), increment the new task's `refCount` per holder, append the new task's reference to each holder's `refs`
- [x] 2.2 Skip all ref acquisition when the task already exists (idempotent re-offer), keeping the current update behavior otherwise

## 3. Engine Lua — death

- [x] 3.1 Add `dieIfDead(prefix, id)` helper: if outcome recorded and `refCount == 0`, apply the outcome policy (delete/keep/mark-as-*), set the dead flag on retained records, then release `refs` via an iterative worklist that decrements each target and re-checks `dieIfDead`
- [x] 3.2 Route `writeSuccess` through it: record the success outcome, publish `task.completed` at completion, move to no list if pinned, call `dieIfDead`
- [x] 3.3 Route `failTask`'s terminal branch through it the same way (retries exhausted or `Canceled`)
- [x] 3.4 Make `RemoveTaskScript` reject pinned tasks (`refCount > 0` errors) and force death of unpinned ones: release refs with cascade (skipping the done check), then delete the record
- [x] 3.5 Ensure cross-queue release publishes list-move events to the target task's own queue stream

## 4. TaskQueue API

Moved out of this change: the user-facing `heldBy`/`createdBy` options on `TaskQueue.offer` (and its TSDoc) will be a separate follow-up change.

## 5. Tests

- [x] 5.1 Acquisition: holder validation (missing holder errors, dead-retained holder errors, done-but-pinned holder accepted), multi-holder counts, cross-queue acquisition
- [x] 5.2 Replay: re-offering an existing task with `heldBy` does not double-pin (in `src/TaskEngine.pinning.test.ts`)
- [x] 5.3 Lifecycle: pinned task defers each policy (delete/keep/mark-as-*) until last release; unpinned behavior unchanged against existing completion-policy tests
- [x] 5.4 Cascade: holder death releases children transitively (fan-out and pipeline shapes); `removeTask` on an alive holder force-releases
- [x] 5.5 `createdBy` stored, returned, unaffected by creator deletion (in `src/TaskEngine.pinning.test.ts`)
- [x] 5.6 Success/failed lists receive pinned tasks only at death

## 6. Release

- [x] 6.1 Add a changeset (minor: engine-level pinning via `heldBy`, `createdBy` provenance; semantics note for pinned tasks). README/TSDoc updates move to the follow-up TaskQueue change. (`.changeset/great-pandas-remember.md`)
