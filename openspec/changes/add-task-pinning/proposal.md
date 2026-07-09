# add-task-pinning

## Why

Tasks that depend on other tasks' results have no safe way to read them: the default `delete` policy removes a task's record the moment it completes, so a dependent task (e.g. a parent replaying while waiting on children it spawned) races against deletion — the only workaround is `keep`, which leaks forever. We want durable-workflow-style replay reads (a parent re-reads a child's pinned result across its own retries) without BullMQ-style parent/child hierarchy and without records living forever: a task's record should survive exactly as long as another task holds a reference to it, then fall back to its own disposal settings.

## What Changes

- Add a per-task `refCount` at the TaskEngine level. A task whose `refCount > 0` is *pinned*: its record cannot be disposed of, even after it completes.
- Refs are acquired only at task creation via a new `heldBy` option: creating task B with `heldBy: [A]` increments B's `refCount` and appends B to A's `refs` list, atomically, in the create script. Holders are always pre-existing (older) tasks, so the ref graph is acyclic by construction. No standalone addRef API. Re-creating an existing task (idempotent re-offer, e.g. a replaying parent) skips ref acquisition entirely.
- Introduce an alive/dead task lifecycle: a task is **done** when it reaches a terminal success/failure, and **dead** only when it is done *and* `refCount == 0`. Death is when its pins on other tasks release (cascading further deaths) and when its own success/failure policy applies.
- **BREAKING** (semantics, pinned tasks only): completion policies (`onSuccessPolicy`/`onFailurePolicy`) now apply at *death* rather than at completion. A done-but-pinned task sits in no list — it does not enter the `success`/`failed` lists until its last holder dies. Unpinned tasks (the default; `refCount == 0` at completion) are observably unchanged. Field names are unchanged.
- `removeTask` on an alive task is a forced death: it releases the task's refs (cascading) before deleting the record.
- Add an optional `createdBy` field on tasks: pure provenance metadata (`{prefix, id}` of the creating task) set at creation, never read by lifecycle logic. Exists so future devtools can visualize the spawn graph even after refs release.
- `TaskQueue.offer` gains `heldBy` and `createdBy` pass-through options.

## Capabilities

### New Capabilities
- `task-pinning`: refCount-based task lifetime — ref acquisition at creation via `heldBy`, the alive/done/dead lifecycle, release-at-death with cascading, forced death via `removeTask`, and the `createdBy` provenance field.

### Modified Capabilities
- `task-completion-policies`: policy application time moves from completion to death. New requirement that a done-but-pinned task defers its policy (stays out of `success`/`failed` lists, record retained) until `refCount` reaches 0; existing requirements are restated as applying at death (identical to completion for unpinned tasks).

## Impact

- `src/TaskEngine.ts`: create script (acquire refs, `createdBy`), success/terminal-failure paths route through a shared `dieIfDead` Lua helper (policy application + ref release + iterative cascade), `removeTask` forced-death path. New task hash fields: `refCount`, `refs`, `createdBy`.
- `src/Schemas.ts`: `EngineTask`/`EngineTaskInsert` gain `refCount`, `refs`, `createdBy`; encode/decode for the new fields.
- `src/TaskQueue.ts`: `TaskOptions` gains `heldBy`/`createdBy`.
- Tests: new engine-level tests for pinning lifecycle, cascade, idempotent re-offer; existing completion-policy tests unaffected (unpinned behavior unchanged).
- Not in scope: suspend/requeue ergonomics for replaying parents (deliberate "not ready yet" failures currently consume `maxRetries` and pollute the errors list — follow-up change), holder-set introspection ("who pins X"), a dedicated list for done-but-pinned tasks, retention/trimming policies for `success`/`failed` lists (this design keeps those lists dead-only so future trimming needs no ref awareness).
