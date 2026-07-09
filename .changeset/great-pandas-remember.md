---
"@effectmq/core": minor
---

Add engine-level task pinning: tasks can hold references on other tasks so their records (and results) survive until every holder is gone.

- `TaskEngine.createTask` accepts `heldBy` (a list of `{prefix, id}` task refs): each holder pins the new task by incrementing its `refCount`, atomically and validated at creation (holders must exist and be alive). Refs work across queues. Re-creating an existing task (idempotent re-offer) never double-pins, so replaying holders are safe.
- New alive/dead lifecycle: a task dies only when it is done (terminal success/failure) **and** `refCount` is 0. Completion policies (`onSuccessPolicy`/`onFailurePolicy`) now apply at death — a done-but-pinned task keeps its record in no list until its last holder dies. Unpinned tasks (the default) are observably unchanged. Death releases the task's own refs, cascading through held children in the same atomic operation; the `success`/`failed` lists only ever contain dead tasks.
- `removeTask` on a pinned task now fails ("task is pinned") — remove the holders first. On an unpinned task it releases the task's refs before deleting, so removing a holder cannot leak its children.
- New optional `createdBy` task ref on creation: pure provenance metadata for tooling, never used by the lifecycle.

The user-facing `TaskQueue.offer` options for pinning ship separately.
