# add-task-pinning — Design

## Context

The engine stores each task as a Redis hash (`<prefix>:task:<id>`) and moves task ids between lists (`wait`, `scheduled`, `active`, `failed`, `success`) via atomic Lua scripts. All deletion currently funnels through one Lua helper, `deleteTask`, reached from three sites: success with `delete` policy, terminal failure with `delete` policy, and `removeTask`. Completion policies apply immediately at completion.

Ordering between dependent tasks already works without hierarchy — a handler can `wait`/`execute` any task in any queue — but *reading* a dependency's result is racy: with the default `delete` policy the record may be gone by the time it's read. Pinning fixes lifetime, not ordering: a pinned task's record survives until no live task holds a reference to it.

Target usage is durable-workflow-style replay: a parent task runs multiple short executions (suspending between them via retryable failures), idempotently re-offering children each replay and re-reading their pinned results.

Key constraints:
- Everything must stay atomic inside single Lua scripts (the engine's core invariant).
- Cross-queue refs must work: a ref target is addressed by `(prefix, id)` and all key names are computed from those, so touching another queue's task hash from a script is just another key in the same Redis. (This deepens the existing single-instance/non-cluster assumption — scripts already touch multiple slots and declare `numberOfKeys: 0` — no new constraint.)
- No worker-pool/orchestration machinery; pinning is an engine primitive, composition stays in userland.

## Goals / Non-Goals

**Goals:**
- A task's record survives exactly as long as some live task holds a reference to it, then falls back to its own disposal policy.
- Refs work across queues and require no parent/child hierarchy.
- Ref graph is acyclic by construction — no cycle detection, cascade always terminates.
- Unpinned tasks behave observably identically to today.
- `success`/`failed` lists only ever contain dead tasks, so future retention/trimming needs no ref awareness.
- Idempotent re-offer (replay) is safe: no double-pinning.
- `createdBy` provenance survives independently of refs, for future devtools.

**Non-Goals:**
- Execution ordering / dependency scheduling (use `wait`/`execute` in handlers).
- Suspend/requeue ergonomics for replaying parents (follow-up change; today a deliberate "not ready" retryable failure consumes `maxRetries`).
- Reverse-edge introspection ("who pins X") — the target stores only a count.
- A dedicated list for done-but-pinned tasks (they sit in no list, like `keep`).
- Retention policies for `success`/`failed` lists (only kept compatible).
- Redis Cluster support (pre-existing non-goal).

## Decisions

### D1: Lifecycle — alive / done / dead

- **done**: reached a terminal outcome (success, or failure with no retry pending).
- **dead**: done ∧ `refCount == 0`. Death is a *condition*, not a single event — either conjunct can become true last.
- Only at death: (1) the outcome-keyed policy (`onSuccessPolicy`/`onFailurePolicy`) applies, (2) the task's own refs release.

Why: deferring *all* policies (not just `delete`) keeps the `success`/`failed` lists dead-only, decoupling any future retention trimming from ref bookkeeping. The alternative — apply policy at completion and defer only deletion — puts pinned tasks into `failed`/`success` lists where a trimmer would have to check refCounts.

### D2: Refs release at death, not at record deletion

A dead record is inert data; deleting it (policy, future trim, manual cleanup) is a bare `DEL` with no ref logic. Alternative (release on record deletion) also works but forces every current and future deletion path to run the release loop, and makes `keep`-policy holders pin their children forever. Pins exist so a replaying task can re-read results; replay ends at death, so pins past death serve no one. Recorded here as a decision — if implementation surfaces a reason to switch, it's an isolated change.

### D3: Acquisition only at child creation, `heldBy` direction only

`createTask(B, { heldBy: [A] })` atomically increments B's `refCount` and appends `{prefix, id}` of B to each holder's `refs` field. Edges always point older → newer (spawner → spawned); a task never pins a pre-existing task. Since a node's outgoing pins are fixed at its birth and only reference... (holders must exist at the child's creation), every edge points forward in creation time and cycles are impossible — the cascade in D5 always terminates without cycle detection.

Rejected alternatives:
- Standalone `addRef`: breaks acyclicity-by-construction; reopens leak-by-mutual-pinning.
- A `refs: [...]` option letting a new task pin *older* tasks: sound on its own (new→old edges), but combined with `heldBy` it allows 2-cycles unless an either-or rule is added. All target scenarios (fan-out, pipeline, sibling data flow via the parent) need only `heldBy`; add the other direction later if a real case appears.
- Sibling edges (C pins B): unnecessary — the parent's pin already keeps B alive for as long as A lives; A passes B's output (or its id) to C via C's payload.

Acquisition rules:
- Every holder in `heldBy` must exist (hash present) at creation; otherwise the create script errors ("holder not found"). A dangling holder at birth is a caller bug.
- Holders must be **alive**, not merely existent. A done-but-pinned holder is fine (it will die later and release). But a *dead* record can still exist (`keep` / `mark-as-*` policies retain the hash after death) — its death already ran and will never run again, so a ref held by it would never release. The create script therefore rejects holders whose hash is missing *or* whose dead flag is set. Death sets a `dead` flag on records it retains (delete-policy records are simply gone).
- If the created task already exists (idempotent re-offer / replay), ref acquisition is skipped entirely — no diffing, no error on mismatch. Replay is the main path, not an edge case.

### D4: Storage

On the task hash:
- `refCount` (integer, default 0) — on the pinned task. `HINCRBY` to acquire/release.
- `refs` (JSON array of `{prefix, id}`, default `[]`) — on the holder; what to release at its death.
- `createdBy` (optional JSON `{prefix, id}`) — provenance only; never read by lifecycle logic; survives until the record is deleted.
- an outcome flag (`success` | `failure`) set once done, so a later refCount-zero event knows which policy to apply; and a `dead` flag set at death on records the policy retains (`keep` / `mark-as-*`), so the create script can reject dead holders (D3).

No reverse index (who pins X) — a count suffices for correctness; a holder set is a purely additive later change.

Coordinate system: every TaskRef prefix in storage (`refs`, `createdBy`) is **fully qualified** (`{prefix: "~effectmq:my-queue", id}`) — the service layer applies the engine's global prefix to `heldBy`/`createdBy` inputs exactly as it does to `task.prefix`, so the Lua boundary uniformly speaks fully-qualified prefixes and stored refs are directly key-addressable (the death cascade dereferences them with no extra context). Engine *inputs* stay queue-level; the qualified form appears on read-back (`getTask`, events). The engine level is explicit/physical; friendlier coordinates belong to the layer above. (Considered queue-level storage with a `globalPrefix` ARGV composed in Lua — rejected: it made the create script the only script receiving unqualified coordinates, and no current reader needs to strip prefixes.)

### D5: `dieIfDead` — single choke function, multiple call sites

One Lua helper checks `done ∧ refCount == 0`; if met it runs death: apply the outcome policy (delete → `DEL` hash; `keep` → no list, hash stays; `mark-as-*` → success/failed list), then release own refs by decrementing each entry in `refs` and re-checking `dieIfDead` on each target. Cascade is an iterative worklist, not Lua recursion.

Call sites:
1. `writeSuccess` — task becomes done.
2. `failTask` terminal branch (retries exhausted or `Canceled`) — task becomes done.
3. The decrement loop inside a death cascade — `refCount` reaches 0 on an already-done task.
4. `removeTask` — **forced death**: skips the `done` check but runs the same death body (release refs, cascade) before deleting the record. Without this, force-removing a parent leaks its children.

This replaces `deleteTask` as the choke point, one level up. A done-but-pinned task is removed from all lists (limbo, like `keep`) with its outcome recorded on the hash; events for completion still fire at completion time.

### D6: Events

- `task.completed` / `task.failed` fire at completion, unchanged.
- Death emits the existing `task.moved` (to the policy's target list or nil) at death time; for pinned tasks this means the move event is deferred along with the policy. Whether to add an explicit `task.died` event is left to implementation — not required by the specs, and additive later.
- Cross-queue release publishes to the *target's* queue stream (publishEvent already takes prefix).

### D7: API surface

- `EngineTaskInsert` gains `heldBy?: Array<{prefix, id}>` and `createdBy?: {prefix, id}`.
- `TaskQueue.offer` `TaskOptions` gains the same, with `heldBy`/`createdBy` accepting `(queue, task)`-shaped references and encoding them to `{prefix, id}`.
- No new engine methods. No `addRef`, no `release` — lifecycle is fully implicit.

## Risks / Trade-offs

- [Leaked pins if a holder is never terminally completed (e.g. unbounded retries forever)] → Same failure mode as a task that never finishes today; the child is exactly as leaked as its parent. `removeTask` on the parent force-releases.
- [Done-but-pinned tasks are invisible to list scans] → Accepted; hash carries `refCount` + outcome. A dedicated ZSET is additive later if devtools need it.
- [Cross-queue scripts deepen the non-cluster assumption] → Pre-existing; key scheme and `numberOfKeys: 0` already preclude cluster. Documented, not worsened in kind.
- [Cascade touches many keys in one script (deep pipelines dying at once)] → Bounded by the spawn tree's size; Lua scripts are atomic but block Redis. Acceptable for realistic tree sizes; worklist keeps stack flat.
- [Policy-at-death changes observable timing for pinned tasks (`mark-as-*` list entry appears later)] → Intentional (BREAKING note in proposal); unpinned tasks unchanged.
- [Replay double-pin] → Prevented by skip-refs-if-exists in the create script (D3).

## Open Questions

- Should the umbrella term in docs/specs move from "completion policy" to "disposal policy"? Field names stay; this is naming only. Default: update spec prose, keep field names.
- `task.died` event: add now or later? Default: later (additive).
