# Design

## Context

See `proposal.md` for motivation and the delta specs for the behavioral contract.
This change crosses the typed task API, managed worker processing, the storage
protocol, and Redis lifecycle functions, so a design artifact is required.

The current implementation provides these integration points:

- `src/Task.ts` constructs pure task definitions and owns retention and storage
  defaults. A queue binds one definition to one name.
- `src/TaskQueue.ts` invokes a handler with a decoded task while retaining the
  lease token in its internal attempt. `processAttempt` races handler execution
  against renewal and currently sends every typed handler failure to the task's
  business-error encoder. `Worker` delegates managed execution to this path.
- `src/TaskEngine.ts` and `src/lua/taskEngine.lua` own generation identity,
  attempt acquisition, state transitions, holds, and cleanup. `deleteTask` is
  used for completion disposal, maintenance, removal, and replacement. Results
  have separate retention and cannot serve as the lifetime owner of history.
- The queue-wide lifecycle stream already serves `stream`, `wait`, and
  `execute`; the recently added `EventQueue` serves acknowledged subscriber
  deliveries. Neither provides independently paginated task-owned history.
- The lease, retention, and queue-event suites already use real Redis with
  deterministic engine time. They provide reusable patterns for race and
  cleanup assertions rather than requiring a second storage harness.

## Goals / Non-Goals

**Goals:** Keep task ownership authoritative for both progress and disposal;
preserve schema inference and honest Effect failure/service channels; use one
ordered history for application activity and lifecycle context; keep reads
bounded even when total history is unlimited.

**Non-Goals:** Browser transport, UI components, consumer groups,
acknowledgements, a new general-purpose event bus, exactly-once emission,
independent history retention, or a live Effect Stream reader in this change.
Polling the paginated API is the supported initial live-view mechanism.

## Decisions

### 1. Enable history through the progress schema and persist that decision

Add `progress?: Progress` to every `Task.make` overload, with a trailing defaulted
schema type parameter on exported types to preserve existing generic argument
positions. Omission disables history; schema presence enables it, including
`Schema.Never` for lifecycle-only history. Keep a separate presence flag rather
than deriving enablement from the inferred value type.

Add `storageLimits.maxHistoryEntries?: number | null`, defaulting to `null`.
Use a positive safe integer for a configured count limit. Keep this separate
from the existing `maxEventEntries`, which bounds queue-wide events. Snapshot
enablement and the effective history limit on offer; duplicate offers preserve
the original configuration. Missing fields on pre-feature records mean disabled.
If a runtime definition cannot supply the schema for an enabled generation,
fail its typed history operation rather than ignoring progress.

Alternative: a separately configured event queue would require a second schema
and lifecycle owner. Using the progress declaration keeps these together.

### 2. Give managed handlers an attempt-bound emission capability

Extend the handler shape to `(task, context) => Effect`, with
`context.progress(value)` returning an Effect of the stored event ID. The
context captures the queue, exact generation, progress schema, and lease token;
application code does not choose an arbitrary task ID or supply a token.
Existing one-argument callbacks remain assignable. Tasks without a progress
schema expose no callable progress capability in their inferred context.

Use the same context in `complete`, `completeOne`, and `Worker.make` handlers.
Offer a corresponding raw engine append operation for low-level users already
holding an engine attempt. The high-level API must not require an attempt that
it cannot produce. Encoding requirements come from the progress schema, and
reading requirements come from its decoder; offering a task does not acquire
progress schema services unnecessarily.

Alternative: expanding the ambient `TaskContext.currentTask` to a public writer
would make the schema binding less explicit. Keep that reference's existing
identity role and pass the typed emission capability directly.

### 3. Separate progress operational errors from business failures

Introduce a tagged `ProgressWriteError` wrapper for expected schema, storage,
ownership, disabled-history, and engine failures encountered by the emission
capability. Preserve structured reason and cause fields. The handler contract
admits the declared business error or this operational wrapper. Before calling
the business-error encoder, `processAttempt` recognizes the wrapper using the
library-owned identity and returns it through the completion API's error union.
Do not wrap interruption or defects as business failures.

An unhandled progress error does not acknowledge success or record a business
failure. Renewal stops with the failed processing operation; the attempt is
recoverable by the existing lease-expiry/stall policy. A handler can explicitly
handle a progress failure, but any later write or settlement still requires
current ownership. Update worker failure handling so this error follows its
existing operational-error path. Test a `Schema.Never` task directly.

Alternative: silently logging failed appends would hide missing history;
requiring applications to put Redis errors in every business schema would
couple task data to infrastructure.

### 4. Store one stream per exact generation with compact envelopes

Derive the stream key using the existing task identity/key conventions, adding
the generation and a history suffix. Store history metadata on the task hash,
including the last sequence and the number of entries trimmed. No history key
or metadata writes occur for disabled generations. Do not add a stream TTL.

Expose typed history entries in a dedicated `TaskHistory` module:

```ts
type Entry<Progress> = {
  id: string                 // Redis Stream ID
  taskId: string
  generation: number
  attempt: number             // 0 before acquisition
  timestamp: Date             // server-assigned append time
  event:
    | { _tag: "Progress"; data: Progress }
    | { _tag: "Lifecycle"; data: LifecycleEvent }
}
```

Persist the protocol version, schema identity, and a monotonic sequence with
each entry. The sequence supports gap detection; the public continuation token
encodes it so callers need not manage it. Encode custom data through
`StorageProtocol` using a new `progress` value kind; Lua treats those bytes as
opaque. Lifecycle data uses compact built-in fields: the existing lifecycle
tag, prior/new state where applicable, terminal flag, retry time, failure kind,
and policy. Do not copy entire task payloads or results into each history entry;
the task/result APIs remain their source. This also prevents arbitrary custom
tags from colliding with built-in lifecycle variants.

Reuse lifecycle publication points to append a compact history entry only when
the generation is enabled: creation/update, state moves (including acquisition
and retry), failure, and completion. Acquisition is represented by entering
the leased state; retry by entering retry-scheduled; cancellation and stalled
recovery by failure kind. Preserve the existing queue-wide event payloads and
cursor behavior. Renewal emits no entry. Multiple meaningful entries from one
transition retain a deterministic order.

Alternative: filtering the queue stream requires scanning unrelated tasks and
inherits queue retention. A dedicated stream directly supports range reads and
task-owned cleanup.

### 5. Fence appends and share the existing disposal paths

Add a Redis Lua script for custom progress that checks record existence, exact
generation, schema identity, enabled history, leased state, current token, and
lease deadline against authoritative engine time before appending. Check the
deadline even if maintenance has not recovered the expired lease. Do not renew
the lease or change retry counters as a side effect of progress.

Lifecycle appends run in the same Redis Lua script as their transition. Validate
expected arguments and history key types before mutating execution state;
Redis Lua script atomicity prevents interleaving but does not roll back commands
after a script error. Do not introduce a new count-cap failure at settlement.

Extend `deleteTask` to remove the generation's history with the record. Audit
ordinary removal, force removal, new-generation replacement, completion,
expiry, hold release, and failed-offer cleanup for bypasses. A retained result
must never keep the history alive after the record is disposed of. History
reads check record identity and read its entries atomically, so a concurrent
replacement cannot mix a prior handle with a new generation's entries.

### 6. Unlimited history by default; exact oldest-first trimming when configured

With `maxHistoryEntries: null`, append without an entry-count trim. No hidden
finite default and no reuse of `eventMs` or `maxEventEntries` is allowed.
Progress values still obey `maxValueBytes`.

With a finite limit, use exact oldest-first stream trimming as part of every
append, counting lifecycle and custom entries together. Record how many
entries were actually removed, keeping sequence and trim metadata consistent.
Approximate trimming is not appropriate because the contract promises at most
the configured count after an append. The cap is immutable within a generation,
so steady-state appends evict only the new overage; no retroactive large trim
operation is needed. A limit of one retains only the latest entry. Old attempt
events can be evicted by this explicit policy; retry itself never resets history.

### 7. Use an atomic, bounded page operation with generation-bound cursors

Add `TaskQueue.readEvents(queue, handle, { after?, limit? })`. The handle is the
existing offer handle; its queue-wide wait cursor is not a history cursor.
Default page size is 100 and the supported range is integer 1 through 1,000.
Use exclusive oldest-first range reads and fetch at most `limit + 1` entries
to determine `hasMore`. Return:

```ts
{
  entries: ReadonlyArray<TaskHistory.Entry<Progress>>,
  cursor: string,
  hasMore: boolean,
  truncated: boolean,
}
```

The opaque versioned cursor binds queue, task ID, generation, last sequence,
and last Redis ID. Validate its format, identity, and position. It is not an
authorization token. Do not offer offset pagination or snapshot guarantees.

Store sequences contiguously from one; retain a trimmed-through watermark and
the ID immediately before the earliest remaining entry. For an explicit cursor
at sequence `s`, report `HistoryCursorExpired` if `s` is less than the
trimmed-through sequence. Include an `earliestCursor` at that boundary so a
reader can explicitly resume. A cursor exactly at that boundary is valid,
even though its own event was removed. Reject inconsistent sequence/ID pairs
or future positions. The identity/state check, watermark check, and page read
execute in one Redis Lua script so concurrent trimming cannot hide a gap.

Without `after`, begin at that boundary and set `truncated` if any earlier
entries were removed. The indicator describes the history, not just this page.
An empty page preserves `after`; if no cursor was supplied, return the valid
boundary cursor. `hasMore` is only the state observed by this read; an empty
page never means the running task is complete. Deleted/replaced generations,
disabled history, invalid cursors, schema mismatch, corruption, and unsupported
protocols remain distinct typed outcomes. Do not return partial success when
decoding one entry fails.

Alternative: a blocking reader adds shutdown, terminal-drain, and connection
management semantics. Keep it outside this change; a UI backend can poll pages
and independently use existing task result APIs.

### 8. Preserve uncertain-write semantics without automatic replay

Append once and return the assigned event ID on success. Wrap an uncertain
transport outcome with a structured indeterminate-write reason and generation
identity. Do not retry the append in the library or through a replaying adapter
configuration. Reader calls are safe to retry, subject to retention and trimming.
Callers may inspect retained history before deciding whether to emit again,
but there is no producer deduplication key or exactly-once guarantee here.

## Risks / Trade-offs

- Unlimited retained history can grow indefinitely → Document the default and
  optional count cap prominently; preserve per-value limits and bounded pages.
- Delete-on-completion can remove final entries before polling sees them →
  Explicitly document this accepted behavior and demonstrate retained tasks.
- Trimming can remove early lifecycle context and prior attempts → Surface
  truncation and cursor gaps; use one explicit policy for the complete history.
- More writes for opted-in tasks → Keep lifecycle entries compact, reuse
  transition calls, and verify disabled tasks create no history keys or writes.
- Progress errors widen managed operational failures → Add inference and
  runtime tests, especially Never business errors and schema service requirements.
- Old workers or schedulers cannot maintain the new history contract → Use the
  rollout boundary below instead of claiming mixed-version support for enabled
  generations.

## Migration Plan

1. Add backward-readable optional task metadata and the new history envelope.
   Existing records default to history-disabled; do not backfill earlier events.
   Extend the storage compatibility fixtures and keep old task/result envelopes
   readable. Use history format version 1 and the existing storage envelope
   version 1 with the new `progress` kind; old readers are not supported for
   that new kind. Keep existing queue-event contents and value-kind bytes
   unchanged.
2. Upgrade all producers, workers, and maintenance/scheduler processes for an
   affected queue before offering progress-enabled generations. Verify how the
   existing loader uses content-addressed SCRIPT LOAD/EVALSHA and reloads after
   NOSCRIPT; it does not replace global Redis function libraries. Rehearse the
   upgrade with retained legacy records. Do not enable history while older
   runtimes can process enabled generations using their own script digests.
3. Enable `progress` declarations using an appropriate task schema identity and
   explicitly choose completion retention for applications needing history after
   completion. No in-place history enablement for existing generations.
4. To roll back, stop offering enabled generations, finish active enabled work,
   and dispose of enabled task records and history through the upgraded engine
   before returning to an older runtime. Retained enabled records require
   staying on the upgraded runtime until they can be removed; old cleanup code
   must not orphan their streams. Unlimited or held records may require explicit
   administrative removal as part of a planned rollback.
