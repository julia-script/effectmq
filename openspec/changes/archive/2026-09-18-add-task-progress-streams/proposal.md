# Proposal

## Why

Long-running tasks, including tasks executed by agents, need to expose typed progress while they run so a UI can show intermediate activity and explain retries. A history owned by the task gives readers both application progress and execution lifecycle context without requiring every task to pay for an additional stream.

## What Changes

- Add an optional `progress` schema to `Task.make`, alongside `payload`, `success`, and `error`. Declaring it opts the task into a Redis Stream containing custom progress and automatic lifecycle events. The schema describes custom progress values; built-in lifecycle events use a library-defined envelope.
- Give each enabled task generation its own ordered history, identified by queue, task ID, and generation. Persist enablement with the generation so workers and maintenance consistently honor it. Tasks without `progress` gain no additional history stream or history writes; existing queue-wide lifecycle events continue unchanged.
- Allow workers to emit schema-validated progress from their current execution attempt. Appends must atomically validate generation, live lease ownership, and execution state. Stale workers cannot append, including after task deletion or replacement.
- Record meaningful lifecycle transitions automatically for enabled tasks, including creation, acquisition, retry scheduling, success, and terminal failure, with cancellation and stalled recovery represented. Append lifecycle entries atomically with their state transitions. Routine lease renewals do not create history entries.
- Preserve history across retries and attach attempt identity to execution events. A new generation starts a fresh history; duplicate offers returning the existing generation do not reset or duplicate it.
- Expose a typed read API with bounded, cursor-based pagination for an exact task generation. Independent readers can replay and poll without consumer groups or acknowledgements. Reads distinguish an existing task with no entries from a removed generation, and expose storage, schema, and cursor failures explicitly.
- Default to no history entry-count limit. An optional positive limit trims the oldest entries across both progress and lifecycle events, keeps new entries flowing, and reports missing history to readers. Existing per-value encoded-size limits still apply to custom progress values.
- Bind history disposal to the task record. Retained completed tasks retain their history; retention holds preserve both. Completion with deletion, retention expiry, administrative removal, and replacement that disposes of an old generation remove its history with its record. Separately retained results do not extend history lifetime. A reader may miss final events when the task uses delete-on-completion; applications needing post-completion history must retain the task.
- Preserve existing task definitions and one-argument handlers. Progress operations must expose their schema services and operational failures accurately, and progress-write failures must not be encoded as the task's declared business error, including for tasks whose error schema is `Schema.Never`.

Example definition:

```ts
const Greet = Task.make({
  name: "greet",
  schemaId: "greet/v1",
  payload: { name: Schema.String },
  success: Schema.String,
  error: Schema.Never,
  progress: GreetingProgress,
  idempotencyKey: ({ name }) => name,
})
```

### Design scope

- The design specifies worker emission, the page response, and lifecycle envelopes. A live Effect `Stream` reader is deferred; paginated reading while a task runs supports polling in this change.
- Indeterminate appends remain visible to callers and are not automatically retried. This change does not promise emission deduplication or exactly-once progress across transport retries or repeated task attempts.

## Capabilities

### New Capabilities

- `task-progress-streams`: Opt-in, schema-typed task-generation history containing custom progress and automatic lifecycle events, with attempt attribution and independently paginated reads.

### Modified Capabilities

- `task-completion-policies`: Extend task-record disposal and retention behavior to the associated history, including holds and deletion on completion.
- `task-delivery-safety`: Extend attempt fencing to progress appends and preserve generation isolation under stale writes, retries, duplicate offers, and replacement.
- `storage-protocol`: Extend lossless typed-value encoding, schema identity, and per-value limits to progress values; support optional history count limits with oldest-first trimming and an unlimited default.

The existing `task-events-stream` capability describes the queue-wide stream used by `TaskQueue.stream`, `wait`, and `execute`; its behavior is unchanged. Existing `effect-api-contracts` requirements apply to the new APIs without relaxing their error or service channels. Durable `EventQueue` subscriptions and acknowledgement semantics are unaffected.

## Impact

- Public task, queue, and worker types must carry the optional progress schema while preserving inference and compatibility for existing definitions and handlers.
- TaskEngine and Redis transition functions need conditional history appends, generation and lease checks, paginated reads, and cleanup coordinated with task-record disposal. Storage encoding and decoding must support progress values.
- Redis storage and write volume increase for enabled task generations. History count is unlimited by default, so retained long-running tasks can accumulate substantial history. No new Redis deployment dependency is intended; the design defines an explicit upgrade boundary for progress-enabled tasks.
- Documentation and examples must explain polling, attempt history, opt-in costs, and the explicit consequence of delete-on-completion. Verification should cover typed round trips, disabled-task behavior, concurrent appends and pagination, stale leases, retries, generation reuse, retention holds, and every task-disposal path.
