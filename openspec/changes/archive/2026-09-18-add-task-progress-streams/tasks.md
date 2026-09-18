# Tasks

## 1. Definition and storage contracts

- [x] 1.1 Add the optional progress schema and trailing defaulted type parameters through Task, TaskQueue, and Worker descriptions; verify inference for tagged-union progress, omitted progress, lifecycle-only Never progress, and existing explicit generic arguments with compile-time fixtures and `pnpm typecheck`.
- [x] 1.2 Add `storageLimits.maxHistoryEntries` with an unlimited null default and positive-safe-integer validation, preserving pure constructors; verify first-use rejection of invalid values, the explicit null case, and independence from `maxEventEntries` in definition and invariant tests.
- [x] 1.3 Extend task insertion/decoding to carry generation-persisted enablement and the effective history limit, treating absent legacy fields as disabled; verify old-record fixtures, immutable configuration on duplicate offers, and new-generation configuration with focused engine tests.
- [x] 1.4 Extend StorageProtocol with the progress value kind and version/schema checks, and introduce TaskHistory entry types, compact lifecycle variants, codecs, and semantic errors; verify lossless binary/null/collection round trips, corrupt data, schema mismatch, unsupported versions, and per-value byte limits in storage/history tests.

## 2. Redis history writes and ownership

- [x] 2.1 Add generation-specific history keys, contiguous sequence/trim metadata, and the conditional append helper in `src/lua/taskEngine.lua`; verify enabled generation isolation and that disabled tasks create no history keys or metadata writes.
- [x] 2.2 Implement unlimited appends and exact oldest-first trimming for a configured count limit; verify null/default history grows beyond a small queue-event cap, limit one retains the newest entry, progress and lifecycle entries share the cap, and the count never exceeds it after an append.
- [x] 2.3 Add the engine progress-append operation with atomic generation, schema, enabled-state, token, leased-state, and deadline checks; verify current-owner success and rejection of stale, expired-but-unrecovered, settled, removed, and replaced attempts using deterministic Redis time.
- [x] 2.4 Connect conditional compact lifecycle appends to creation/update, acquisition/state moves, attempt failure, retry, cancellation, stalled recovery, and completion; verify one ordered attempt-attributed history and no entries for renewals or unchanged duplicate offers in real-Redis lifecycle tests.
- [x] 2.5 Keep append transport failures structured and indeterminate without automatic replay, including the Redis adapter path; verify a lost acknowledgement can leave one committed entry but does not trigger a second append using fault injection.

## 3. History disposal and retention

- [x] 3.1 Extend the shared task-disposal operation and any bypasses to remove the exact generation's history; verify success/failure delete policies, ordinary removal, force removal, replacement, and failed-offer rollback leave no orphaned history keys.
- [x] 3.2 Carry task-record retention and holds through history lifetime without a separate TTL; extend the pinning/retention suites to verify keep/mark policies, final hold release, record expiry, retained results after record deletion, and rejected removal preserving history.
- [x] 3.3 Exercise append-versus-settlement, append-versus-force-removal, and old-handle-versus-replacement races; verify accepted entries precede settlement, deleted streams cannot be recreated by late writers, and generations never share entries.

## 4. Cursor pagination and typed reading

- [x] 4.1 Implement versioned history cursors binding queue, task ID, generation, sequence, and Redis ID, including the trimmed-through recovery boundary; verify malformed, mismatched, inconsistent, future, initial, and boundary cursors with focused cursor tests.
- [x] 4.2 Add the atomic Redis page operation with identity/existence checks, per-page gap detection, and a bounded `limit + 1` range read; verify concurrent append/trim/replacement behavior, no duplicate page boundaries, truthful `hasMore`, and bounded page sizes with real Redis.
- [x] 4.3 Expose `TaskQueue.readEvents(queue, handle, { after?, limit? })` with default 100/max 1,000 and decoded entries, cursor, hasMore, and truncated; verify independent readers, empty-page cursor preservation, typed disabled/unavailable errors, corrupted-page failure, and exact schema service requirements.
- [x] 4.4 Verify gap recovery end to end: new readers see retained history plus truncation, lagging explicit cursors fail with an earliest recovery cursor, and a cursor whose own entry was removed still works when all subsequent entries remain; cover trimming between successive page calls.

## 5. Managed worker emission

- [x] 5.1 Pass an attempt-bound context with typed `progress(value)` to complete, completeOne, and Worker handlers while preserving one-argument callbacks; verify progress inference, stored event IDs, disabled-task typing, and that captured contexts cannot write after lease loss.
- [x] 5.2 Route ProgressWriteError through managed processing's operational error channel before business-error encoding and update worker handling; verify a Never-error task with a failed append records neither business failure nor success, interruption remains interruption, and the abandoned lease follows existing recovery behavior.
- [x] 5.3 Preserve explicit application handling of progress failures and exact Effect service/error unions; verify catching a progress error, emitting with a service-dependent schema, ordinary business retries, and successful result completion in compile-time and runtime tests.

## 6. Integration, compatibility, and release evidence

- [x] 6.1 Add a producer/worker/reader integration test that observes progress while the handler is still running, continues across a retry, and pages the retained completed history; verify a separate delete-on-completion case removes the entire history even while its result remains readable.
- [x] 6.2 Add compatibility fixtures and a rollout/rollback rehearsal for retained legacy records, new history format v1, and the existing content-addressed Redis script loader; verify old records remain readable, progress stays disabled on them, and rollback cleanup through the upgraded engine leaves no enabled-record history behind.
- [x] 6.3 Export the new public history API/types through the package root and subpath metadata, regenerate the Lua binding using `pnpm gen:lua`, and verify `pnpm check:lua`, `pnpm check:architecture`, and `pnpm verify:package` pass with a consumer that declares and reads typed progress.
- [x] 6.4 Update task/queue/worker reference docs, storage-protocol documentation, and a runnable retained-task polling example; verify docs explicitly state unlimited history by default, optional trimming and gaps, attempt attribution, delete-on-completion semantics, uncertain writes, and the all-process upgrade boundary using `pnpm check:docs` and `pnpm docs:typecheck`.
- [x] 6.5 Add the appropriate feature changeset and run the complete validation suite (`pnpm test`, `pnpm check`, and `pnpm check:changesets`); verify existing queue streams, wait/execute, disabled tasks, and EventQueue regressions remain green and record the command results for review.
