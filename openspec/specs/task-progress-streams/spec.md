# task-progress-streams Specification

## Purpose

Provide opt-in, typed task-generation history that combines worker progress with execution lifecycle events so independent readers can follow work before completion and inspect retained tasks afterward.

## Requirements

### Requirement: Progress declaration opts a task generation into history

A task definition SHALL accept an optional progress schema. Declaring that schema SHALL enable both custom progress and automatic lifecycle history for new generations. Enablement and history configuration SHALL be persisted for the generation. A generation without this opt-in SHALL create no additional history stream and perform no additional history writes. Existing queue-wide lifecycle event behavior SHALL remain unchanged.

#### Scenario: Enabled task is offered
- **WHEN** a producer offers a new generation whose definition declares a progress schema
- **THEN** that generation has enabled history including its creation lifecycle event

#### Scenario: Ordinary task is processed
- **WHEN** a task without a progress schema is offered, acquired, retried, and settled
- **THEN** no per-task history stream is created and no per-task history entries are written
- **AND** its existing queue-wide events and result-waiting behavior remain available

#### Scenario: Definition changes after an offer
- **WHEN** a later process uses a progress-enabled definition for an existing generation that was offered without history
- **THEN** the existing generation remains history-disabled
- **AND** attempting to emit or read its history fails with a typed history-disabled error

### Requirement: Custom progress is typed independently of lifecycle events

Custom progress SHALL be encoded and decoded using the task's declared progress schema. The read API SHALL distinguish custom progress from library-defined lifecycle events without requiring lifecycle variants in the application's schema. Every entry SHALL identify its event ID, task generation, timestamp, and attempt number; events before acquisition SHALL use attempt zero. Progress schema failures SHALL be visible in typed error channels.

#### Scenario: Valid custom progress round trip
- **WHEN** the current worker emits a valid progress value and a reader reads the entry
- **THEN** the reader receives its decoded value in a progress envelope with the emitting attempt number

#### Scenario: Invalid progress value
- **WHEN** a progress value fails encoding against its declared schema
- **THEN** the append fails with a typed schema-related error and does not store that value

#### Scenario: Application tag resembles a lifecycle tag
- **WHEN** a custom progress value uses the same tag text as a built-in lifecycle event
- **THEN** readers can still distinguish the custom value from the lifecycle event by its enclosing variant

### Requirement: Lifecycle history follows meaningful execution transitions

An enabled generation SHALL record creation, acquisition, execution-state changes, attempt failures, retry scheduling, and terminal success or failure. Cancellation and stalled recovery SHALL carry their distinguishing failure kind. Lifecycle entries SHALL be appended atomically with their associated transitions and ordered with custom progress in the same history. Routine lease renewals and unchanged duplicate offers SHALL NOT append history. Task deletion SHALL remove even the terminal entries according to task-record disposal rules.

#### Scenario: Attempt retries and later succeeds
- **WHEN** attempt one emits progress and fails retriably, and attempt two emits progress and succeeds on a retained task without a history count limit
- **THEN** a reader can observe the first attempt's progress and failure, the retry transition, the second acquisition and progress, and terminal success in append order
- **AND** each execution event identifies the relevant attempt

#### Scenario: Lease heartbeat
- **WHEN** an enabled task's current lease is renewed without an execution-state change
- **THEN** its history does not gain an entry

#### Scenario: Terminal stalled or canceled task
- **WHEN** an enabled retained task settles because of cancellation or exhausted stalled recovery
- **THEN** its lifecycle history identifies the terminal outcome and the cancellation or stalled cause category

### Requirement: History is isolated by generation and preserved across retries

History SHALL belong to exactly one queue, task ID, and generation. Retrying SHALL NOT reset it. A duplicate offer returning the existing generation SHALL preserve its history and configuration. A new generation SHALL begin fresh history according to its own definition, and old generation handles SHALL never read or append the new generation's entries. Preservation across attempts remains subject to an explicitly configured oldest-entry trimming limit.

#### Scenario: Duplicate offer
- **WHEN** a producer repeats an offer that returns an existing enabled generation
- **THEN** its existing history, enablement, and limit remain unchanged
- **AND** no duplicate creation entry is added

#### Scenario: Task ID is reused
- **WHEN** a terminal generation is replaced with a new generation using the same task ID
- **THEN** the new generation begins a separate history and the disposed generation's history is removed
- **AND** reading with the old handle returns a typed generation-unavailable error rather than the new history

### Requirement: Readers paginate independently using exclusive cursors

The read API SHALL accept an exact generation handle, an optional exclusive cursor, and a bounded positive page size. Entries SHALL be returned in append order, with a continuation cursor and an indication of whether more entries were available at that read. A reader without a cursor SHALL start at the earliest retained entry. A caught-up read SHALL preserve the input cursor, or return a valid beginning cursor for an empty history. Reading SHALL neither consume entries nor require acknowledgement. Pages SHALL reflect a single generation-consistent read, not promise a snapshot across multiple calls.

#### Scenario: Events arrive between pages
- **WHEN** a reader requests the next page after the preceding page's cursor and additional entries have been appended
- **THEN** returned entries are strictly after that cursor, without repeating already returned entries
- **AND** additional retained entries can be reached through subsequent pages

#### Scenario: Empty page while task runs
- **WHEN** a reader is caught up and the task is still running
- **THEN** the page is empty, its cursor remains usable, and later polling can return newly appended entries
- **AND** the empty page does not imply task completion

#### Scenario: Independent readers
- **WHEN** two readers request pages for the same generation using their own cursors
- **THEN** either reader's requests do not change the entries available to the other

#### Scenario: Invalid pagination input
- **WHEN** a request has a malformed or mismatched cursor, a cursor ahead of the generation's history, or a page size outside the supported range
- **THEN** it fails with a typed pagination error rather than silently changing the requested range

### Requirement: Reads expose unavailable and incompatible history

Readers SHALL distinguish a retained history with no new entries from a disabled history and an unavailable task generation. They SHALL reject mismatched task/schema identities, unsupported protocol versions, and corrupt entries with typed errors. A separately retained result SHALL NOT make deleted task history readable.

#### Scenario: Task record was deleted but result remains
- **WHEN** a reader requests history for a deleted task whose terminal result is still retained
- **THEN** the read fails with a typed generation-unavailable error

#### Scenario: Stored progress is corrupt
- **WHEN** a page contains progress bytes that cannot be decoded
- **THEN** the page fails with a typed decoding error without silently dropping the entry or returning a successful partial page

### Requirement: Trimming is visible to readers

When an explicit history limit has removed entries, a read without a cursor SHALL return the earliest retained page and indicate that earlier history was truncated. Resuming with an explicit cursor SHALL fail with a typed cursor-expired error if any entries after that cursor have been removed. The error SHALL provide a recovery cursor immediately before the earliest retained entry. A cursor whose immediate next entry is still retained SHALL remain valid, even if its own entry was removed. Gap detection SHALL be performed on every page.

#### Scenario: Reader falls behind
- **WHEN** entries after a reader's cursor are trimmed before its next page request
- **THEN** that request fails with a cursor-expired error
- **AND** retrying with its recovery cursor starts at the earliest retained entry without hiding the gap

#### Scenario: New reader opens a trimmed history
- **WHEN** a reader supplies no cursor after some history has been trimmed
- **THEN** the first page contains retained entries and an explicit truncated-history indicator

#### Scenario: Last consumed entry was trimmed
- **WHEN** the cursor's entry was trimmed but all later entries remain
- **THEN** the reader resumes successfully without a false gap error

### Requirement: Progress failures remain operational failures

Managed handlers SHALL be able to emit progress without adding infrastructure errors to their declared business-error schema. An unhandled progress operational failure SHALL fail managed processing through its typed operational error channel, without recording it as a business failure or success. Interruption and lease loss SHALL retain their existing ownership semantics. Existing handlers that accept only the task SHALL remain compatible.

#### Scenario: Progress fails in a task with no business failures
- **WHEN** a handler whose business error schema is Never encounters an unhandled progress transport failure
- **THEN** managed processing fails with an identifiable progress operational error
- **AND** it does not attempt to encode that failure using the Never schema or settle the task successfully

### Requirement: Uncertain appends are not silently replayed

A progress append whose Redis outcome cannot be determined SHALL expose a typed indeterminate-write failure. The library SHALL NOT automatically retry that append or claim exactly-once emission. Successful appends SHALL return the stored event ID. Documentation SHALL explain that application retries and repeated task attempts can produce duplicate application progress.

#### Scenario: Connection fails after Redis accepts an append
- **WHEN** the server may have appended progress but the client loses the acknowledgement
- **THEN** the caller receives an indeterminate-write error and the library does not issue a second append automatically
