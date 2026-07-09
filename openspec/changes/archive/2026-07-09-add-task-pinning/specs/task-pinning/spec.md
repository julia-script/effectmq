# task-pinning

## ADDED Requirements

### Requirement: Ref acquisition at child creation

The engine SHALL accept an optional `heldBy` list of task references (`{prefix, id}`) on task creation. For each holder, atomically within the create script, the engine SHALL increment the created task's `refCount` by one and append the created task's reference to the holder's `refs` field. Refs SHALL NOT be acquirable through any other operation.

#### Scenario: Child created with a holder
- **WHEN** task A exists and is alive, and task B is created with `heldBy: [A]`
- **THEN** B's `refCount` is 1 and A's `refs` contains B's reference

#### Scenario: Cross-queue holder
- **WHEN** task A exists in queue `qa` and task B is created in queue `qb` with `heldBy` referencing A in `qa`
- **THEN** the acquisition succeeds identically to the same-queue case

#### Scenario: Multiple holders
- **WHEN** tasks A1 and A2 exist and are alive, and task B is created with `heldBy: [A1, A2]`
- **THEN** B's `refCount` is 2 and both A1's and A2's `refs` contain B's reference

### Requirement: Holders must be alive

The engine SHALL reject task creation with an error when any `heldBy` entry references a task whose hash does not exist or whose dead flag is set, without creating the task or acquiring any refs.

#### Scenario: Missing holder
- **WHEN** task B is created with `heldBy` referencing a task id that does not exist
- **THEN** the create call fails and B is not created

#### Scenario: Dead holder with retained record
- **WHEN** task A has died with a `keep` or `mark-as-*` policy (record retained, dead flag set) and task B is created with `heldBy: [A]`
- **THEN** the create call fails and B is not created

#### Scenario: Done-but-pinned holder is a valid holder
- **WHEN** task A is done but has `refCount > 0` (alive), and task B is created with `heldBy: [A]`
- **THEN** the acquisition succeeds

### Requirement: Idempotent re-creation skips ref acquisition

When the created task id already exists (idempotent re-offer, e.g. a replaying holder re-offering a child), the engine SHALL skip ref acquisition entirely: no refCount increment, no `refs` append, and no error regardless of the `heldBy` contents.

#### Scenario: Replay does not double-pin
- **WHEN** task B was created with `heldBy: [A]` and the same create call is issued again
- **THEN** B's `refCount` remains 1 and A's `refs` contains B's reference exactly once

### Requirement: Task death

A task SHALL be considered dead when it is done (terminal success, or terminal failure with no retry pending) and its `refCount` is 0. The engine SHALL evaluate this condition wherever either conjunct can become true: on success write, on terminal failure, and on each refCount decrement. While a task is done but `refCount > 0`, the engine SHALL keep its record with the terminal outcome recorded, place it in no list, and defer its policy.

#### Scenario: Unpinned task dies at completion
- **WHEN** a task with `refCount` 0 completes
- **THEN** it dies immediately and its outcome policy applies at completion time

#### Scenario: Pinned task defers death
- **WHEN** a task with `refCount` 1 completes successfully with `onSuccessPolicy` of `mark-as-success`
- **THEN** the task appears in no list, `getTask` still returns it with its result, and the `success` list does not contain it

#### Scenario: Last release triggers death
- **WHEN** the same task's `refCount` reaches 0 because its holder died
- **THEN** the task dies and appears in the `success` list

### Requirement: Death releases held refs and cascades

At death, after applying the outcome policy, the engine SHALL decrement the `refCount` of every task in the dying task's `refs` and re-evaluate death for each, cascading iteratively until no further tasks die. Refs SHALL be released at death, not at record deletion; dead retained records hold no refs and their later deletion SHALL NOT involve ref bookkeeping.

#### Scenario: Holder death releases child
- **WHEN** holder A (pinning done task B, both `delete` policy) terminally completes
- **THEN** A dies, B's `refCount` drops to 0, B dies, and both records are deleted

#### Scenario: Cascade through a pipeline
- **WHEN** A pins B, B pins C, all are done with `delete` policy and A's `refCount` is 0
- **THEN** A's death cascades: B and C die in the same operation and all three records are deleted

#### Scenario: Kept dead record is inert
- **WHEN** a task dies with `keep` policy while holding no refs, and its record is later removed via `removeTask`
- **THEN** the removal is a plain record deletion with no ref decrements

### Requirement: removeTask rejects pinned tasks and forces death of unpinned ones

`removeTask` on a task with `refCount > 0` SHALL fail with an error and change nothing — a pinned task's holders may still read it; the holders must be removed first. `removeTask` on an unpinned alive task SHALL run the death behavior (release held refs with cascade) before deleting the record, regardless of whether the task is done.

#### Scenario: Removing a pinned task is illegal
- **WHEN** task B has `refCount > 0` and `removeTask(B)` is called
- **THEN** the call fails and B's record is unchanged

#### Scenario: Force-removing a holder releases its children
- **WHEN** unpinned alive task A pins done task B (`delete` policy) and `removeTask(A)` is called
- **THEN** A's record is deleted and B dies (its `refCount` reached 0)

### Requirement: createdBy provenance field

The engine SHALL accept an optional `createdBy` task reference (`{prefix, id}`) at creation, store it on the task hash, and return it when the task is read. The field SHALL have no effect on task lifecycle, refs, or scheduling, and SHALL NOT be validated against existing tasks.

#### Scenario: createdBy is stored and returned
- **WHEN** task B is created with `createdBy` referencing task A
- **THEN** `getTask(B)` includes the `createdBy` reference

#### Scenario: createdBy outlives the creator
- **WHEN** task A is deleted after task B was created with `createdBy: A`
- **THEN** `getTask(B)` still returns the `createdBy` reference unchanged
