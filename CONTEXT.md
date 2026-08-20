# effectmq

An Effect-native durable task queue for running typed work through Redis with explicit at-least-once delivery, ownership, retry, retention, and operational semantics.

## Language

**Task**:
A typed unit of durable work offered to a named queue. A task describes what may be attempted; it does not promise that its handler runs only once.
_Avoid_: Job, exactly-once operation

**Task identity**:
The stable queue-local id used to recognize repeated offers of the same logical task. Reusing it is an idempotency decision, not an instruction to overwrite work already in progress.
_Avoid_: Mutable record key, execution id

**Task generation**:
One complete execution lifecycle for a task identity. An intentional later run under the same task identity is a new generation and cannot inherit ownership, history, relationships, or outcome accidentally.
_Avoid_: In-place reset, retry

**Task handle**:
A typed reference to exactly one task generation that a caller may inspect or await. A handle identifies durable work; it is not a promise that must stay in process memory.
_Avoid_: Promise, task result

**Task attempt**:
One opportunity for a worker to run a task generation while holding its lease. A retry or recovery creates another attempt, so handler code and external effects may be observed more than once.
_Avoid_: Task generation, exactly-once execution

**At-least-once delivery**:
The guarantee that eligible work remains recoverable after ownership expires, with the consequence that an attempt may be repeated when completion cannot be proven. Consumers make non-repeatable external effects safe through idempotency or an inbox/outbox boundary.
_Avoid_: Exactly-once handler execution, exactly-once side effects

**Lease**:
Temporary, exclusive permission for one task attempt to report progress or an outcome. A lease establishes current ownership but cannot prove that a previous attempt performed no external side effect.
_Avoid_: Permanent lock, worker identity

**Lease token**:
The opaque identity of one lease. A token from an earlier attempt has no authority after ownership changes.
_Avoid_: Worker id, task id

**Lease loss**:
The condition in which a task attempt can no longer prove that it is the current owner. Managed work is interrupted and a late outcome is rejected, while already-performed external effects remain the consumer's responsibility.
_Avoid_: Handler failure, cancellation request

**Handler failure**:
An expected typed failure reported by the current task attempt and governed by the task's retry policy. It is distinct from lease loss, Redis unavailability, interruption, and defects.
_Avoid_: Stall, ownership loss, defect

**Stalled attempt**:
An attempt whose ownership expires before a valid outcome is recorded. Stalls have their own bounded recovery policy and are not passed through the handler's typed-error retry policy.
_Avoid_: Handler failure, ordinary retry

**Settlement**:
The durable point at which a task generation has succeeded or failed and will not become runnable again. Settlement determines execution outcome; it does not by itself determine how long the record or result remains stored.
_Avoid_: Death, disposal, deletion

**Disposal**:
Removal of a settled task generation's retained record and outcome after its policy and active retention holds allow it. Disposal is a storage-lifetime decision, not an execution transition.
_Avoid_: Settlement, task death

**Completion policy**:
The declared storage and terminal-index treatment applied after settlement, subject to explicit result-retention holds and configured expiry.
_Avoid_: Execution dependency, retry policy

**Creator**:
The task generation whose managed handler offered another task. Creator provenance explains where work came from but creates no retention, ordering, joining, cancellation, or failure propagation.
_Avoid_: Parent, owner, retention holder

**Spawned task**:
A task offered while another managed task is running. It may record its creator, but executes independently unless a future explicit execution relationship says otherwise.
_Avoid_: Child task, subtask

**Result-retention hold**:
An explicit relationship that keeps a settled task generation's result readable until a named live holder settles or releases the relationship. It controls disposal only.
_Avoid_: Pin, execution dependency, parent/child link

**Retention holder**:
The live task generation on whose behalf a result-retention hold exists. Two holders retain independently, and replaying the same relationship does not acquire it twice.
_Avoid_: Creator, task owner, lease holder

**Execution dependency**:
A relationship that would govern whether tasks wait for, cancel, join, or propagate failure to one another. effectmq's production queue model does not infer such a relationship from creator provenance or result retention.
_Avoid_: Retention hold, creator link

**Child task**:
A reserved term for a future task whose lifecycle is structurally owned by another task through explicit joining, cancellation, or failure semantics. Ordinary tasks offered from a handler are spawned tasks, not child tasks.
_Avoid_: Any nested offer, creator provenance only

**Task result**:
The typed success or typed terminal failure of one settled task generation while that outcome remains retained. Missing work, expired results, ownership loss, and protocol incompatibility are distinct operational outcomes.
_Avoid_: Any terminal API response, event only

**Lifecycle event**:
A durable observation that one task generation changed execution state or attempt status. Events support replay and observability within their retention window; the task generation's durable state remains the authority for its current outcome.
_Avoid_: Source of truth, unbounded audit log

**Event cursor**:
A position in the retained lifecycle-event history used to begin or resume observation. A cursor may expire and does not identify a task generation by itself.
_Avoid_: Task handle, wall-clock timestamp

**Scheduled tick**:
The nominal occurrence of a named schedule, materialized as an idempotently identified ordinary task. Tick creation can be deduplicated while its handler execution remains at-least-once.
_Avoid_: Exactly-once scheduled execution, in-process timer callback

**Storage protocol**:
The versioned durable meaning shared by compatible effectmq processes for task metadata and opaque typed values. Compatibility is declared explicitly rather than inferred from package version similarity.
_Avoid_: Codec alone, Redis implementation detail

**Operational error**:
A typed failure of queue infrastructure or protocol behavior, such as lease loss, result expiry, cursor expiry, corruption, incompatibility, or an indeterminate write. It is not a user handler's typed failure.
_Avoid_: Task failure, defect
