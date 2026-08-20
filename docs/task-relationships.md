# Task relationships

EffectMQ deliberately has no execution parent/child relationship. Offering a
task from inside another handler records immutable creator provenance for
diagnostics, but does not join, cancel, fail, or otherwise control either task.

The only behavioral relationship is explicit result retention:

```ts
import { Task, TaskQueue } from "@effectmq/core"
import { Effect, Schema } from "effect"

const ChildTask = Task.make({
  name: "child-task",
  payload: { childId: Schema.String },
  success: Schema.Void,
  error: Schema.Never,
  idempotencyKey: ({ childId }) => childId
})
const childQueue = TaskQueue.make("children", ChildTask)

const retainChild = Effect.gen(function* () {
  yield* TaskQueue.offer(
    childQueue,
    { childId: "child-42" },
    { retainResultUntil: "current-task-settles" }
  )
})
```

This option is valid only inside a live managed task context. It creates an
idempotent relationship from the current holder generation to the offered
generation. Several holders may retain the same result independently. Replaying
the offer does not duplicate the relationship.

Settlement is immediately visible to waiters. A hold postpones record/result
disposal only; it does not keep the retained task active and does not delay its
success or failure event. When the holder settles or is removed, its holds are
released in bounded durable continuation batches. Cleanup can resume after a
crash without processing an unbounded set in one Lua invocation.

Ordinary removal rejects a generation with active incoming holds. The
administrative `TaskEngine.forceRemoveTask` operation revokes leases and removes
retained data despite relationships; reserve it for incident recovery because
holders may subsequently observe `TaskNotFound` or `ResultExpired`.

Creator provenance and result retention are separate concepts. Neither means:

- wait for another task before settling;
- propagate success, failure, retry, cancellation, or removal;
- keep a worker process alive;
- prevent duplicate handler execution;
- form a workflow or saga.

Use Effect Workflow or an application-level orchestration model when the
relationship itself needs durable execution semantics.
