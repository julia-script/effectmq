# Idempotent offers

`effectmq` derives a task id from the task definition's `idempotencyKey`. By default, offering the same queue/id again returns the existing task generation unchanged. It does not replace its payload, retry history, lease, state, relationships, or outcome.

If a producer loses its Redis connection while sending an offer, it may be impossible to know whether Redis committed the script before the connection failed. `TaskQueue.offer` reports this as `IndeterminateWriteError` with the queue and task id.

Retry the same payload and idempotency identity. If the first write committed, the retry returns `TaskExisting` and its original handle. If it did not commit, the retry creates the generation and returns `TaskCreated`.

Do not switch to `onDuplicate: "new-generation"` when recovering an indeterminate offer. That mode means “run this logical id again after its current generation has settled” and is a separate, deliberate operation.
