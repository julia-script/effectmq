# Delivery guarantees

EffectMQ provides fenced, at-least-once task execution. A task generation has
one current lease token, but its handler may run more than once when a process
loses ownership after performing an external side effect and before Redis
records the acknowledgement. Handlers must therefore make side effects
idempotent or use a downstream idempotency key derived from queue, task id, and
generation.

## What the lease guarantees

Acquiring work creates a unique opaque token for that attempt. Only the current
token may renew, succeed, fail, or voluntarily release the task. A late attempt
gets `LeaseLost` and cannot overwrite a newer attempt's state. Redis server time
sets lease deadlines, so application clock skew does not determine ownership.

Heartbeats extend the current lease. The managed processing loop races the
handler against heartbeat supervision and interrupts the handler when ownership
cannot be established safely. Transport retries are finite. If a worker dies or
stops heartbeating, bounded maintenance recovers the expired lease. Recovery
increments `stalledAttemptCount`; after `maxStalledCount` the task terminates
with the built-in `~effectmq/Error/Stalled` failure.

These rules prevent stale acknowledgements. They cannot retract an email,
payment, webhook, or database write that the stale handler already performed.

## Producer outcomes

`TaskQueue.offer` returns `TaskCreated` or `TaskExisting`, both carrying an
authoritative `TaskHandle`. The default duplicate behavior never changes the
existing generation. If the connection fails while a write may have committed,
the producer receives `IndeterminateWriteError`; retry the same task identity.
See [Idempotent offers](./idempotent-offers.md).

## Waiting and events

Lifecycle events and state changes are committed by one Redis script. Events
are retained, bounded Redis Stream records, not an infinite replay log.
`TaskQueue.wait(queue, handle)` first reads durable state, subscribes from the
handle's authoritative cursor, and rechecks state after subscribing. It handles
completion before or during subscription without a lost-wakeup window.

Callers can receive distinct task failure, task-not-found, result-expired,
cursor-expired, schema/protocol, and caller-timeout errors. A caller timeout
does not cancel the task. Event consumers must persist their cursor and handle
`CursorExpired` by reconciling durable task state.

## Failure table

| Failure | Observable outcome | Required application behavior |
| --- | --- | --- |
| Producer response lost | `IndeterminateWriteError` | Retry the same queue/id |
| Worker dies before side effect | Lease expires; task is retried | No special action |
| Worker dies after side effect | Handler may run again | Make side effect idempotent |
| Heartbeat loses ownership | Handler is interrupted; stale ack is fenced | Treat `LeaseLost` as final for that attempt |
| Redis primary fails | Sentinel reconnect; in-flight write may be indeterminate | Reconcile by task identity |
| Event history trimmed | `CursorExpired` with earliest cursor | Re-read durable state, then resume |
| Result retention expires | `ResultExpired` | Use an external durable result store if needed longer |

Scheduling has the same guarantee. A schedule materializes deterministic queue
tasks; it does not execute handlers itself and does not strengthen delivery.
