# Durable application events

Use `EventQueue` when several independent consumers must each acknowledge an
emitted event. Each named subscription receives its own delivery. Multiple
workers using the same subscription name share that subscription's work.

## Emit and acknowledge an event

Register subscriptions before emitting events. This example registers `billing`
and `email`, emits one order event, and processes both obligations:

```ts
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { EventEngine, EventQueue } from "@effectmq/core";

const orders = EventQueue.make(
  "orders",
  Schema.Struct({ orderId: Schema.String }),
  { onCompletion: "archive" },
);

const program = Effect.gen(function* () {
  const billing = yield* EventQueue.subscribe(orders, "billing");
  const email = yield* EventQueue.subscribe(orders, "email");
  const event = yield* EventQueue.emit(orders, { orderId: "order-123" });

  yield* EventQueue.processOne(orders, billing, (event) =>
    Effect.log(`Bill ${event.payload.orderId}`),
  );
  yield* EventQueue.processOne(orders, email, (event) =>
    Effect.log(`Email receipt for ${event.payload.orderId}`),
  );

  const archived = yield* EventQueue.get(orders, event.id);
  yield* Effect.log(archived?.status); // completed after both acknowledgements
});

program.pipe(
  Effect.provide(EventEngine.layer({ redis: { url: "redis://localhost:6379" } })),
  NodeRuntime.runMain,
);
```

`subscribe` is durable and idempotent within a queue: calling it again with
`billing` returns the same active generation. Restarting a worker preserves
outstanding deliveries. A subscription created after emission receives future
events only. Emitting with zero subscriptions completes immediately.

## Run a consumer continuously

`processOne` returns `false` if no delivery is currently available. Repeat it with
a polling interval, and run maintenance even while consumers are idle:

```ts
import { Effect, Schedule, Schema } from "effect";
import { EventQueue } from "@effectmq/core";

const orders = EventQueue.make(
  "orders",
  Schema.Struct({ orderId: Schema.String }),
  { onCompletion: "archive" },
);

const consumer = Effect.gen(function* () {
  const billing = yield* EventQueue.subscribe(orders, "billing");
  yield* EventQueue.runMaintenance(orders).pipe(Effect.forkChild);
  yield* EventQueue.processOne(orders, billing, (event) =>
    Effect.log(`Bill ${event.payload.orderId}`),
  ).pipe(Effect.repeat(Schedule.spaced("250 millis")));
});
```

Provide `EventEngine.layer` to run this consumer. The child maintenance fiber
stops when its parent stops. Storage errors and typed handler failures propagate;
choose your application's logging/retry policy around the consumer effect.
A handler failure releases its delivery with a one-second retry delay by default.
It does not resolve the subscription's obligation.

Managed processing renews the lease and interrupts the handler if renewal fails.
A crash or interruption leaves the delivery recoverable after its lease expires.
Delivery is **at least once**: make external side effects idempotent using the
event id and subscription identity. Registration idempotency does not deduplicate
emissions; every `emit` call creates a new event.

## Control deadlines and retention

Queue options are persisted at first use; every process using that queue must
supply the same policy. Conflicting definitions fail explicitly.

| Option | Default | Meaning |
| --- | --- | --- |
| `onCompletion` | `"delete"` | Delete settled events or retain them with `"archive"`. |
| `ttlMs` | `null` | Delivery lifetime; null waits indefinitely. |
| `archiveRetentionMs` | `null` | Archive lifetime after settlement; null retains indefinitely. |

`emit(queue, payload, { ttlMs })` overrides the queue's delivery lifetime for that
event. An explicit `null` disables a queue deadline. An expired event remains
`expired` when archived; archival does not turn it into a successfully completed
event. Archived records include recipient statuses and the decoded payload.

Use `get(queue, id)` to inspect an event and `listArchived(queue, { offset, limit })`
to list retained archive ids. Offsets are best-effort positions, not snapshot
cursors: concurrent settlement or cleanup can shift them. Reading an event also
applies its deadline and any subscription removals. A deleted or retention-expired
record returns `null`.

## Remove a subscription

Call `unsubscribe(queue, subscription)` with the handle returned by `subscribe`.
It immediately excludes that generation from future emissions and waives its
outstanding obligations. Waivers are recorded separately from acknowledgements.
Reusing the name creates a new generation that cannot receive the old events;
stale handles cannot remove the replacement subscription.

Removal performs bounded cleanup. `maintain` or `runMaintenance` finishes a large
backlog, and inspecting an affected event resolves its waivers immediately. A
removal before the event deadline can complete it even if cleanup runs later.
Disconnecting a worker does not unsubscribe it.

## Manual delivery and operational bounds

Use `take(queue, subscription, { leaseMs })`, `acknowledge(queue, delivery)`,
`renew(queue, delivery, leaseMs)`, and `release(queue, delivery, delayMs)` when you
need manual control. Pass delivery handles unchanged. Acknowledgement resolves
only that subscription; duplicates cannot count twice. Stale attempts fail with
`LeaseLost`. Acknowledging a deleted event returns `gone` without changing state.

Names must contain 1–256 UTF-8 bytes. A queue supports at most 1,000 active
subscriptions, and encoded payloads use the existing 1 MiB storage-envelope
limit. Durations are integer milliseconds, up to 100 years; leases must be
positive. Processing order is not guaranteed. Events with no deadline and
indefinite archives require explicit capacity planning.

`EventEngine.layer({ engine: { maintenanceBatchSize: 100 } })` processes at most
that many events per cleanup category in each sweep (expiration, archive
retention, and removed subscriptions). The allowed batch range is 1–1,000.
`maintain` returns `{ processed, pending }`; repeat while `pending` to drain
currently due work. `runMaintenance` does this automatically and otherwise polls
every second. Bounded acquisition can return `null` after cleaning stale entries
even if later entries remain; keep polling.

Redis transport errors carry `EventEngineError` with code `IndeterminateWrite`,
the operation, and the event id when applicable. The operation may have committed.
For an uncertain emission, inspect that id before deciding to emit again; full
deletion can make the final outcome unknowable. Do not blindly retry and assume
producer deduplication. Corrupt storage and schema failures remain visible.

The standard layer supports the same standalone and Sentinel deployments as task
queues. See [Redis operations](./operations.md) for persistence, `noeviction`,
backup, and failover requirements. `layerNoDeps` accepts a custom `RedisPool`;
provide Effect's `Crypto` service for subscribe, emit, take, and managed processing.
