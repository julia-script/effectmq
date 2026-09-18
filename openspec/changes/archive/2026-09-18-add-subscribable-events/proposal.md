# Proposal

## Why

Applications need to emit durable events that several independent subscribers
can process and acknowledge, with the event retained until every obligation is
resolved or an optional deadline expires. EffectMQ's existing task queues and
task-lifecycle streams do not express this per-subscription completion model.

## What Changes

- Introduce named, schema-typed event queues through which applications can emit
  events and register subscriptions, following the existing queue API conventions.
- Identify each durable subscription by its queue and subscription name.
  Registering an existing name is idempotent: it returns the same subscription
  without creating another recipient or resetting outstanding deliveries.
  Workers using the same subscription share its deliveries; each event requires
  one acknowledgement from that subscription, independent of worker count.
- Capture the registered subscription set atomically when an event is emitted.
  Only those subscriptions owe acknowledgements for that event. Subscriptions
  created later receive future emissions and do not acquire earlier events.
- Track acknowledgement separately for each event/subscription pair. An event
  completes when no acknowledgement obligations remain. Repeated
  acknowledgements must not settle another subscription's delivery or count twice.
- Preserve subscriptions and outstanding obligations across worker disconnects
  and restarts. Explicit subscription removal excludes it from future emissions
  and waives its outstanding obligations. Waived deliveries are distinguished
  from acknowledged deliveries; removing the last outstanding obligation can
  complete an event.
- Treat recreation of a removed subscription name as a new subscription
  generation. It receives future emissions without reviving waived deliveries,
  and stale acknowledgements cannot affect the new generation.
- Allow optional event expiration. An event without a deadline remains active
  indefinitely until its obligations are acknowledged or waived. An event whose
  deadline expires while obligations remain becomes terminal with an expired
  outcome, distinct from completion.
- Complete events emitted with zero subscriptions immediately and apply the same
  terminal retention policy used for other events.
- Configure deletion or archival at the event-queue level. Deletion fully removes
  the settled event and its associated delivery state; archival retains an
  inspectable terminal record, including whether it completed or expired and
  which obligations were acknowledged or waived. Archive retention is separate
  from the deadline for acknowledging an active event.

## Capabilities

### New Capabilities

- `event-subscriptions`: Durable, queue-scoped subscription names; idempotent
  registration; shared workers; disconnect persistence; explicit removal and
  safe recreation.
- `event-delivery`: Typed event emission, atomic recipient capture, independent
  per-subscription delivery and acknowledgement, and protection against duplicate
  or stale acknowledgements.
- `event-completion-policies`: Completion after all obligations resolve,
  subscription-removal waivers, optional expiration, immediate completion for
  zero recipients, and queue-configured deletion or archival.

### Modified Capabilities

None. These capabilities introduce application events alongside the existing
task-specific delivery, lifecycle-stream, and completion contracts.

## Impact

- Add public event-queue and subscription APIs, schema contracts, and supported
  exports consistent with the package's Effect module conventions.
- Extend Redis-backed storage and atomic transitions to persist subscriptions,
  event recipients, acknowledgement state, terminal outcomes, and archives.
  Maintenance must handle expiring events and terminal cleanup without expiring
  active events that have no deadline.
- Add documentation and coverage for concurrent registration/emission/removal,
  repeated registration and acknowledgement, worker restart, subscription name
  reuse, expiration races, zero-recipient emission, and both retention policies.
- Resolve exact API signatures, delivery retry and lease behavior, ordering,
  archive retention defaults, and storage layout during design. Idempotent
  subscription registration does not by itself promise idempotent emission or
  exactly-once handler side effects.
- No new external dependency or breaking change to the existing task APIs is
  proposed.
