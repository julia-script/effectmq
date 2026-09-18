# Design

## Context

See proposal.md for motivation. Existing task delivery uses Redis Lua for atomic
state transitions and fenced ownership, with schema encoding through the versioned
StorageProtocol. Event subscriptions need independent recipient state rather than
a task's single execution owner. This is a new persistent data model, so a design
artifact is required.

## Goals / Non-Goals

**Goals:** Reuse Redis adapters and storage envelopes; preserve honest Effect
error/service channels; make races deterministic and maintenance resumable.

**Non-Goals:** Historical replay to late subscriptions, exactly-once side effects,
producer deduplication, strict processing order, and task lifecycle API changes.

## Decisions

1. Introduce EventQueue (typed operations), EventEngine (atomic storage service),
   and EventRecord (public record schemas). Supply dependency-free and standard
   Node layers. This avoids coupling multi-recipient state to TaskEngine retries.
2. Queue handles define payload schema, onCompletion (delete by default), optional
   ttlMs, and optional archiveRetentionMs (indefinite by default). A null per-event
   ttlMs disables a queue deadline. Persist a canonical queue policy on first use;
   subsequent conflicting definitions fail. This avoids per-worker policy drift.
3. Use a separate versioned Redis namespace and a hash-tagged queue prefix, a hash
   of active name-to-generation registrations, one JSON record per event, and one
   sorted delivery index per subscription generation. UUIDs come from the Effect
   Crypto service. Payloads remain opaque StorageProtocol envelopes rather than
   being interpreted by Lua. Record protocol versions and reply shapes are checked.
4. Snapshot recipients inside the emission script. Each event stores recipient
   name, generation, status, and lease state. Limit registrations to 1,000 active
   subscriptions per queue, bounding atomic fan-out and per-event work. Names are
   nonempty and at most 256 UTF-8 bytes. Payload limits reuse StorageProtocol.
5. Take returns one delivery with a 30-second lease by default. Redis time is
   authoritative. Due-time indexes allow lease recovery and explicit release;
   renewal and acknowledgement require the current token. Acknowledging an already
   acknowledged retained delivery with its original token is harmless; a deleted
   event returns a gone outcome. No ordering or exactly-once guarantee is implied.
6. Provide explicit take/acknowledge/release/renew and a managed process-one helper
   with supervised heartbeat and success acknowledgement. Failed handlers release
   the attempt with a retry delay and propagate their error; repeated processing is
   caller-controlled and does not silently exhaust a subscriber obligation.
7. Removal atomically drops the active name and records a retired generation for
   cleanup. Reads/transitions resolve removed recipients as waived. Bounded sweeps
   drain retired generations' delivery indexes, expiration deadlines, and finite
   archive-retention indexes. A large removal therefore has immediate logical
   effect without an unbounded script. Re-registration cannot revive old work.
8. Every transition resolves removal timestamps and deadlines before acting.
   Waivers that occurred before a deadline can complete an event even when cleanup
   runs later; otherwise expiration wins over late acknowledgements. Completion/expiry removes all delivery memberships; deletion removes
   the record, archival keeps it and its outcome. Archive TTL starts at settlement,
   independently of delivery TTL. Maintain and a scoped maintenance loop are public;
   run maintenance even when consumers are idle for timely physical reclamation.

## Risks / Trade-offs

- Fan-out costs scale with subscriber count → cap active subscriptions and payload
  size, batch maintenance, and document that throughput depends on fan-out.
- No-deadline events and indefinite archives can grow without bound → explicit
  configuration and documented operational ownership; never silently evict them.
- Network failure after mutation can leave an uncertain result → preserve typed
  indeterminate-write errors with operation/event identity; inspect that identity
  before retrying emission and do not claim producer deduplication.
- Full deletion cannot retain acknowledgement receipts → repeated acknowledgements
  of deleted ids return gone without changing state.
- Maintenance must keep running for unattended queues → provide an interruptible
  loop; event reads and transitions also enforce terminal semantics.

## Migration Plan

This is additive: new exports and a distinct Redis namespace require no task data
migration. Deploy consumers that register durable names before producers emit.
Rollback stops new event producers/consumers and leaves their namespace intact;
existing task workers remain unaffected.
