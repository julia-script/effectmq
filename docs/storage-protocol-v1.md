# Storage protocol v1

EffectMQ stores user payloads, successful results, and typed failures as opaque
v1 envelopes. Redis and the Lua state engine do not interpret these values.

## Compatibility contract

- This release reads protocol version 1 and writes protocol version 1.
- The only declared rolling-deployment pair is a v1 reader with a v1 writer.
- A future writer version is not rollable until the previous reader can decode
  its committed golden fixtures. Otherwise, workers must be stopped and stored
  data migrated before the new writer starts.
- `schemaId` identifies a queue's payload/success/failure schema family. It
  defaults to the task name, but applications should set a stable explicit id
  when a task may be renamed. A mismatched id is an error, never a best-effort
  decode.

## Envelope

The canonical MessagePack tuple is:

```text
[1, schemaId, kind, value]
```

`kind` is `payload`, `success`, or `failure`. The bytes are stored as the ASCII
string `effectmq:v1:` followed by canonical base64. The outer ASCII form is
intentional: Redis Lua's bundled `cmsgpack` does not preserve nested binary
strings reliably when it decodes and re-encodes failure history. Lua can append
the envelope without opening it, while the TypeScript boundary remains the
only user-value codec.

Task hashes and lifecycle events also carry `protocolVersion` and `schemaId`
as queryable metadata. The committed fixture in
`src/testing/fixtures/storage-v1.json` is the byte-stability authority for v1.
Production writes use the `~effectmq:v1:` Redis key namespace. Before enabling
them, run `pnpm storage:inspect -- --assert-drained` against the deployment's
`EFFECTMQ_REDIS_URL`. This command is read-only, lists pre-v1 key names without
reading their values, and fails when the old namespace has not been drained.

## Supported value domain

After a task's Effect Schema has encoded it, v1 accepts:

- `null`, strings, booleans, and `Uint8Array`
- finite numbers whose magnitude is at most `Number.MAX_SAFE_INTEGER`
- arrays containing supported values
- plain string-keyed objects containing supported values

Empty arrays and objects, nested nulls, Unicode, binary bytes, and fractional
safe numbers round-trip losslessly. `undefined`, `bigint`, non-finite and unsafe
numbers, class instances, symbols, functions, and cyclic objects are rejected.
Encoded user values are limited to 1 MiB by default. A task definition can
override `storageLimits.maxValueBytes`; the same bound applies independently to
payload, success, and typed-failure envelopes.

Error history keeps the newest 100 entries by default and discards the oldest
before appending beyond `storageLimits.maxErrorEntries`. Each task generation
may participate in at most 1,000 outgoing retention holds and 1,000 incoming
holders by default; `storageLimits.maxRelationships` configures both sides.
Relationship replay stays idempotent at the cap, while a genuinely new hold
fails with `StorageCountLimitExceeded`.

Lifecycle streams use approximate Redis `MAXLEN` trimming and retain 10,000
entries per queue by default. `storageLimits.maxEventEntries` configures this
bound; the engine exposes the earliest retained cursor and fails an expired
resume with `CursorExpired`.

## Bounded inspection

`TaskEngine.listTasks` returns at most 100 ids by default and rejects limits
above 1,000. Pass the returned `nextCursor` unchanged to read the next page.
The waiting list is ordered FIFO. Scheduled, active, success, and failed pages
are ordered by ascending Redis score, with task id as the tie-breaker. Pages
are live inspection views rather than snapshots, so concurrent task movement
may change later offsets.

## Errors and reserved tags

Malformed envelopes, wrong tuple shapes, wrong kinds, unsupported versions,
schema mismatches, unsupported values, and size violations are distinct typed
errors. They are not converted to missing values or empty collections.

The v1 engine reserves these canonical built-in error tags:

- `~effectmq/Error/Stalled`
- `~effectmq/Error/Canceled`

Built-in errors are protocol control values. A task's own typed failures always
use a `failure` envelope and cannot be mistaken for them.
