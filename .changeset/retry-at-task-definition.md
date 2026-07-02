---
"@effectmq/core": minor
---

Move retry configuration to the task definition and drive it with Effect `Schedule`.

`Task.make` now takes a `retry` option — an Effect `Schedule` (or a `{ while, until, times, schedule }` options object) — that decides when a failed task is retried. On failure the next run time is computed from the schedule and the task lands on the scheduled list until then; when the schedule is exhausted, the failure policy applies. `maxRetries` caps the attempts so an unbounded schedule can't loop forever: it defaults to `5`, is overridable per-`offer` (the per-offer value wins), and can be set to `null` for truly unbounded retries. A `Canceled` error still skips remaining retries. Each stored error entry and the `task.failed` event now carry `retryAt`.

`Task.make`'s config keys `successSchema`/`errorSchema` are renamed to `success`/`error` (aligning with `payload`).

The manual queue primitives `takeUnsafe`, `succeed`, and `fail` are no longer exported — use `TaskQueue.complete`, which applies the definition's retry policy on failure.

Also fixes a double-JSON-encoding bug in success values and a double-decode in the `task.failed`/`task.completed` event schemas.
