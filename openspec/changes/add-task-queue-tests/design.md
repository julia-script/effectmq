## Context

`src/TaskQueue.ts` is the typed API over `TaskEngine`. A queue is built from a `Task.make({ name, payload, successSchema, errorSchema, idempotencyKey })` definition plus `TaskQueue.make(name, definition)`. The lifecycle: `offer(queue, payload, opts)` encodes the payload to JSON and calls `engine.createTask`; `complete(queue, handler)` calls `takeUnsafe` (which polls `engine.takeTask` and decodes the typed task), forks a lock-refresh heartbeat, runs the handler, then calls `succeed`/`fail` and returns `true`/`false`.

Tests run against the same real-Redis `TestRuntime` and `getLists` helper already used by `TaskEngine.test.ts`, with deterministic mock time.

## Goals / Non-Goals

**Goals:**
- Cover `complete` end-to-end for both handler outcomes (success → `true`, failure → `false`).
- Confirm the handler receives the *decoded* typed payload, not the raw JSON string.
- Confirm `offer` lands a task with the deterministic id on the wait list.

**Non-Goals:**
- Not exhaustively testing each primitive (`extendLock`/`release`/`takeUnsafe`) in isolation — `complete` exercises them transitively; per the chosen scope this change is complete-focused.
- Not testing the poll-interval backoff timing or heartbeat cadence precisely (real timers + forked fiber); we only need the handler to run once on an available task.

## Decisions

- **Deterministic `idempotencyKey`.** Each test queue is built with `idempotencyKey: (p) => p.<field>` so offered tasks get stable, assertable ids and the wait-list assertions are exact.
- **Offer immediately before complete, so `takeUnsafe` finds a task on the first poll.** `complete` loops with `Effect.sleep(poolInterval)` (default 1s) only when the wait list is empty; offering first means it returns on iteration one and the test never waits a real second.
- **Use `mark-as-failure` for the failure test** so the routed task is observable on the failed list via `getLists`, distinguishing "failed and persisted" from "deleted".
- **Assert the handler's view of the payload inside the handler** (capture into a ref/closure or assert directly), since that is where the decoded `Task` is available.
- **Distinct queue `name` (prefix) per test** for isolation on the shared container, mirroring the TaskEngine tests.

## Risks / Trade-offs

- **Forked lock heartbeat fiber.** `complete` forks `extendLock` on a 10s schedule and interrupts it after the handler. With a short test and mock time this fires at most once; `Fiber.interrupt` cleans it up. → If a test hangs, suspect the heartbeat or the poll loop; keep handlers synchronous-returning to avoid sleeping into a real poll interval.
- **`complete` swallows handler errors into a `false` return** (it uses `Effect.result`). → That is the contract under test, not a defect; the failure test asserts both the `false` return and the engine-side routing.
- **Possible latent bugs in the typed encode/decode path** (payload/success/error JSON round-trip through `Schema.fromJsonString`). → If decode fails, that is a real `TaskQueue` bug to report (same posture as the TaskEngine change), not something to paper over in the test.

## Open Questions

- None blocking. If `complete` proves hard to drive deterministically because of the real poll sleep, fall back to asserting the `offer → takeUnsafe → succeed/fail` primitives directly and note `complete` as partially covered.
