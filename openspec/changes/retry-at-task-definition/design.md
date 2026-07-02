## Context

Retries used to be a per-offer `maxRetries` counter: on failure the Lua `failTask` decremented against the count and re-queued to **wait** immediately. That put a scheduling decision at the call site and offered no control over *when* a retry runs.

The work-in-progress on the branch already moves retry policy onto the `TaskDefinition` and derives the next run time from an Effect `Schedule`:

- `src/Task.ts` — `Task.make` now takes `retry` (a `Schedule` or `{ while, until, times, schedule }`), stored as `retrySchedule`; also renamed `successSchema`/`errorSchema` config keys to `success`/`error`.
- `src/utils.ts` — `buildFromOptions` normalizes the options object into a `Schedule`; `nextRunAt` steps the schedule over the error history to produce the next timestamp.
- `src/TaskQueue.ts` — `fail` computes `retryAt` from `nextRunAt` and passes it to `engine.writeError`.
- `src/TaskEngine.ts` — Lua `failTask` routes on `retryAt`: future → **scheduled**, past → **wait**, else apply failure policy; `Canceled` skips retry.
- `src/Schemas.ts` — `retryAt` added to the `task.failed` event payload.

The code works but is unfinished: dead code (`Effect.retry;`, commented generics, `TODO`), debug lines (`console.log("retryAt", ...)`, verbose `redis.log`), a lint rule flipped (`noExplicitAny: off`), broken tests, and stale docs. There is also a correctness gap: `makeInternal` sets `maxRetries: config.maxRetries === null ? Infinity : 5`, ignoring a passed number.

This design covers finishing that migration cleanly and locking down the public surface.

## Goals / Non-Goals

**Goals:**
- Retry policy lives on the `TaskDefinition`, expressed as an Effect `Schedule`.
- Failed retryable tasks land on the **scheduled** list at the computed time.
- `maxRetries` is a hard cap (queue default, per-offer override) that bounds unbounded schedules.
- Reduce the public API: drop `takeUnsafe`, `succeed`, `fail`; keep `complete` as the managed path.
- Green tests, updated README, formatted code, no dead code.

**Non-Goals:**
- Changing the completion-policy model or the event-stream shape beyond adding `retryAt`.
- Changing the Scheduler (cron) API.
- Reworking the Lua engine's locking or list model beyond the fail-routing path.

## Decisions

### Schedule as the retry primitive
Use Effect's `Schedule` (keyed on the error type) as the single source of retry timing, normalizing the `{ while, until, times, schedule }` sugar into one `Schedule` via `buildFromOptions`. Rationale: reuses a composable, well-understood Effect primitive (backoff, jitter, predicates) instead of a bespoke counter; consistent with the library's "it's just Effect" framing. Alternative considered: keep the numeric counter and add a separate delay function — rejected as a strictly weaker, redundant API.

### Next run time computed from full error history
`nextRunAt` replays the schedule from `createdAt + delay` across all recorded errors to derive the next timestamp, and the engine stores/acts on `retryAt`. Rationale: the schedule state is not persisted between attempts, so re-deriving from the durable error log keeps it stateless and correct across workers/restarts. Trade-off: O(attempts) replay per failure — negligible given `maxRetries` caps attempt count.

### maxRetries is a cap, not the driver
Default to *not* looping forever: an unbounded schedule (`Schedule.forever`) is bounded by a queue-level `maxRetries` default of **5**, overridable per-offer, per-offer winning. Setting `maxRetries` to `null` or `Infinity` disables the cap for truly unbounded retries. Rationale: schedules can be infinite; a safe default must terminate. Fix the current `makeInternal` bug so a passed `maxRetries` number is honored (currently hardcodes `5` regardless of input) and so `null`/`Infinity` map to no cap.

### Remove take/succeed/fail from the public surface
`takeUnsafe`/`succeed`/`fail` become internal (used by `complete`). Rationale: with retry-on-fail now driven through `fail`, hand-driving the lifecycle exposes a confusing partial flow (e.g. calling `succeed` without the heartbeat, or `fail` without going through retry routing). `complete` is the one correct path. Keep them as non-exported module functions so `complete` still composes them.

### Debug-only Lua source in error messages
The `ev` helper in `TaskEngine.make` currently prepends the full numbered Lua source to every `TaskEngineError`. Gate that behind `debugMode`: production errors carry just the message, debug builds carry the lined source. Rationale: the lined source is a valuable debugging aid but noisy and leaks script internals into every error in production. Also delete the leftover `redis.log` debug calls (dead in all modes). Alternative considered: always include the source — rejected as noisy; deleting it entirely — rejected because it's genuinely useful under `debugMode`.

### retryAt on the error entry and event
Add `retryAt` to each stored error entry (`appendTaskError`) and to the `task.failed` event payload (`Schemas.ts`), optional/absent when no retry is scheduled. Rationale: observability — consumers can see the scheduled retry time from the stream and from task history.

## Risks / Trade-offs

- **Removing public primitives / renaming config keys** → Not yet released, so there are no external consumers to migrate; just update call sites, tests, and README. Point users to `complete`.
- **Schedule replay assumes error timestamps are accurate and ordered** → Errors are appended with the engine's `now`, so ordering holds; `nextRunAt` steps chronologically.
- **`retryAt` stored inside the JSON error blob** → Kept optional so old/absent entries decode; schema change is additive.
- **Default cap of 5 could surprise users who genuinely want infinite retries** → `maxRetries: null` / `Infinity` disables the cap; document this clearly in the Retries note.

## Migration Plan

1. Finish `Task.make` / `makeInternal`: honor a numeric `maxRetries`, set the default cap, keep the `Schedule`/options overloads.
2. Confirm `fail` cap logic (queue default vs. per-offer `-1` sentinel → override) and the `nextRunAt` inputs.
3. Un-export `takeUnsafe`/`succeed`/`fail`; verify `complete` still resolves them internally; update the package entry/index exports.
4. Clean up: delete dead code and debug logging, restore/relax `biome.json` intentionally, run the formatter across touched files.
5. Fix `TaskQueue.test.ts` / `TaskEvents.test.ts` and add coverage for schedule-timed retries, the `maxRetries` cap (default + override), and `Canceled` short-circuit.
6. Update `README.md`: Retries note (definition-level + schedule + cap), the Define-a-task example (`success`/`error`), and remove any `take`/`fail`/`succeed` references.

Rollback: revert the branch; changes are contained to the six source files, tests, README, and `biome.json`.

## Open Questions

- None outstanding. Default cap is **5**; `maxRetries: null` / `Infinity` opts into unbounded retries.
