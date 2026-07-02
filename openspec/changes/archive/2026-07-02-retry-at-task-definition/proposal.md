## Why

Retries were configured per-offer and driven by a bare `maxRetries` counter, which put a scheduling concern at the call site and gave no control over *when* a retry runs. Moving retry policy onto the `TaskDefinition` and expressing it with Effect's `Schedule` lets a task carry its own backoff/jitter policy once, and lets failed tasks land on the scheduled list at a computed time instead of immediately re-queuing. The migration left dead code, unformatted files, broken tests, and stale docs that need cleanup before release.

## What Changes

- Move retry configuration from `TaskQueue.offer` options to `Task.make` (the `TaskDefinition`), accepting either an Effect `Schedule` or a `{ while, until, times, schedule }` options object.
- On failure, compute the next run time from the definition's `Schedule` and the task's error history, and route the task to the **scheduled** list at that time (instead of straight back to **wait**).
- Keep `maxRetries` as a hard cap that bounds an otherwise-unbounded schedule (e.g. `Schedule.forever`): a queue-level default cap of **5**, overridable per-offer, with the per-offer value winning when set. Truly unbounded retries require explicitly setting `maxRetries` to `null` or `Infinity`. This prevents an unbounded schedule from looping forever by default.
- Rename `Task.make`'s `successSchema`/`errorSchema` config keys to `success`/`error`.
- Stop exposing the manual queue primitives `takeUnsafe`, `succeed`, and `fail` from the public API — the retry-on-fail flow makes hand-driving take/complete/fail confusing and error-prone. `complete` remains the supported managed path.
- Add a `retryAt` field to stored error entries and to the `task.failed` event payload so consumers can see when a retry is scheduled.
- Cleanup: remove dead code (commented-out generics, stray `Effect.retry`, `console.log`/`redis.log` debug lines, `TODO`s), format all touched files, and re-enable/relax the lint rules the migration flipped.
- Fix the broken tests and update the README to describe definition-level retries and the reduced surface.

## Capabilities

### New Capabilities
- `task-retry-policy`: How retries are declared on a task definition, how the next run time is derived from a `Schedule`, how `maxRetries` caps an unbounded schedule (queue default vs. per-offer override), and how `Canceled` short-circuits retries.

### Modified Capabilities
<!-- No existing main spec covers retries or the take/complete/fail primitives; the public-surface reduction is captured in the new capability and design. -->

## Impact

- **API**: `Task.make` config keys (`success`/`error`); removal of `takeUnsafe`/`succeed`/`fail` from public exports; `TaskOptions.maxRetries` semantics change (cap override, not the retry driver). Not yet released, so no external consumers to migrate.
- **Code**: `src/Task.ts`, `src/TaskQueue.ts`, `src/TaskEngine.ts` (Lua `failTask`/`appendTaskError`), `src/Schemas.ts` (`retryAt` on error entries + `task.failed` event), `src/utils.ts` (`buildFromOptions`, `nextRunAt`).
- **Tests**: `src/TaskQueue.test.ts`, `src/TaskEvents.test.ts`, plus new coverage for schedule-driven retry timing and the `maxRetries` cap.
- **Docs**: `README.md` (Retries note, Define-a-task example, any `take`/`fail`/`succeed` references).
- **Tooling**: `biome.json` lint rules touched during migration.
