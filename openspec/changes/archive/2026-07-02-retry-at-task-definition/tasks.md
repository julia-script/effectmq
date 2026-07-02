## 1. Task definition & retry construction

- [x] 1.1 In `src/Task.ts`, fix `makeInternal` so `maxRetries` resolves as: unset → default `5`, a number → that number, `null`/`Infinity` → unbounded (no cap). (Currently hardcodes `5` regardless of input.)
- [x] 1.2 Remove dead code in `src/Task.ts`: the stray `Effect.retry;`, commented-out generics, and the `TODO` comment.
- [x] 1.3 Confirm both `Task.make` overloads (Schedule vs. options object) normalize correctly through `buildFromOptions`; tighten types so the `retry` union is unambiguous.
- [x] 1.4 Review `src/utils.ts` `buildFromOptions`/`nextRunAt` for correctness and remove any leftover debug/unused branches.

## 2. Failure routing & cap logic

- [x] 2.1 In `src/TaskQueue.ts` `fail`, verify the cap resolution (queue default vs. per-offer `maxRetries`, per-offer wins) and remove the `console.log("retryAt", ...)`.
- [x] 2.2 Confirm `nextRunAt` is called with `createdAt + delay` and the full error history including the current failure.
- [x] 2.3 In `src/TaskEngine.ts` Lua `failTask`, verify routing: `retryAt` in future → scheduled, in past → wait, else failure policy; `Canceled` skips retry.
- [x] 2.4 In `src/TaskEngine.ts` `appendTaskError`, ensure `retryAt` is recorded on the stored error entry.

## 3. Schema & event changes

- [x] 3.1 In `src/Schemas.ts`, add `retryAt` (optional) to the stored error entry schema.
- [x] 3.2 Confirm the `task.failed` event payload carries `retryAt` and decodes correctly in `TaskQueue.stream`.

## 4. Reduce public surface

- [x] 4.1 Un-export `takeUnsafe`, `succeed`, and `fail` from `src/TaskQueue.ts` (make them module-internal); keep `complete` composing them.
- [x] 4.2 Update the package index/exports so the removed primitives are no longer part of the public API.
- [x] 4.3 Update the `TaskQueue.ts` module doc comment that references `takeUnsafe`/`succeed`/`fail`.

## 5. Cleanup & tooling

- [x] 5.1 Remove the Lua `redis.log` debug calls in `src/TaskEngine.ts`: line 180 (`dumpState`), lines 316/321/332 (`failTask`, incl. the commented-out one), lines 754-756 (`ConsumeScheduleScript`). Leave the `dumpState`/`dumpList` helpers only if referenced under `debugMode`; otherwise drop them too.
- [x] 5.2 Gate the lined-source error output behind `debugMode`: in the `ev` helper (`TaskEngine.make`), only append the numbered `source` to `TaskEngineError` messages when `debugMode` is on; otherwise use just `message`.
- [x] 5.3 Restore/relax `biome.json` intentionally (decide whether `noExplicitAny: off` stays or is scoped) and document the choice.
- [x] 5.4 Run the formatter/linter across all touched files; resolve remaining warnings.
- [x] 5.5 Sweep for any remaining dead code or debug logging across the changed files (e.g. the `console.log` in `TaskQueue.fail`, `Effect.retry;` in `Task.ts`).

## 6. Tests

- [x] 6.1 Fix `src/TaskQueue.test.ts` and `src/TaskEvents.test.ts` for the `success`/`error` config-key rename.
- [x] 6.2 Add coverage: a schedule-driven retry lands on the scheduled list at the computed time (using mock time).
- [x] 6.3 Add coverage: `maxRetries` cap stops an unbounded schedule (queue default) and per-offer override wins.
- [x] 6.4 Add coverage: `Canceled` error short-circuits retries.
- [x] 6.5 Add/verify coverage: `task.failed` event exposes `retryAt`.
- [x] 6.6 Run the full test suite green.

## 7. Docs

- [x] 7.1 Update `README.md` Retries note to describe definition-level `retry` (Schedule/options) and the `maxRetries` cap (default + per-offer override).
- [x] 7.2 Update the Define-a-task example(s) to use `success`/`error` keys.
- [x] 7.3 Remove or rewrite any README references to `take`/`succeed`/`fail` now that they're internal.
