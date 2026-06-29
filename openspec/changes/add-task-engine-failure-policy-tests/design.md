## Context

`src/TaskEngine.test.ts` currently has a single integration test: the success happy path with the `delete` policy, asserted via `toMatchInlineSnapshot` over `getLists()` output and parsed task objects. It runs against a real Redis through `TestRuntime` (`src/testing/redisLayer.ts`), with deterministic time via `setMockTime`/`stepMockTime` (the layer is built with `debugMode: true`).

The behavior we want to cover lives entirely in two Lua scripts already in `TaskEngine.ts`:
- `failTask` (called by `WriteErrorResultScript`): appends the error, and if the error is not `Canceled` and `#errors < maxRetries`, re-queues to wait; otherwise applies `onFailurePolicy` (`delete` / `mark-as-failure` / `keep`).
- `WriteSuccessResultScript`: applies `onSuccessPolicy` (`delete` / `mark-as-success` / `keep`).

## Goals / Non-Goals

**Goals:**
- Cover the failure paths: retry-until-`maxRetries`, terminal failure, and the `Canceled` short-circuit.
- Cover each `onFailurePolicy` and `onSuccessPolicy` value not already tested.
- Keep the existing inline-snapshot + `getLists` style so the tests read like the one already there.

**Non-Goals:**
- No changes to production Lua or TypeScript.
- Not covering scheduling/locking concurrency, stalled-lock recovery, or multi-worker contention — separate concerns.

## Decisions

- **Reuse `TestRuntime` and `getLists`, one `test()` per path.** Matches the existing file and keeps each scenario independently runnable. Each test uses its own `prefix` so state never bleeds between tests on the shared container.
- **Drive failures through the real take → writeError cycle.** A task must be locked by the worker before `writeError` is accepted (`isLockedBy`), so each failure test does `createTask` → `takeTask` → `writeError`, re-taking between retries. This exercises the lock check too, for free.
- **Assert both list placement (`getLists`) and hash survival (`getTask`).** `delete` vs `keep` vs `mark-*` differ only in whether the task hash survives and which list it lands in; checking `getTask` is what distinguishes `keep` from `delete`.
- **Use inline snapshots for list state, explicit assertions for `errors`/`getTask`.** Snapshots are concise for the five-list shape; targeted `expect` is clearer for "errors has N entries" and "getTask is null".

## Risks / Trade-offs

- **Latent bug: `getList` reads `failed`/`success` with `LRANGE`, but they are written as sorted sets (`ZADD`).** → The `mark-as-failure` / `mark-as-success` tests are precisely what surface this. If they fail because the list reads empty, that is a real production bug in `GetListScript`, not a test defect. Fix `GetListScript` to use `ZRANGE` for those two lists (mirroring `active`) and note it; do not weaken the test to paper over it. Flagging here so the apply step expects it.
- **Shared Redis container across tests** → distinct `prefix` per test isolates state; mock time is set per-test so ordering is deterministic.
- **`errors` length vs `maxRetries` semantics** → `failTask` retries while `#errors < maxRetries`, so `maxRetries: 2` yields one retry then terminal on the second failure. Tests pin the exact counts to lock this in.

## Open Questions

- Should the `GetListScript` `LRANGE`→`ZRANGE` fix ship inside this change or as a separate one? Default: fix it here since the tests are blocked without it, and scope it tightly to those two branches.
