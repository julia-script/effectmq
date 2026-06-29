## Why

The TaskEngine has one integration test covering only the success happy path with the `delete` policy. The failure paths (retries, terminal failure, `Canceled` short-circuit) and the non-`delete` completion policies (`keep`, `mark-as-success`, `mark-as-failure`) are entirely unverified, even though they drive the Lua `failTask`/success-policy branching that determines where a task lands. We want test coverage on these before building further on the MVP.

## What Changes

- Add failure-path integration tests for `writeError`: retry-until-`maxRetries`, terminal failure, and the `Canceled`-tag short-circuit that skips retries.
- Add tests covering each `onSuccessPolicy` value (`keep`, `mark-as-success` in addition to the existing `delete`).
- Add tests covering each `onFailurePolicy` value (`keep`, `mark-as-failure` in addition to `delete`).
- No production code changes — tests only, exercising existing `TaskEngine` behavior through the existing Redis test layer.

## Capabilities

### New Capabilities
- `task-completion-policies`: The observable list-placement behavior of a task after success or failure, as a function of `onSuccessPolicy`, `onFailurePolicy`, `maxRetries`, and error tag. This change documents that behavior as testable requirements.

### Modified Capabilities

_None — no existing requirement behavior changes._

## Impact

- `src/TaskEngine.test.ts` — new test cases.
- Reuses `src/testing/redisLayer.ts` (`TestRuntime`, `getLists`) and mock-time helpers (`setMockTime`/`stepMockTime`); no new dependencies.
- Exercises existing Lua scripts (`WriteErrorResultScript`, `WriteSuccessResultScript`) — no production source edits.
