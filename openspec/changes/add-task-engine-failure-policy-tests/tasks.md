## 1. Unblock failed/success list reads

- [x] 1.1 Run the new `mark-as-failure`/`mark-as-success` tests; if they read an empty list, fix `GetListScript` in `src/TaskEngine.ts` to use `ZRANGE` (not `LRANGE`) for the `failed` and `success` branches, mirroring the `active` branch
- [x] 1.2 Confirm the existing happy-path test still passes after the change

## 2. Failure-path tests

- [x] 2.1 Add a test: task with `maxRetries: 2`, take → `writeError` (non-Canceled) once, assert it returns to `wait` with one entry in `errors`
- [x] 2.2 Extend that test: re-take and `writeError` again to exhaust retries, assert it leaves `wait`/`active` and lands per `onFailurePolicy`
- [x] 2.3 Add a test: `writeError` with a `~effectmq/Error/Canceled`-tagged error on a task with `maxRetries > 0`, assert no retry and failure policy applied immediately

## 3. onFailurePolicy coverage

- [x] 3.1 Test `onFailurePolicy: "delete"` — terminally-failed task in no list and `getTask` returns null
- [x] 3.2 Test `onFailurePolicy: "mark-as-failure"` — task in `failed` list, `getTask` returns task with errors
- [x] 3.3 Test `onFailurePolicy: "keep"` — task in no list, `getTask` returns task with errors

## 4. onSuccessPolicy coverage

- [x] 4.1 Test `onSuccessPolicy: "mark-as-success"` — take → `writeSuccess`, task in `success` list, `getTask` returns task
- [x] 4.2 Test `onSuccessPolicy: "keep"` — take → `writeSuccess`, task in no list, `getTask` returns task

## 5. Verify

- [x] 5.1 Run the full `TaskEngine.test.ts` suite and confirm all tests pass
- [x] 5.2 Use distinct `prefix` per test so the shared Redis container stays isolated
