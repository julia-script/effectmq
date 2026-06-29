## 1. Test scaffolding

- [x] 1.1 Create `src/TaskQueue.test.ts` with a helper that builds a typed queue via `Task.make` + `TaskQueue.make`, using a deterministic `idempotencyKey` derived from the payload
- [x] 1.2 Reuse `TestRuntime`, `getLists`, and mock-time helpers from the testing layer; use a distinct queue name (prefix) per test

## 2. offer

- [x] 2.1 Test `offer` with no delay places a task with the deterministic id on the queue's wait list

## 3. complete — success path

- [x] 3.1 Test: offer a payload, run `complete` with a handler returning a success value; assert it resolves to `true`
- [x] 3.2 In the same test, assert the handler received the decoded typed payload (not the raw JSON string)
- [x] 3.3 Assert the task is gone from wait/active after completion (per default `delete` success policy)

## 4. complete — failure path

- [x] 4.1 Test: offer a payload to a queue with `onFailurePolicy: "mark-as-failure"`, run `complete` with a handler that fails; assert it resolves to `false`
- [x] 4.2 Assert the task is routed to the failed list

## 5. Verify

- [x] 5.1 Run `src/TaskQueue.test.ts`; confirm all tests pass and `tsc --noEmit` is clean
- [x] 5.2 If any production bug in `TaskQueue.ts`/encode-decode is surfaced, report it and fix the root cause (do not weaken the test)
