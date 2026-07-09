---
"@effectmq/core": minor
---

Tasks offered from inside a `TaskQueue.complete` handler now automatically reference the task being processed: the new task is pinned by it (`heldBy`), so its record and result survive until the outer task dies, and is attributed to it (`createdBy`). Pass `detached: true` in the offer options to skip the pin while keeping the `createdBy` attribution. Offers made outside a handler are unchanged.
