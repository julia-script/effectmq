---
"@effectmq/core": minor
---

Add task lifecycle events and streaming APIs. The engine publishes `task.created`, `task.updated`, `task.failed`, `task.completed`, and `task.moved` events to a per-queue Redis Stream, exposed as a typed Effect `Stream` via `TaskQueue.stream`. New `TaskQueue.wait(queue, taskId)` awaits a task's terminal outcome, and `TaskQueue.execute(queue, payload)` offers and awaits in one call.

Also fixes a double-JSON-encoding bug where `task.completed` (and thus `wait`/`execute`) returned the success value wrapped in extra quotes.
