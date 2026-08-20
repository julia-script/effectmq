---
"@effectmq/core": patch
---

Make public Effect contracts honest and restructure the package around focused
task-record and task-event modules. Task, queue, worker, and scheduler
definitions remain pure while programmer-authored definition invariants become
defects at their first runtime use. TaskEngine errors use semantic reason tags,
the standard `TaskEngine.layer` is fully wired, and malformed codec/Redis inputs
remain in typed error channels.
