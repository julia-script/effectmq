---
"@effectmq/core": major
---

Make public Effect contracts honest and restructure the package around focused
task-record and task-event modules. Task and scheduler construction is now
effectful, TaskEngine errors use semantic reason tags, the standard
`TaskEngine.layer` is fully wired, and malformed codec/Redis inputs remain in
typed error channels.
