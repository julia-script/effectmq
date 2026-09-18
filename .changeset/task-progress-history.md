---
"@effectmq/core": minor
---

Add opt-in typed task progress and lifecycle history backed by per-generation Redis Streams. Managed handlers can emit progress through an attempt-bound context, and readers can page history using durable cursors with explicit trimming gaps. History is unlimited by default, supports an optional oldest-first count cap, and follows the task record's retention and disposal. Progress failures remain operational, including uncertain writes that are not automatically replayed.

Upgrade every queue process before enabling progress; dispose of enabled generations through the upgraded engine before rolling back to a version without history support.
