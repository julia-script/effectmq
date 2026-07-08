---
"@effectmq/core": patch
---

Fix four task-engine bugs surfaced by new scheduler and locking tests:

- Task locks now expire after the intended number of milliseconds (`PX`) instead of interpreting the timeout as seconds (`EX`), so tasks whose worker died are actually recovered as stalled instead of staying locked ~1000× longer than configured.
- Stalled tasks record the proper `~effectmq/Error/Stalled` tag; previously the raw `Stalled` tag made the typed task decode fail, so a stalled task could never be processed again through `TaskQueue`.
- `Canceled` errors now short-circuit retries even when a retry time was computed, as the retry-policy spec requires (the tag check read the wrong field).
- `consumeSchedule` honors the debug-mode mock clock and reports the corrective next run time when a tick is not consumed (Lua `false` return values were being converted to null replies that truncated the response array).
