---
"@effectmq/core": patch
---

Fix `stream` (and everything built on it: `wait`, `execute`, `TaskQueue.stream`) failing with a `SchemaError` when using the bundled `NodeRedisPool`: node-redis returns `XREAD` replies as an object keyed by stream name, while the engine only decoded the ioredis-style `[stream, entries]` tuple array. The reply is now normalized before decoding, so both Redis clients work.
