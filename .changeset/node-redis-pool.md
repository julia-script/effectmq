---
"@effectmq/core": minor
---

Add the `RedisPool` service — the minimal `send`/`eval` Redis surface `TaskEngine` now depends on instead of `Redis` from `effect/unstable` — and `NodeRedisPool`, a bundled connection-pooled implementation backed by [node-redis](https://github.com/redis/node-redis)'s `createClientPool`. `NodeRedisPool.layer(options)` provides `RedisPool` (and the generic `Redis` service for interop), connects lazily on first command, and closes when the layer scope ends.
