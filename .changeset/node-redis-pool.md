---
"@juliascript/effectmq": minor
---

Add `NodeRedisPool`, a bundled connection-pooled Redis layer backed by [node-redis](https://github.com/redis/node-redis)'s `createClientPool`. `NodeRedisPool.layer(options)` provides the `Redis` service `TaskEngine` requires — no `@effect/platform-node` needed for the Redis side. The pool connects lazily on first command and closes when the layer scope ends.
