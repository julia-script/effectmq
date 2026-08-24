---
"@effectmq/core": minor
---

Split the Node convenience graph out of `TaskEngine.layer`.

`TaskEngine.layer` (and `layerNoDeps`) now require an ambient `RedisPool`. They
no longer embed `NodeRedisPool` or `NodeCrypto`. Use `NodeLive.layer` for the
previous zero-requirement Node graph. Bun plus node-redis is
`TaskEngine.layer` composed with `NodeRedisPool.layer` and `BunCrypto.layer`.
