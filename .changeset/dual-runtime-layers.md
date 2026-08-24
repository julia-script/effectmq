---
"@effectmq/core": minor
---

Document Bun plus node-redis as a supported composition.

`TaskEngine.layer` stays the live graph. `TaskEngine.layerNoDeps` is the
compose path for a custom `RedisPool` or `BunCrypto`. Bun's built-in
`RedisClient` is not the supported adapter yet.
