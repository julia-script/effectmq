# Runtime boundaries

EffectMQ contains foreign behavior at the narrowest owning module:

| Foreign behavior | Owner | Policy |
| --- | --- | --- |
| MessagePack pack/unpack | `MessagePack` and `StorageProtocol` | Capture throws as schema/storage codec failures with stage, path, and cause. |
| Redis promises and reply values | `NodeRedisPool` and `TaskEngine` | Wrap promise rejection; validate every consumed scalar, tuple, collection, byte, and stream shape. |
| node-redis listeners | `NodeRedisPool` | Register before connect, use bounded service-free callback bridges, remove listeners before close. |
| Redis shutdown | `NodeRedisPool` | Await idempotent graceful close; force destroy after rejection; scoped partial acquisition unwinds. |
| Readiness | `RedisReadiness` | Convert only `RedisError` to `false`; preserve defects and interruption. |
| Wall-clock reads | `Scheduler`, `TaskQueue`, `NodeRedisPool` | Read Effect `Clock` during execution; explicit instants remain available to deterministic cores. |
| UUID generation | `Task` and `TaskEngine` | Require Effect `Crypto`; map platform failures to semantic task/engine errors. |
| CLI config/acquisition | `InspectPreReleaseData` | Use Effect `Config`, the scoped Redis layer, typed scan errors, and `NodeRuntime.runMain` only at the executable edge. |

TaskEngine is also the only place that interprets vendor Redis diagnostics. It
immediately translates them to stable reason tags. Higher modules branch only
on those tags and retain the original value solely as diagnostic ancestry.

Open-key stream dictionaries use null-prototype records. Tests cover RESP array
and Map representations, malformed values, prototype-sensitive keys,
interruption, rejected close promises, listener removal, and partial
multi-client acquisition.
