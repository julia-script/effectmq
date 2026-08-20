# Package architecture

The public surface is concept-oriented and has matching root namespaces and
package subpaths:

- `Task`, `TaskQueue`, `Worker`, and `Scheduler` own definition, queue,
  processing, and scheduling APIs;
- `TaskRecord` owns durable typed task records and `TaskEvent` owns lifecycle
  events;
- `TaskEngine` owns atomic queue storage behavior;
- `RedisPool`, `NodeRedisPool`, `StorageProtocol`, and `Observability` own the
  external client, live Node adapter, value protocol, and metrics boundaries.

Internal modules are deliberately not package subpaths. `MessagePack` owns the
binary transform, `EngineRecord` owns Redis-facing record schemas,
`RedisReadiness` owns readiness recovery policy, and `RetrySchedule` owns retry
schedule construction and stepping.

The dependency direction is:

```text
Task -> RetrySchedule
TaskQueue -> Task + TaskRecord + TaskContext + TaskEngine + StorageProtocol
Worker/Scheduler -> TaskQueue + TaskEngine
TaskEngine -> EngineRecord + TaskEvent + MessagePack + RedisPool
TaskEvent -> TaskRecord + EngineRecord + MessagePack
NodeRedisPool -> RedisPool + RedisReadiness + Observability
```

`TaskEngine.layer()` is the standard zero-requirement Node live graph. It
retains `TaskEngine`, `RedisPool`, `RedisConnectionRoles`,
`RedisConnectionHealth`, Effect Redis, and Crypto services. Custom Redis
integrations provide `RedisPool` to `TaskEngine.layerNoDeps()`.

Public queue declarations use named exact aliases for success, typed failure,
and required services. The strict test compiler pins `complete`, `completeOne`,
`decodeTask`, `wait`, `execute`, and both TaskEngine layer modes.
