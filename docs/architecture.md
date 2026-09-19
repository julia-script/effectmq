# Package architecture

The public surface is concept-oriented and has matching root namespaces and
package subpaths:

- `Task`, `TaskQueue`, `Worker`, and `Scheduler` own definition, queue,
  processing, and scheduling APIs;
- `TaskRecord` owns durable typed task records and `TaskEvent` owns lifecycle
  events; `TaskHistory` owns typed per-generation progress, compact lifecycle
  history, pagination, and progress-write failures;
- `TaskEngine` owns atomic task queue storage behavior;
- `EventQueue`, `EventEngine`, and `EventRecord` own application event APIs,
  atomic subscription/delivery storage, and public event identities;
- `RedisPool`, `NodeRedisPool`, `StorageProtocol`, and `Observability` own the
  external client, live Node adapter, value protocol, and metrics boundaries.

Internal modules are deliberately not package subpaths. `MessagePack` owns the
binary transform, `EngineRecord` owns Redis-facing record schemas,
`RedisReadiness` owns readiness recovery policy, and `RetrySchedule` owns retry
schedule construction and stepping.

The dependency direction is:

```text
Task -> RetrySchedule
TaskQueue -> Task + TaskRecord + TaskContext + TaskEngine + TaskHistory + StorageProtocol
Worker/Scheduler -> TaskQueue + TaskEngine
TaskEngine -> EngineRecord + TaskEvent + TaskHistory + MessagePack + RedisPool
TaskHistory -> EngineRecord + StorageProtocol
EventQueue -> EventEngine + EventRecord + StorageProtocol
EventEngine -> EventRecord + RedisPool + NodeRedisPool
TaskEvent -> TaskRecord + EngineRecord + MessagePack
NodeRedisPool -> RedisPool + RedisReadiness + Observability
```

`TaskEngine.layer()` is the standard zero-requirement Node live graph. It
retains `TaskEngine`, `RedisPool`, `RedisConnectionRoles`,
`RedisConnectionHealth`, Effect Redis, and Crypto services. Custom Redis
integrations provide `RedisPool` to `TaskEngine.layerNoDeps()`.

`EventEngine.layer()` provides the corresponding event runtime with Node Redis
and Crypto services. Task lifecycle streams, task-owned history, and application
event subscriptions are separate resources with separate retention contracts.
See [durable events](./events.md) and the [API reference](./api-reference.md).

Public queue declarations use named exact aliases for success, typed failure,
and required services. The strict test compiler pins `complete`, `completeOne`,
`decodeTask`, `wait`, `execute`, and both TaskEngine layer modes.
