## Context

See `proposal.md` for motivation. Graph analysis identifies `Schemas.ts` as the shared owner of unrelated codec, record, and event paths, while `TaskEngine`, `TaskQueue`, and `StorageProtocol` are already recognizable domain seams. The root barrel exports Observability, but the package manifest omits its matching subpath. TaskContext models optional ambient state as a service, and layer names do not reveal whether Redis dependencies remain required.

## Goals / Non-Goals

**Goals:**

- Give every schema, model, service, and layer an obvious conceptual owner.
- Make root exports, subpath exports, and generated declarations agree.
- Standardize service declaration and layer dependency semantics.
- Encode Effect coding conventions in source structure and automated checks.

**Non-Goals:**

- Split `TaskQueue` merely to reduce line count; its operations share one queue/handle lifecycle.
- Change Redis keys, Lua behavior, or storage bytes.
- Publish every internal codec and engine record as a supported API.
- Preserve imports from `Schemas.ts`, `utils.ts`, or obsolete package subpaths.

## Decisions

### 1. Replace catch-all modules with explicit concept owners

The target ownership is:

| Module | Visibility | Responsibility |
|---|---|---|
| `MessagePack.ts` | internal | Configured MessagePack byte encoding and decoding boundary |
| `TaskRecord.ts` | public | Task identity, durable task state, and typed task record models/codecs |
| `EngineRecord.ts` | internal | Redis-facing command/result record schemas |
| `TaskEvent.ts` | public | Versioned lifecycle event models and codecs |
| `RetrySchedule.ts` | internal | Retry schedule evaluation and composition currently hidden in `utils.ts` |

`StorageProtocol.ts` remains the public owner of storage envelopes, protocol versions, and limits. Task, Scheduler, TaskQueue, TaskEngine, Worker, RedisPool, NodeRedisPool, and Observability remain cohesive modules. Shared source may be physically small; module boundaries follow concepts, not file-size thresholds.

A renamed `CodecUtils.ts` was rejected because it would preserve mixed ownership. Splitting TaskQueue by individual methods was rejected because it would create shallow modules with heavy shared state.

### 2. Publish only types that cross supported APIs

`TaskRecord` and `TaskEvent` become supported root namespaces and matching package subpaths because public engine/stream values reference them. Observability receives the missing subpath. Internal MessagePack, EngineRecord, and RetrySchedule modules are omitted from `package.json` exports and must not appear as inaccessible deep imports in declarations. The package verification fixture imports every supported subpath.

Exporting every new file was rejected because physical organization is not automatically a compatibility promise. Keeping public types reachable only transitively was rejected because consumers need a stable naming home.

### 3. Use service classes, with TaskContext as a Reference

Runtime capabilities such as TaskEngine and RedisPool use `Context.Service` classes with identifiers under `@effectmq/core/<Service>`. Service interfaces become `Service.Shape` types or focused exported aliases rather than separate value/interface pairs with generic IDs. `TaskContext` becomes a `Context.Reference` whose default is absent provenance, since reading it outside a handler is valid and needs no layer. Handler execution uses `Effect.locally`/the appropriate reference-local operation to set it.

Keeping TaskContext as an optional service was rejected because it conflates “service not installed” with the valid “no current task” state. Making it a required service was rejected because it adds boilerplate to unrelated effects.

### 4. Layer names state whether dependencies remain

`TaskEngine.layerNoDeps(config)` constructs the engine while requiring `RedisPool`. `TaskEngine.layer(config)` is the standard Node live graph and supplies NodeRedisPool; it intentionally retains the Redis role/health/pool services needed by Worker and observability, and documents that output union. Similar service modules follow the same naming rule. `Layer.provide` is the default composition; `Layer.provideMerge` is used only in the standard live graph where retained Redis services are deliberate API outputs.

Keeping the current ambiguous `layer` name was rejected because callers cannot tell whether it is live or still has requirements. A generic `Runtime.ts` composition module was rejected because it does not name a domain capability.

### 5. Narrow imports and standard Effect function definitions are enforced

Production files import from `effect/Effect`, `effect/Schema`, and other supported subpaths. Reusable generator-backed functions—including dual implementations—use `Effect.fnUntraced`; exported and recursive effects pin exact return types or `Effect.fn.Return`. Small one-off inline effects may remain direct expressions. An architecture check scans production imports and known reusable function patterns so the convention does not rely only on review memory.

A blanket wrapper around every anonymous effect was rejected as ceremony without architectural value. Root `effect` imports were rejected because they create broad, unstable dependency edges.

### 6. Repository guidance changes with the code

`CLAUDE.md` and contributor-facing commands will describe concept modules, stable service IDs, TaskContext Reference semantics, layer naming, narrow imports, exact contracts, and the testing policy from `adopt-effect-native-testing`. Contradictory guidance endorsing ambiguous layer composition or unmanaged test runtimes is removed in the same breaking release.

## Risks / Trade-offs

- [File moves create a noisy diff and merge conflicts] → Apply after behavior/error proposals, use mechanical import changes in isolated commits, and preserve user changes in overlapping files.
- [New public subpaths expand long-term support obligations] → Export only TaskRecord, TaskEvent, and Observability because they are already public concepts; keep codec internals private.
- [Stable service ID changes duplicate services across mixed versions] → Treat this as a coordinated breaking release and prohibit mixing old/new library instances in one Effect graph.
- [Live layer retains more services than some callers need] → Keep `layerNoDeps` as the precise composition primitive for custom graphs.

## Migration Plan

1. Land contract and boundary changes first so moved modules expose their final types.
2. Create the new concept modules and move definitions without compatibility re-exports.
3. Convert services and TaskContext, then rename and rebuild layer constructors.
4. Update narrow imports and reusable Effect function definitions.
5. Rebuild root exports, package subpaths, generated declarations, docs, and packed-consumer fixtures.
6. Remove `Schemas.ts` and `utils.ts`; run architecture checks, typechecks, tests, docs, and package verification. Rollback is a release revert rather than a compatibility layer.
