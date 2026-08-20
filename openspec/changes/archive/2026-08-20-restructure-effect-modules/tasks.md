## 1. Establish Target Contracts

- [x] 1.1 Record the final public declarations and import graph after the contract and runtime-boundary changes land.
- [x] 1.2 Add architecture checks for forbidden production root `effect` imports, generic `Schemas`/`utils` modules, unsupported deep imports, and missing public subpaths.

## 2. Split Concept Modules

- [x] 2.1 Create internal `MessagePack.ts` and move only configured MessagePack encoding/decoding ownership into it.
- [x] 2.2 Create public `TaskRecord.ts` and move task identity, durable task state, and typed task-record models/codecs into it.
- [x] 2.3 Create internal `EngineRecord.ts` and move Redis-facing command/result record schemas into it.
- [x] 2.4 Create public `TaskEvent.ts` and move versioned lifecycle event models/codecs into it.
- [x] 2.5 Create internal `RetrySchedule.ts`, move the retry schedule operations from `utils.ts`, and update their names/documentation.
- [x] 2.6 Update all consumers to import the focused owners and delete `Schemas.ts` and `utils.ts` without compatibility re-exports.

## 3. Standardize Services and Layers

- [x] 3.1 Convert TaskEngine, RedisPool, connection-role, and connection-health services to `Context.Service` classes with `@effectmq/core/<Service>` identifiers.
- [x] 3.2 Convert optional TaskContext provenance to a `Context.Reference` with an absent default and locally provide it around handlers.
- [x] 3.3 Add `TaskEngine.layerNoDeps` requiring RedisPool and migrate custom Redis compositions to it.
- [x] 3.4 Rebuild `TaskEngine.layer` as the fully wired Node live graph with its retained Redis operational services documented in the output type.
- [x] 3.5 Replace incidental `Layer.provideMerge` uses with `Layer.provide`; retain it only where upstream Redis services are intentional live-layer outputs.
- [x] 3.6 Add compile-time layer tests for unresolved `layerNoDeps` requirements and the zero-requirement live graph.

## 4. Normalize Effect Source Style

- [x] 4.1 Convert all production imports from the broad `effect` root to supported narrow subpaths and apply deterministic import ordering.
- [x] 4.2 Convert reusable generator-backed functions in TaskQueue, codec modules, and Worker to `Effect.fnUntraced`, including dual implementations.
- [x] 4.3 Pin exact public and recursive Effect return types or `Effect.fn.Return` and remove any inference-erasing annotations introduced by the move.
- [x] 4.4 Run the architecture check and inspect the dependency graph to confirm the new modules form distinct cohesive seams.

## 5. Rebuild the Public Surface

- [x] 5.1 Add root namespace exports and matching package subpaths for TaskRecord, TaskEvent, and Observability.
- [x] 5.2 Ensure public declarations reference only supported subpaths and internal MessagePack, EngineRecord, and RetrySchedule modules do not leak.
- [x] 5.3 Update packed-consumer fixtures to import every supported root namespace and subpath and to reject removed deep imports.
- [x] 5.4 Update API documentation and migration notes for module moves, stable service identifiers, TaskContext Reference semantics, and layer naming.

## 6. Repository Policy and Verification

- [x] 6.1 Update `CLAUDE.md` and contributor guidance to match the final module, service, layer, import, exact-contract, and Effect function conventions.
- [x] 6.2 Remove contradictory guidance endorsing ambiguous layer composition or unmanaged test-runtime patterns.
- [x] 6.3 Run production/test typechecks, architecture checks, lint, unit/integration/fault tests, docs checks, declaration inspection, and packed-package verification.
