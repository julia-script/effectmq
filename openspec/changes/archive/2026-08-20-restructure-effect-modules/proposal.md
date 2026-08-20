## Why

The current package hides several distinct concepts in `Schemas.ts` and `utils.ts`, uses inconsistent service declarations and layer wiring, and exposes an incomplete subpath surface. The result is unnecessary coupling and an API whose module boundaries do not explain the domain.

## What Changes

- **BREAKING** Replace the catch-all schema module with focused MessagePack, task-record, engine-record, and task-event modules; replace `utils.ts` with a retry-schedule module.
- **BREAKING** Declare runtime services as `Context.Service` classes with stable package-qualified identifiers, model optional ambient task provenance as a `Context.Reference`, and standardize `layerNoDeps` versus fully wired `layer` constructors.
- **BREAKING** Rebuild package exports around supported concept modules, including an explicit Observability subpath, and remove obsolete aggregate/internal exports.
- Replace broad root `effect` imports with stable narrow subpath imports throughout production code.
- Define reusable Effect-returning functions with `Effect.fnUntraced` and pin exact public or recursive return types.
- Update repository conventions to describe the new module, service, layer, import, and function-definition rules.

## Capabilities

### New Capabilities

- `effect-module-architecture`: Defines the package's concept-oriented modules, supported subpaths, service identities, layer ownership, and reusable Effect function conventions.

### Modified Capabilities

None.

## Impact

This is an intentionally breaking source-layout and import-surface change affecting most files in `src`, generated declarations, package exports, documentation, and consumer fixtures. Redis wire data is not renamed by this proposal; storage compatibility changes, if any, remain governed by the storage protocol.
