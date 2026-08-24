# effect-module-architecture

## Purpose

Defines stable package seams in which each module owns one recognizable domain concept and public Effect services expose predictable identities, layers, and import paths.

## Requirements

### Requirement: Modules are organized by domain concept
Each production module SHALL have one singular, nameable responsibility. Serialization, task records, engine records, task events, and retry schedules SHALL have distinct owners rather than sharing a generic schema or utility module.

#### Scenario: Maintainer changes event decoding
- **WHEN** a maintainer changes the representation or decoder for task lifecycle events
- **THEN** the change is localized to the task-event concept and its direct consumers
- **AND** MessagePack configuration and unrelated retry scheduling do not share that module

#### Scenario: Generic module names are checked
- **WHEN** production modules are reviewed statically
- **THEN** catch-all names such as `Schemas` and `utils` are absent

### Requirement: Public concepts have stable import paths
Every supported public module namespace SHALL be available from the root namespace barrel and from a matching package subpath. Internal storage and adapter helpers SHALL NOT be accidentally reachable through undocumented deep imports or generated public declarations.

#### Scenario: Consumer imports Observability
- **WHEN** a packed-package consumer imports the Observability namespace through its documented subpath
- **THEN** Node and TypeScript resolve the same supported module exposed by the root barrel

#### Scenario: Public declaration references a model
- **WHEN** a generated declaration exposes a task-record or task-event model
- **THEN** that model is reachable from a documented public subpath

### Requirement: Service identities are stable
Project runtime services SHALL use class-based service declarations with stable package-qualified identifiers. Optional ambient task provenance SHALL use a context reference with an explicit default rather than a fabricated always-present service.

#### Scenario: Two modules request the engine service
- **WHEN** independently imported modules request the task engine
- **THEN** both resolve the same stable service identity

#### Scenario: Handler runs without provenance
- **WHEN** code reads task provenance outside a managed task handler
- **THEN** it receives the documented absent default without requiring an extra layer

### Requirement: Service layers communicate dependency ownership
`TaskEngine.layer` SHALL provide the live graph and SHALL retain Redis operational services plus Crypto. `TaskEngine.layerNoDeps` SHALL require an ambient `RedisPool` and SHALL NOT select a process runtime or Redis client. A Bun plus node-redis application SHALL run `TaskEngine.layer` under `BunRuntime`, or compose `TaskEngine.layerNoDeps` with `NodeRedisPool.layer` and `BunCrypto.layer`.

#### Scenario: Application supplies a custom Redis pool
- **WHEN** an application uses `TaskEngine.layer` or `TaskEngine.layerNoDeps`
- **THEN** the type system requires the Redis pool service from the application

#### Scenario: Application uses the live layer
- **WHEN** an application uses `TaskEngine.layer`
- **THEN** it receives the documented engine and Redis operational services with no unresolved requirements

#### Scenario: Application uses Bun with node-redis
- **WHEN** an application composes `TaskEngine.layerNoDeps` with `NodeRedisPool.layer` and `BunCrypto.layer`
- **THEN** the composed layer has no unresolved requirements

### Requirement: Effect implementation style preserves contracts
Reusable Effectful functions SHALL use the project's traceable function wrapper convention and public or recursive functions SHALL declare exact return contracts. Production modules SHALL import Effect APIs through stable narrow subpaths.

#### Scenario: Reusable queue operation is inspected
- **WHEN** a reusable queue operation that builds an Effect is inspected statically
- **THEN** it uses the standard untraced Effect function wrapper
- **AND** its declaration has no inferred `any` channel

#### Scenario: Production imports are checked
- **WHEN** production sources are checked by the architecture lint rule
- **THEN** they do not import APIs from the broad `effect` package root
