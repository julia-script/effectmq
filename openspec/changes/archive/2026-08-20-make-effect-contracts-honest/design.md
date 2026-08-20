## Context

See `proposal.md` for motivation. The package uses Effect's `Effect<A, E, R>` type as its public contract, but several explicit return annotations and schema casts currently remove real `E` and `R` members. MessagePack and byte conversion can throw synchronously, and queue policy infers Redis conditions from diagnostic strings. Breaking changes are acceptable, so the design optimizes for one truthful model rather than compatibility shims.

## Goals / Non-Goals

**Goals:**

- Make generated declarations a complete account of caller obligations and recoverable outcomes.
- Establish one semantic error boundary between Redis/client details and queue policy.
- Treat invalid public configuration and corrupt stored input as typed failures.
- Make compound APIs derive their contracts compositionally.

**Non-Goals:**

- Preserve source compatibility with current constructors or narrowed error unions.
- Change the current storage encoding or protocol version.
- Collapse all failures into one universal package error.
- Convert programmer invariants and impossible internal states into recoverable errors.

## Decisions

### 1. Infer implementations, publish named exact contracts

Reusable implementations will be defined so TypeScript infers their real effects, then checked against exported operation-specific aliases such as `CompleteError`, `CompleteRequirements`, `WaitError<F>`, and `ExecuteError<F>`. `completeOne` will reuse the same aliases rather than introducing `any`. Declaration tests will inspect assignability of both `E` and `R`.

The alternative—leaving every signature fully anonymous—would be truthful but difficult for consumers to read and for maintainers to regression-test. A single broad `EffectMqError` was rejected because it would erase recovery distinctions.

### 2. Compound operations are algebraic unions of their steps

`execute` will be implemented and typed as `offer` followed by `wait`, retaining the `TaskFailed<F>` terminal wrapper and every protocol, retention, engine, storage, and schema failure from either step. Its requirements are the union of the engine plus payload encoding and success/failure decoding services. `complete` similarly includes the engine, payload decoding, result/failure encoding, and handler environment.

Mapping the terminal wrapper back to raw task failure was rejected because it makes `execute` disagree with the documented handle protocol and loses attempt/generation context.

### 3. Constructors validate in Effect

`Task.make`, scheduler construction, and engine configuration will return Effects with focused tagged configuration errors. Values are checked once at construction; successfully constructed descriptors are valid by construction. There will be no implicit throwing compatibility overload. If an internal constant needs a non-effectful constructor, it will use a private helper after local proof of validity.

Keeping public pure constructors that throw was rejected because callers cannot see or compose the failure. Adding public `unsafeMake` variants was rejected unless a concrete bootstrap use case appears.

### 4. The engine owns Redis error translation

The TaskEngine boundary will expose stable structured failures. Recoverable domain conditions such as relationship-limit and indeterminate-write remain distinct tagged errors. Lower-level operational failures use a `TaskEngineError` with a tagged `reason` union such as transport failure, script failure, invalid reply, and unsupported response, plus optional diagnostic cause. TaskQueue switches only on tags/reasons.

Retaining a free-form `message`/`cause` error and helper regexes was rejected because client versions and Redis deployments can change wording without changing semantics.

### 5. All codec exceptions are captured at the codec boundary

MessagePack pack/unpack and byte conversion will use fallible schema transforms or `Effect.try` at the smallest boundary. Caught values are translated into the package's typed encoding/decoding errors with path and cause details. Structural schema validation remains separate from serializer failure so diagnostics retain the failed stage. No `as Uint8Array` cast is allowed to stand in for input validation.

Replacing MessagePack with another codec was rejected because this change is about failure semantics and must not silently alter stored bytes.

### 6. TaskQueue exposes complete lifecycles only

The high-level TaskQueue module will stop exporting `extendLock` and `release` operations that accept its private attempt shape without exposing a corresponding acquisition operation. Worker continues to own that internal typed attempt lifecycle. Applications implementing a fully custom low-level worker use TaskEngine, whose public `take`, attempt, fencing, extension, and release operations form a complete abstraction.

Adding another public TaskQueue take API was rejected because it would duplicate Worker/TaskEngine lifecycle policy and widen the high-level surface merely to justify two orphaned methods.

## Risks / Trade-offs

- [Large compile-time blast radius from newly honest `E` and `R`] → Migrate leaf codecs and engine errors first, then allow compiler failures to drive queue and consumer updates.
- [Effectful constructors add call-site ceremony] → Validate once and keep the resulting descriptors pure; provide clear examples using `yield*` and layers.
- [Error algebra becomes too granular] → Add public variants only when callers can act differently; keep vendor diagnostics inside a bounded engine error reason.
- [Codec wrapping accidentally changes wire bytes] → Add golden byte fixtures and round-trip/corruption tests before replacing the existing transforms.

## Migration Plan

1. Add failing declaration/type tests for the exact completion, decode, wait, and execute contracts.
2. Introduce typed configuration and codec errors, then make constructors/codecs effectful.
3. Define the semantic engine error algebra and translate Redis/client failures at their owning boundary.
4. Remove string parsing and service-erasing casts; let inferred contracts propagate through TaskQueue.
5. Remove the orphaned high-level attempt operations and point custom low-level integrations to TaskEngine.
6. Update all internal call sites, examples, and package declarations in one breaking release.
7. Run storage golden fixtures, strict typechecks, and the full Redis integration suite. Rollback requires reverting the release as a unit; no mixed old/new source API is supported.
