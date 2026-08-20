## 1. Contract Characterization

- [x] 1.1 Add compile-time assertions for the current `complete`, `completeOne`, `decodeTask`, `wait`, and `execute` success, failure, and service channels.
- [x] 1.2 Add runtime regression tests proving malformed MessagePack and invalid byte inputs fail in the typed channel rather than as defects.
- [x] 1.3 Add regression tests that exercise relationship-limit and indeterminate-write recovery without relying on diagnostic wording.

## 2. Typed Codec Boundary

- [x] 2.1 Introduce focused storage encoding and decoding error variants that retain codec stage, schema path, and cause details.
- [x] 2.2 Replace infallible MessagePack transforms with fallible transforms that capture pack and unpack exceptions.
- [x] 2.3 Validate external string/byte representations before conversion and remove the service-erasing MessagePack schema cast.
- [x] 2.4 Correct task codec generics so payload, success, and failure decoding services propagate through stored-task decoding.
- [x] 2.5 Add golden byte, round-trip, truncated-input, invalid-input-type, and corrupt-structure codec tests.

## 3. Semantic Error Algebra

- [x] 3.1 Define stable tagged engine reason variants for transport, script, protocol/invalid-reply, relationship-limit, and indeterminate-commit failures.
- [x] 3.2 Translate Redis/client outcomes to the semantic algebra at the TaskEngine boundary while preserving diagnostic causes.
- [x] 3.3 Remove recursive cause stringification, connection-message regexes, and storage-limit sentinel parsing from TaskQueue.
- [x] 3.4 Update offer/recovery branches to switch exhaustively on semantic tags and add wording-independent tests.

## 4. Typed Construction

- [x] 4.1 Define tagged configuration errors for task, scheduler, and engine fields with structured constraint details.
- [x] 4.2 Make task construction effectful and remove synchronous RangeError paths for caller-supplied task configuration.
- [x] 4.3 Make scheduler construction validate missed-tick/backfill settings before materialization and remove `Effect.die` for invalid settings.
- [x] 4.4 Move engine limit validation into typed layer/construction failure before Redis acquisition or commands.
- [x] 4.5 Update internal call sites, examples, and tests to yield the newly effectful constructors.

## 5. Honest Queue Contracts

- [x] 5.1 Export named exact error and requirement aliases for offer, complete, wait, and execute operations.
- [x] 5.2 Correct `complete` to require TaskEngine, handler environment, payload decoding, and success/failure encoding services and to expose every recoverable failure.
- [x] 5.3 Correct `completeOne` to reuse the completion contracts with no `any` channel.
- [x] 5.4 Correct stored-task decoding to require payload, success, and failure decoding services.
- [x] 5.5 Rebuild `execute` as the exact composition of `offer` and `wait`, retaining `TaskFailed` and all protocol, retention, engine, storage, schema, and timeout failures.
- [x] 5.6 Remove explicit annotations and casts that narrow implementation-inferred Effect channels, then satisfy the public named contracts.
- [x] 5.7 Remove public TaskQueue `extendLock`/`release` operations that require its unobtainable private attempt type, keep Worker ownership internal, and document TaskEngine as the complete low-level lifecycle.

## 6. Verification and Migration

- [x] 6.1 Regenerate declarations and verify the compile-time contract assertions fail under deliberate `E`/`R` erasure mutations.
- [x] 6.2 Update API documentation and release notes with the constructor and error-channel breaking changes and migration examples.
- [x] 6.3 Run production/test typechecks, lint, unit tests, Redis integration/fault tests, storage golden fixtures, docs checks, and packed-consumer verification.
