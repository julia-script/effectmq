## 1. Boundary Characterization

- [x] 1.1 Inventory every node-redis promise, callback, listener, cast, text conversion, ambient time/random read, and CLI resource acquisition and assign each to an owning boundary.
- [x] 1.2 Add regression tests for readiness interruption/defects, rejected close promises, partial pool acquisition, malformed replies, and prototype-sensitive keys.

## 2. Redis Promise and Lifecycle Boundary

- [x] 2.1 Add focused typed adapters for node-redis promise operations using the semantic errors from `make-effect-contracts-honest`.
- [x] 2.2 Convert each Redis connection to scoped acquisition with an immediately registered, idempotent typed finalizer.
- [x] 2.3 Define and implement graceful-close versus forced-destroy policy without floating promises.
- [x] 2.4 Ensure partial multi-client acquisition releases every already-acquired client.
- [x] 2.5 Restrict readiness recovery to expected Redis failures so defects and interruption propagate unchanged.
- [x] 2.6 Register and remove node-redis event listeners with pool scope; document and test the bounded service-free callback bridge.

## 3. Redis Reply Validation

- [x] 3.1 Implement operation-specific decoders for scalar, nullable, tuple, collection, buffer, and stream replies accepted from Redis.
- [x] 3.2 Replace `asText` coercion with explicit supported text representations and typed invalid-reply failures.
- [x] 3.3 Replace XREAD/stream and command-result casts with validation before domain event or record construction.
- [x] 3.4 Replace open-key plain objects with `Map` or null-prototype dictionaries and test `__proto__`, `constructor`, and `prototype` keys.
- [x] 3.5 Add fixtures for every supported RESP representation and unexpected reply shape.

## 4. Explicit Time and Randomness

- [x] 4.1 Replace scheduler default-parameter and `Date.now` reads with execution-time Clock access while retaining an explicit-time pure materialization core.
- [x] 4.2 Replace TaskQueue retry timestamps with Clock access inside the Effect.
- [x] 4.3 Replace ambient UUID defaults with an Effect-owned cryptographic randomness service and update public requirement aliases.
- [x] 4.4 Add deterministic clock/identity tests proving construction time and ambient process globals do not affect execution.

## 5. Effect-Native CLI

- [x] 5.1 Define typed CLI configuration for Redis URL and inspection options using Effect Config.
- [x] 5.2 Reuse the scoped Redis layer in the inspection command and move scan failures into semantic typed errors.
- [x] 5.3 Compose the command as one scoped Effect and call `NodeRuntime.runMain` only in the executable entry module.
- [x] 5.4 Add CLI tests for missing configuration, scan failure, normal shutdown, and interruption cleanup.

## 6. Verification

- [x] 6.1 Run malformed-reply, prototype-safety, lifecycle, interruption, Redis restart, and Sentinel failover suites.
- [x] 6.2 Run production/test typechecks, lint, unit/integration/fault tests, CLI smoke tests, and packed-package verification.
- [x] 6.3 Update boundary and operational documentation with the new Clock/randomness requirements, invalid-reply errors, and shutdown behavior.
