## Why

Several public APIs claim narrower Effect error and service types than their implementations actually require, while predictable configuration, serialization, and storage failures can escape as defects. This makes successful compilation an unreliable description of what a caller must provide or handle.

## What Changes

- **BREAKING** Make every public `TaskQueue`, task-codec, and engine operation expose its complete error and service requirements, with named public aliases for reusable contracts and no `any` or service-erasing casts.
- **BREAKING** Make `execute` preserve the full offer-and-wait protocol, including typed wrapper failures, generation/cursor failures, retention failures, and all schema services.
- **BREAKING** Remove high-level queue operations that require an attempt value callers cannot obtain; custom low-level worker integrations use the coherent TaskEngine acquisition API.
- **BREAKING** Replace message- and regex-based recovery with a stable semantic TaskEngine error algebra translated at the Redis boundary.
- **BREAKING** Return typed configuration errors from public task, scheduler, and engine construction instead of throwing or dying for predictable invalid input.
- Route MessagePack, byte conversion, and schema failures through the typed error channel so malformed external data cannot become a defect.

## Capabilities

### New Capabilities

- `effect-api-contracts`: Defines honest public Effect error/service contracts and typed validation behavior for constructors.

### Modified Capabilities

- `storage-protocol`: Strengthens typed corruption handling so serializer and byte-conversion exceptions cannot escape as defects.
- `task-events-stream`: Makes `execute` explicitly expose the same typed terminal protocol and schema requirements as `offer` followed by `wait`.
- `redis-operations`: Replaces diagnostic-string recovery with stable semantic engine failures, including indeterminate writes and relationship limits.
- `scheduler-delivery`: Requires invalid scheduler definitions to fail through a typed configuration error.

## Impact

This intentionally breaks source compatibility across `Task`, `Scheduler`, `TaskQueue`, `TaskEngine`, and codec APIs. Callers must yield or otherwise handle effectful construction, provide the newly visible schema services, and match the exact published error unions. Storage bytes remain compatible; the change is to failure fidelity rather than wire representation.
