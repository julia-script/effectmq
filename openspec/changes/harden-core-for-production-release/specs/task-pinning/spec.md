## REMOVED Requirements

### Requirement: Ref acquisition at child creation
**Reason**: Nested offers no longer acquire implicit lifecycle references; creator provenance and explicit result retention are separate capabilities.
**Migration**: Remove `heldBy` from ordinary offers. Request an explicit result-retention hold only when the terminal record must outlive its disposal policy.

### Requirement: Holders must be alive
**Reason**: The refCount holder model is replaced by named, set-idempotent result-retention holds.
**Migration**: Use the holder validation behavior defined by `task-relationships`.

### Requirement: Idempotent re-creation skips ref acquisition
**Reason**: Silently skipping all acquisition prevents a second legitimate holder from retaining an existing task and couples retention to duplicate offer behavior.
**Migration**: Duplicate offers return the existing task unchanged; explicit retention acquisition is independently set-idempotent per holder and task generation.

### Requirement: Task death
**Reason**: The alive/done/dead model conflates execution settlement with record disposal and creates terminal tasks hidden in listless limbo.
**Migration**: Use explicit execution states plus terminal settlement; deletion is governed by completion policy and active result-retention holds.

### Requirement: Death releases held refs and cascades
**Reason**: Unbounded synchronous death cascades can block Redis and make result lifetime implicit.
**Migration**: Holder settlement releases explicit retention holds in bounded, resumable batches.

### Requirement: removeTask rejects pinned tasks and forces death of unpinned ones
**Reason**: Removal should refer to terminal records and explicit holds, not the overloaded concept of task death.
**Migration**: Ordinary removal rejects active holds; administrative force removal is explicit and releases owned holds through bounded maintenance.

### Requirement: createdBy provenance field
**Reason**: Provenance is no longer part of pinning and moves to the `task-relationships` capability.
**Migration**: Read the creator link as informational provenance only; do not infer result retention or execution dependency.
