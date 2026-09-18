# Tasks

## 1. Event storage and subscription lifecycle

- [x] 1.1 Implement public event records and the Redis event engine with queue policy validation and idempotent subscription generations; verify source typecheck and registration/isolation tests.
- [x] 1.2 Implement atomic emission, fenced per-subscription delivery and acknowledgements, removal waivers, expiration, and bounded archive/deletion maintenance; verify real Redis lifecycle and race tests.

## 2. Typed API and consumers

- [x] 2.1 Implement schema-typed EventQueue operations with explicit error/service channels, manual delivery control, and managed processing with renewal; verify codec, stale ownership, and managed-handler tests.
- [x] 2.2 Export supported modules, document event usage and maintenance, and add a changeset; verify documentation examples and packed root/subpath imports.

## 3. Integration verification

- [x] 3.1 Run the event integration suite, full existing tests, repository checks, package verification, and strict OpenSpec validation; resolve failures and record results.

## Verification results

- Real Redis/Sentinel full suite: 25 test files, 187 tests passed, including 27 new event tests.
- `pnpm check`: formatting, lint, source/test/script/docs typechecks, architecture,
  generated Lua, documentation examples, changesets, and release checks passed.
- `pnpm verify:package`: packed ESM root/subpath imports and consumer types passed
  (105 tarball entries, including the event Lua source).
- `openspec validate add-subscribable-events --strict`: passed.
- `git diff --check`: passed.
