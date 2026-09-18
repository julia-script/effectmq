# Verification evidence

Validated locally on 2026-09-18 with isolated local Redis servers.

- `EFFECTMQ_TEST_REDIS=local EFFECTMQ_TEST_SENTINEL=local pnpm test` — 27 files, 205 tests passed (187 existing tests plus 18 history/contract tests).
- `pnpm check` — formatting, lint, source/test/script/docs types, architecture, generated Lua, documentation examples, and release trust checks passed. No validation severity settings were changed.
- `pnpm verify:package` — packed ESM root/subpath exports loaded, 109 tarball entries checked, and an isolated consumer compiled a progress definition, managed emission, and typed page reading.
- `pnpm check:changesets` — passed with a minor feature changeset for `@effectmq/core` (the new changeset must be tracked/staged for this command to discover it).
- `openspec validate add-task-progress-streams --strict` — passed.
- Executed the TypeScript example from `apps/docs/content/docs/how-to/report-task-progress.mdx` against an isolated Redis instance, changing only the connection address. It emitted live progress, printed terminal lifecycle events, and returned `Hello, Julia!`.

## Feature evidence

`src/TaskHistory.test.ts` exercises live producer/Worker/reader interaction across a retry; unlimited and explicitly capped history; trimmed cursor boundaries and recovery; independent readers; deadline/token/generation fencing; unchanged duplicate offers and renewals; cancellation/stall attribution; all completion policies; expiry, holds, replacement and removal; concurrent append/settlement/removal; captured writers; interruption; corrupt pages; and lost acknowledgements without replay.

`src/TaskHistory.types.test.ts` checks legacy generic positions, one-argument handlers, discriminated progress inference, lifecycle-only Never progress, Scheduler compatibility, and separate schema encoding/decoding service requirements. It also decodes the committed `task-history-v1.json` fixture and pins the progress envelope bytes.

Compatibility tests treat records with absent history metadata as disabled, keep an existing generation's configuration immutable, reload scripts after SCRIPT FLUSH, and dispose of enabled generations with the upgraded engine before rollback. All queue processes must be upgraded before enabling the feature; existing scripts are content-addressed, so deploying a new process does not upgrade an old process's script.
