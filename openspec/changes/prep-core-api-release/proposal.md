## Why

The core API (`TaskEngine`, `TaskQueue`, `Task`, `Scheduler`, `Schemas`) is tested and working, but the package is not release-ready: it's marked `private`, carries unused dependencies, has `effect` as a hard dependency rather than a peer, ships heavy commented-out dead code (notably `Task.ts` is ~60% comments), and the public surface lacks TSDoc. We want a clean, documented, correctly-packaged first release.

## What Changes

- **TSDoc on the core public API** — document the exported surface of `TaskEngine`, `TaskQueue`, `Task`, `Scheduler`, and the public `Schemas`, so editors and generated docs explain each export.
- **Remove dead/commented code** — delete the large commented-out blocks (heaviest in `Task.ts`), and remove the scratch `demo.ts` (not exported, only consumer of `@effect/platform-node`).
- **Verify exports** — confirm `src/index.ts` re-exports exactly the intended public modules and the built `dist/index.d.ts`/`dist/index.js` resolve; drop anything not meant to be public.
- **Dependency hygiene** — remove unused deps (`@effect/sql-sqlite-node`, `@xstate/store`, `msgpackr`, and `@effect/platform-node` once `demo.ts` is gone; drop `tsx` if unused). Move `@biomejs/biome` to devDependencies. **BREAKING (packaging):** move `effect` out of `dependencies` and declare it as **both** a `peerDependency` and a `devDependency`, ranged `>=4.0.0-beta.85` (current beta and up).
- **Build & release metadata** — ensure `pnpm build` (tsc) produces correct `dist` output; set `version` for the first release and unset `private` (or confirm intended publish posture).
- No change to runtime behavior of the engine/queue.

## Capabilities

### New Capabilities
- `release-readiness`: The packaging and public-surface contract for publishing — correct exports, a clean build, declared peer/dev dependency on `effect`, no unused dependencies, and documented public API.

### Modified Capabilities

_None — no runtime requirement behavior changes._

## Impact

- `package.json` — dependency moves/removals, `effect` as peer+dev, version/private/exports metadata.
- `src/Task.ts`, `src/TaskEngine.ts`, `src/Schemas.ts`, `src/index.ts` — dead-code removal + TSDoc.
- `src/demo.ts` — deleted.
- `src/Scheduler.ts`, `src/TaskQueue.ts` — TSDoc only.
- Build output `dist/` verified; lint (`biome`) clean.
