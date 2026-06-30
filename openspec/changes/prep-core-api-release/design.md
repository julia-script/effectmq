## Context

The package builds with `tsc -p tsconfig.json` (ESM, `nodenext`, tests excluded), exports a single root entry from `src/index.ts` (`Scheduler`, `Task`, `TaskEngine`, `TaskQueue`), and lints with biome. Current `package.json` is `private: true`, version `1.0.0`, with `effect@4.0.0-beta.85` as a hard dependency.

Findings from auditing `src/` (imports, grep):
- **Unused deps:** `@effect/sql-sqlite-node`, `@xstate/store`, `msgpackr` — zero imports in `src/`. `@effect/platform-node` is imported only by `src/demo.ts` (a scratch file, not exported, not referenced by `index.ts`).
- **Dead code:** `src/Task.ts` is ~107/181 lines commented-out experiments; `src/index.ts` does not export `demo`.
- **effect versions:** installed `4.0.0-beta.85`; npm `beta` tag is `4.0.0-beta.92`; stable `latest` is `3.21.4` (v4 is beta-only).
- **Lint:** one biome formatting nit exists in `src/TaskQueue.test.ts`.
- Build currently passes (`tsc --noEmit` exit 0).

## Goals / Non-Goals

**Goals:**
- Publishable package: `private` removed, `version` `0.1.0`, correct `exports`.
- `effect` as `peerDependency` + `devDependency`, range `>=4.0.0-beta.85`; removed from `dependencies`.
- Remove unused deps and the `demo.ts` scratch file; move `@biomejs/biome` to devDependencies.
- Module-level TSDoc on each core file plus the primary entry points (`make`, `offer`, `complete`, `takeUnsafe`, and the engine service methods at a module level).
- Green release gate: build, typecheck, tests, lint.

**Non-Goals:**
- No runtime/behavior changes to the engine or queue.
- No exhaustive per-member TSDoc (chosen scope is module + key entry points).
- No CI/release-automation setup, no CHANGELOG generation — packaging metadata only.
- Not upgrading the pinned `effect` version itself (the *runtime* dep range widens to `>=` but the dev pin stays at the working `beta.85` unless a bump is trivially clean).

## Decisions

- **`effect` range `>=4.0.0-beta.85`.** Matches "current version and up, but beta for now." Declared in BOTH `peerDependencies` (so consumers bring their own effect) and `devDependencies` (so this repo's build/tests resolve it). Keep the devDependency pinned-or-floored at the version we actually test against.
- **Delete `demo.ts` rather than wire it up.** It's the only `@effect/platform-node` consumer and isn't part of the public API; deleting it removes both the dead file and a dependency.
- **`Task.ts` cleanup is deletion-only.** The commented blocks are superseded experiments (the live `makeTaskSchema` lives in `Schemas.ts`); remove them, keep the active `make`/types.
- **TSDoc altitude: module + entry points.** One `/** ... */` module banner per public file, plus doc comments on `make`/`offer`/`complete`/`takeUnsafe`/`succeed`/`fail` and the `TaskEngine` service. Skip internal Lua-script TSDoc.
- **Keep `tsx` only if a script needs it.** No `src/` import; if no package script uses it after `demo.ts` goes, remove it too. Verify before deleting.
- **Verify build by running it**, not by reasoning: `pnpm build` then assert `dist/index.js` + `dist/index.d.ts` exist and match `exports`.

## Risks / Trade-offs

- **Moving `effect` to peer-only could break this repo's own resolution.** → Also list it as a devDependency; run `pnpm install` + full gate after the edit to confirm.
- **`skipLibCheck: true` masks type issues in deps.** → Out of scope; note it but don't change tsconfig as part of release prep.
- **Deleting `demo.ts` loses a manual example.** → It's unreferenced scratch; if an example is wanted later it belongs in docs/README, not shipped source. Flag, don't preserve.
- **A wider `effect` peer range (`>=beta.85`) may admit incompatible future betas.** → Acceptable for a `0.1.0` pre-release; document the tested version. Tighten later if breakage appears.

## Open Questions

- Should `dist/` be verified as committed/ignored? Default: `dist` is in `files` and built on publish; leave it gitignored. No action unless the user wants prebuilt output committed.
