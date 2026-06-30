## 1. Remove dead code and scratch files

- [x] 1.1 Delete `src/demo.ts` (scratch, not exported; only consumer of `@effect/platform-node`)
- [x] 1.2 Remove the large commented-out blocks in `src/Task.ts`, keeping the live `make`, `TaskDefinition`, and `Task` type
- [x] 1.3 Grep the rest of `src/` for stray commented-out code (`TaskEngine.ts`, `Schemas.ts`, `index.ts`) and remove any dead blocks

## 2. Dependency hygiene (package.json)

- [x] 2.1 Remove unused dependencies: `@effect/sql-sqlite-node`, `@xstate/store`, `msgpackr`, and `@effect/platform-node` (after `demo.ts` is gone)
- [x] 2.2 Move `@biomejs/biome` from `dependencies` to `devDependencies`; remove `tsx` if no `src/` import or package script uses it
- [x] 2.3 Remove `effect` from `dependencies`; add it to `peerDependencies` as `>=4.0.0-beta.85` and to `devDependencies` (pinned at the tested version)
- [x] 2.4 Run `pnpm install` to refresh the lockfile and confirm resolution still works

## 3. Release metadata and exports

- [x] 3.1 Set `version` to `0.1.0` and remove `"private": true`
- [x] 3.2 Confirm `src/index.ts` exports exactly the intended public modules (`Scheduler`, `Task`, `TaskEngine`, `TaskQueue`); add/remove as needed
- [x] 3.3 Verify `exports`/`types`/`files` in `package.json` point at the real built paths

## 4. TSDoc on the core API

- [x] 4.1 Add a module-level TSDoc banner to each public file: `TaskEngine.ts`, `TaskQueue.ts`, `Task.ts`, `Scheduler.ts`, `Schemas.ts`
- [x] 4.2 Document the key entry points: `Task.make`, `TaskQueue.make`/`offer`/`complete`/`takeUnsafe`/`succeed`/`fail`, `Scheduler.make`, and the `TaskEngine` service
- [x] 4.3 Document the public schema helpers in `Schemas.ts` (`makeTaskSchema`, completion-policy and error schemas)

## 5. Release gate (verify)

- [x] 5.1 `pnpm build` succeeds; assert `dist/index.js` and `dist/index.d.ts` exist
- [x] 5.2 `npx tsc --noEmit` clean
- [x] 5.3 `pnpm test` (vitest) all green
- [x] 5.4 `npx biome check src/` clean (fix the existing formatting nit)
