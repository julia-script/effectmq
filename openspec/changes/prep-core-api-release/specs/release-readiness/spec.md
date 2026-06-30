## ADDED Requirements

### Requirement: Public exports are correct and intentional

The package SHALL export exactly the intended public modules from its entry point, and the built type and JS outputs SHALL resolve from the declared `exports` map.

#### Scenario: entry point re-exports the core modules
- **WHEN** a consumer imports from the package root
- **THEN** `TaskEngine`, `TaskQueue`, `Task`, and `Scheduler` are available, and no scratch/demo module is exported

#### Scenario: built outputs resolve
- **WHEN** `pnpm build` runs
- **THEN** `dist/index.js` and `dist/index.d.ts` exist and match the paths in `package.json` `exports`

### Requirement: effect is a peer and dev dependency

The package SHALL declare `effect` as a peer dependency (not a hard runtime dependency) and also as a dev dependency, ranged to the current beta and up.

#### Scenario: effect moved to peer + dev
- **WHEN** `package.json` is inspected
- **THEN** `effect` appears under `peerDependencies` and `devDependencies` with a range of `>=4.0.0-beta.85`, and does NOT appear under `dependencies`

### Requirement: No unused dependencies

The package SHALL NOT list dependencies that are unused by the shipped source, and tooling-only packages SHALL live under devDependencies.

#### Scenario: unused deps removed
- **WHEN** `package.json` is inspected after cleanup
- **THEN** dependencies not imported by `src/` (excluding tests) are removed and `@biomejs/biome` is under devDependencies

### Requirement: Clean build, types, tests, and lint

The project SHALL build, typecheck, pass tests, and lint cleanly as a release gate.

#### Scenario: release gate passes
- **WHEN** `pnpm build`, `tsc --noEmit`, `vitest run`, and `biome check src/` are run
- **THEN** each completes with no errors

### Requirement: Core public API is documented

Each exported member of the core API SHALL carry a TSDoc comment describing its purpose, and commented-out dead code SHALL be removed.

#### Scenario: public surface has TSDoc
- **WHEN** the exported members of `TaskEngine`, `TaskQueue`, `Task`, `Scheduler`, and public `Schemas` are reviewed
- **THEN** each has a TSDoc block, and no large commented-out code blocks remain in the source
