# Releasing

Publication is automated only after the `Exact-commit release gate` succeeds on
`main`. The Release workflow checks out that CI run's exact SHA, verifies it,
and runs Changesets. It has `id-token: write` and requests npm provenance; no
long-lived `NPM_TOKEN` is used.

Repository/package administrators must configure these controls once:

1. In npm package settings, add a trusted publisher for this GitHub owner,
   repository, workflow filename `release.yml`, environment `npm-production`,
   and the `npm publish` action. npm requires only the filename, not the full
   `.github/workflows/` path.
2. In GitHub, create the `npm-production` environment. Restrict deployment to
   `main`, add required maintainers for production promotion, and do not store
   an npm publication token. Enable self-review prevention only when another
   required maintainer can approve; otherwise the release is impossible to
   approve.
3. Protect `main` and require the CI job `Exact-commit release gate` before
   merge. Require review for workflow and Changeset changes.
4. Enable npm two-factor authentication for maintainer account changes and
   package settings even though publication itself uses OIDC.

The package manifest's `repository.url` must remain exactly
`https://github.com/julia-script/effectmq`; npm uses it when validating the
GitHub OIDC publication identity.

## Prepare and validate a release

1. Add a Changeset for a package change with `pnpm changeset`. For a change
   that does not need a package release, use `pnpm changeset --empty`.
2. Fetch the base branch and run `pnpm check` and `pnpm check:changesets`.
   `pnpm check` covers format, lint, typechecks, architecture, Lua drift, docs,
   and release configuration. The changeset check is a separate PR policy and
   compares against `origin/main`.
3. Merge the reviewed change after CI succeeds. Following successful main CI
   and any required `npm-production` approval, Changesets opens or updates
   `changeset-release/main` with the next version and changelog. The repository
   is currently in `rc` prerelease mode.
4. Review the generated version PR. CI skips only the new-changeset requirement
   for this repository's `changeset-release/main` branch: versioning has already
   consumed the pending changesets. All quality, test, compatibility, package,
   soak, and release gates still run. Regular PRs, including forks, retain the
   changeset requirement.
5. Merge the version PR, wait for CI on the merged main commit, and approve its
   Release workflow deployment when it is waiting on `npm-production`. The
   workflow publishes the gated version to npm. A merged PR alone does not
   mean publication has completed.

Run the short CI-equivalent soak against an isolated Redis instance with:

```sh
EFFECTMQ_REDIS_URL=redis://127.0.0.1:6391 \
  EFFECTMQ_SOAK_DURATION_MS=15000 pnpm soak
```

`pnpm soak` uses `node --expose-gc --import tsx scripts/soak.ts`. This exposes
GC in the workload process. Passing `--expose-gc` to the `tsx` CLI launcher
does not expose it in its child process. Omit the duration override for the
five-minute candidate soak; see [Soak evidence](./soak.md).

## Candidate checklist

- Changeset and intended prerelease version are reviewed.
- Compatibility, restart/Sentinel fault, package, benchmark, and soak evidence
  identify the exact candidate commit.
- `pnpm verify:package` passes and its file list is reviewed.
- Backup restore and application rollback are rehearsed against that tarball.
- Security and support-policy changes are reviewed.
- The npm package's trusted-publisher identity exactly matches the workflow and
  protected environment; a fork or differently named workflow cannot publish.

After merge, inspect npm provenance and compare its source SHA to the gated
commit. Install the published version into the ESM consumer again, deploy a
canary, and follow the order in [Upgrade and rollback](./upgrade-and-rollback.md).
If provenance, integrity, canary decoding, or queue invariants differ, stop the
promotion and deprecate the candidate version rather than overwriting it.
