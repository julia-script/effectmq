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
