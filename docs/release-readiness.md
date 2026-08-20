# 0.3.0-rc.0 release record

Candidate status: locally cut and verified; not yet published or production
promoted. Production readiness remains false until npm's trusted publisher is
bound to the protected workflow and the exact candidate commit passes CI.

Dependency baseline: `effect@4.0.0-beta.107` and
`@effect/platform-node@4.0.0-beta.107`, resolved from npm's `beta` dist-tag on
2026-08-19. The newer `rc` tag was intentionally not selected.

## Candidate artifact

- Package: `@effectmq/core@0.3.0-rc.0`
- Tarball: `effectmq-core-0.3.0-rc.0.tgz`
- Entries: 64
- SHA-256: `0af8683d073f151922d6f612efa8b176643d079d1330a8d3d5f62849018374b7`
- Local artifact: `/tmp/effectmq-0.3.0-rc.0/effectmq-core-0.3.0-rc.0.tgz`

The package gate installed this tarball into clean JavaScript and TypeScript ESM
consumers and loaded every root/subpath export. No test, scratchpad, OpenSpec,
research, or editor file was present.

## Evidence

| Gate | Result |
| --- | --- |
| Format, lint, typecheck, Lua drift, docs | Passed |
| Default suite | 97 passed, 2 environment-gated skipped |
| Local restart/Sentinel fault suite | 3 passed |
| Redis 7.2/7.4/8.0, RESP2/RESP3 | Passed locally |
| Standalone and three-Sentinel failover | Passed |
| Generated model sequences | 32 deterministic lifecycles passed |
| Package consumer | 64 intended entries; JS load and TS check passed |
| Performance | [Published matrix](./performance.md) |
| Soak | [233,106 accepted/completed over five minutes, zero command errors](./soak.md) |
| Rollback | Pre-upgrade snapshot restored; 9 candidate keys removed; 0 remained |

Rollback was rehearsed against an isolated Redis namespace with
`pnpm rehearse:rollback`. It stored and completed candidate work, changed a
pre-upgrade sentinel value, removed every candidate namespace key, restored the
snapshot with Redis `DUMP`/`RESTORE`, and verified the exact original value.
Measured rollback mutation time was 3.39 ms on loopback after the beta.107
refresh.

## Publication controls

The repository's `npm-production` environment exists with a required reviewer
and a `main` deployment branch policy. Because that reviewer is currently the
sole maintainer, self-review prevention is disabled so the protected deployment
can actually be approved. The Release workflow runs only after successful CI
for the same `main` SHA, verifies its checkout, requests an OIDC token, enables
npm provenance, and contains no npm token secret. The package manifest records
the exact public GitHub repository required by npm's OIDC validation.

The npm trusted-publisher record now identifies `julia-script/effectmq` and
workflow `release.yml`, with `npm publish` and `npm stage publish` permissions.
The protected GitHub environment remains restricted to `main` and requires a
reviewer.

Still required before checking the final production boxes:

1. Commit/push the candidate, let every exact-commit CI job pass, and require
   that gate in `main` branch protection after the check name exists remotely.
2. Publish the `rc` tag through the protected workflow, verify npm provenance
   SHA and integrity, then canary the package before production promotion.

The package must not be marked production-ready merely because the local
artifact passed. Those controls are part of the product's release guarantee.
