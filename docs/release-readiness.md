# 0.3.0-rc.0 release record

Candidate status: published, provenance-verified, and production-ready as a
release candidate. The `rc` tag points to `0.3.0-rc.0`; npm's `latest` tag
intentionally remains on `0.2.0` until the observation period and an explicit
stable promotion decision.

Dependency baseline: `effect@4.0.0-beta.107` and
`@effect/platform-node@4.0.0-beta.107`, resolved from npm's `beta` dist-tag on
2026-08-19. The newer `rc` tag was intentionally not selected.

## Candidate artifact

- Package: `@effectmq/core@0.3.0-rc.0`
- Git tag: `v0.3.0-rc.0`
- Candidate commit: `4594e4bba32cc005b7a50efed1628cdac6f8c1d6`
- Registry tarball: `https://registry.npmjs.org/@effectmq/core/-/core-0.3.0-rc.0.tgz`
- Entries: 64
- Registry integrity: `sha512-7KHgBH516Oh4mI+qKXpUbBoZHxN+knFxPuP30oe3pSuhE0YM3Boesi1qj14g7dW8WU5Le4kgPIR3faOk3kJcMQ==`
- Registry SHA-1: `a6f0c774014af8739f48c131f30f89c42ba8cfc1`
- Local candidate SHA-256: `ac7d6437deac3056151e11612aa9a37508dbfb79144d8476540e9d6b2657128a`
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
| Package consumer | 64 intended entries; Node 22/24 JS load and TS check passed |
| Performance | [Published matrix](./performance.md) |
| Soak | [233,106 accepted/completed over five minutes, zero command errors](./soak.md) |
| Rollback | Pre-upgrade snapshot restored; 9 candidate keys removed; 0 remained |
| Exact candidate CI | [Run 32317781098](https://github.com/julia-script/effectmq/actions/runs/32317781098) passed every required job |
| Protected publication | [Run 32317832772](https://github.com/julia-script/effectmq/actions/runs/32317832772) published through `npm-production` |
| Registry canary | Clean npm install loaded the root and every documented subpath export on Node 26 |
| Provenance | Signed SLSA v1 statement resolves the package digest to candidate commit `4594e4b` |

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

The Release workflow accepted only the successful CI `workflow_run` for commit
`4594e4b`, checked out that SHA explicitly, and verified it before npm could
publish. GitHub `main` protection now also requires the strict
`Exact-commit release gate`, enforces the rule for administrators, requires
linear history and resolved conversations, and rejects force-pushes and branch
deletion.

npm published with OIDC trusted publishing and provenance, without an npm
token. The registry's signed SLSA v1 statement names repository
`julia-script/effectmq`, workflow `.github/workflows/release.yml`, ref `main`,
release run `32317832772`, and exact git commit
`4594e4bba32cc005b7a50efed1628cdac6f8c1d6`. A fresh registry download matched
both advertised digests byte-for-byte, and a clean install loaded every public
export. These controls, the published compatibility/performance evidence, and
the completed rollback rehearsal close the release-candidate readiness gate.
