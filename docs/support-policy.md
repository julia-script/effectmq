# Support and compatibility policy

The first production release has a deliberately narrow platform contract.
Supported means the combination is exercised in required CI, receives bug and
security fixes, and is eligible for production incident reports.

| Component | Supported | Notes |
| --- | --- | --- |
| Node.js | 20.19+, 22, 24 | Active or maintenance LTS lines tested as ESM |
| Redis | 7.2, 7.4, 8.0 | Official Redis server; persistence and `noeviction` required in production |
| Topology | standalone, Sentinel | Three-Sentinel quorum/failover test is required |
| Redis Cluster | unsupported | Fails startup with `UnsupportedRedisTopology` |
| RESP | RESP2, RESP3 | Text and binary command paths tested on standalone |
| node-redis | 6.1.x | The direct runtime client; upgrades require the full matrix |
| Effect | 4.0.0-beta.107 through compatible 4.x | Peer dependency; the minimum is tested |
| Module system | Node ESM | CommonJS `require` is not a supported consumer boundary |

Valkey, Dragonfly, KeyDB, managed “Redis-compatible” products, proxies, Redis
Cluster, and read-replica routing are not part of the production support
contract even if basic commands appear to work. A compatible product becomes
supported only after it passes the script, failover, persistence, event,
retention, soak, and fault suites and is added here.

## Test matrix

Required pull-request gates run:

- formatting, linting, typechecking, generated Lua drift, package, docs, and
  the unit/integration/fault suite on Node 22;
- the ESM package consumer on Node 20, 22, and 24;
- Redis 7.2, 7.4, and 8.0 with RESP2 and RESP3;
- standalone restart and a primary/replica/three-Sentinel failover, including
  post-promotion `NOSCRIPT` reload.

The lockfile is the tested dependency set. Renovation of Effect or node-redis
must use a pull request and pass this entire matrix. An application may use a
newer compatible Effect 4.x peer, but a regression must be reproduced on the
latest EffectMQ-tested version before it is treated as a library defect.

## Release and maintenance

Minor releases may add APIs and expand the compatibility matrix. Patch
releases preserve the v1 storage and public API contracts. Removing a platform,
changing delivery semantics, or changing the storage/wire protocol requires a
documented migration and a major release once the package reaches 1.0.

Security fixes target the latest minor release. During the pre-1.0 period,
critical correctness fixes may require upgrading to the newest minor. Every
release publishes its exact compatibility, benchmark, soak, package, and
rollback evidence; support claims are never inferred from semver alone.
