# Operating EffectMQ

EffectMQ supports Redis in standalone and Sentinel deployments. Redis Cluster
is intentionally unsupported: queue transitions use multi-key atomic Lua
scripts and the keys are not constrained to one cluster hash slot. Configuring
`topology: "cluster"`, or connecting to a server reporting
`cluster_enabled:1`, fails startup with `UnsupportedRedisTopology` before queue
traffic is accepted.

## Connection configuration

The layer opens independent producer, worker, and maintenance pools during
startup. If any role cannot connect, the layer does not become available.
Every pool drains in-flight commands and closes when its Effect scope closes.

```ts
import { NodeRedisPool } from "@effectmq/core"
import { Config, Effect, Layer, Redacted } from "effect"

const RedisLive = Layer.unwrap(
  Config.all({
    url: Config.string("REDIS_URL"),
    username: Config.string("REDIS_USERNAME"),
    password: Config.redacted("REDIS_PASSWORD")
  }).pipe(
    Effect.map(({ password, url, username }) =>
      NodeRedisPool.layer({
        topology: "standalone",
        url,
        username,
        password: Redacted.value(password),
        socket: {
          connectTimeout: 5_000,
          reconnectStrategy: (attempt) => Math.min(50 * 2 ** attempt, 2_000)
        },
        commandOptions: { timeout: 2_000 },
        pool: {
          minimum: 1,
          maximum: 16,
          acquireTimeout: 2_000,
          cleanupDelay: 5_000
        }
      })
    )
  )
)
```

For TLS, use a `rediss://` URL or node-redis socket TLS options. Supply CA and
client certificate material from a secret manager; do not put it in source or
logs. For Sentinel, TLS and ACL settings for Redis nodes belong in
`nodeClientOptions`; Sentinel credentials belong in `sentinelClientOptions`.

```ts
import { NodeRedisPool } from "@effectmq/core"
import { Config, Effect, Layer, Redacted } from "effect"

const SentinelRedisLive = Layer.unwrap(
  Config.all({
    redisUsername: Config.string("REDIS_USERNAME"),
    redisPassword: Config.redacted("REDIS_PASSWORD"),
    sentinelUsername: Config.string("SENTINEL_USERNAME"),
    sentinelPassword: Config.redacted("SENTINEL_PASSWORD")
  }).pipe(
    Effect.map(
      ({
        redisPassword,
        redisUsername,
        sentinelPassword,
        sentinelUsername
      }) =>
        NodeRedisPool.layer({
          topology: "sentinel",
          sentinel: {
            name: "effectmq-primary",
            sentinelRootNodes: [
              { host: "sentinel-a.internal", port: 26379 },
              { host: "sentinel-b.internal", port: 26379 },
              { host: "sentinel-c.internal", port: 26379 }
            ],
            masterPoolSize: 16,
            maxCommandRediscovers: 20,
            scanInterval: 1_000,
            commandOptions: { timeout: 2_000 },
            nodeClientOptions: {
              username: redisUsername,
              password: Redacted.value(redisPassword),
              socket: { connectTimeout: 5_000 }
            },
            sentinelClientOptions: {
              username: sentinelUsername,
              password: Redacted.value(sentinelPassword),
              socket: { connectTimeout: 5_000 }
            }
          }
        })
    )
  )
)
```

Use three Sentinel processes across independent failure domains and a quorum
of two. Set `down-after-milliseconds` above ordinary network jitter and set
`failover-timeout` to the recovery objective. A write whose connection fails
during failover has an indeterminate outcome. Retry `offer` with the same task
identity; do not create a new id to “make sure.” A promoted primary has an empty
script cache, which EffectMQ handles through `NOSCRIPT` reload.

## Redis durability

Choose Redis persistence from the amount of accepted work the product may lose
after simultaneous primary and replica loss:

- Use AOF `appendfsync everysec` for the common durability/latency balance. The
  documented loss window is approximately the most recent second, plus any
  replication lag that precedes a failover.
- Use `appendfsync always` only after measuring its latency and throughput cost.
- Keep periodic RDB snapshots for faster full restores and an independent
  backup chain. RDB alone can lose all writes since the last snapshot.
- Monitor replica lag and require at least one healthy replica before planned
  maintenance. Sentinel promotion cannot recover writes that never reached the
  promoted replica.

Set `maxmemory-policy noeviction`. Evicting queue keys silently violates task,
event, hold, and cursor invariants. Capacity alerts must fire before Redis
reaches `maxmemory`; leave headroom for Lua execution, replication buffers,
AOF rewrite, and fork copy-on-write memory.

Back up both RDB and AOF according to the Redis version's documented procedure.
Regularly restore into an isolated Redis instance, run
`effectmq-storage-inspect`, verify the protocol/schema identity, and sample
waiting, active, terminal, event, and retention indexes. Never test restore by
overwriting the active primary. During disaster recovery, stop producers and
workers, restore Redis, inspect it, then start maintenance, workers, and finally
producers. Tasks acknowledged after the restored point may need upstream
reconciliation.

## ACL policy

Give EffectMQ a dedicated Redis ACL user and key prefix. It needs ordinary key,
sorted-set, list, set, hash, stream, pub/sub, scripting, and server-time
commands used by the engine. It also needs `SCRIPT LOAD`, `EVALSHA`, and
read-only `INFO cluster` at startup. Do not grant administrative commands such
as `FLUSHALL`, `CONFIG`, `SHUTDOWN`, `MODULE`, or `FUNCTION`. Validate the exact
allowlist against integration tests whenever engine commands change.

Use separate credentials for Sentinel discovery where the deployment supports
Sentinel ACLs. Rotate by creating the replacement user, rolling clients to the
new secret, confirming readiness, and only then removing the old user.

## Capacity and latency

Size each role's pool from observed command concurrency, not worker count.
Start with `minimum: 1` and a small finite `maximum` (8–32 is typical), then
watch pool wait time, Redis CPU, connected clients, and p95/p99 command latency.
The library rejects a maximum above 1,000 and inconsistent bounds, but an
operationally safe value is normally far lower.

Keep queue and maintenance limits finite. Alert on:

- queue depth and oldest-task age;
- due and expired-lease backlog plus maintenance sweep lag;
- Redis command errors, reconnects, and script reloads;
- ownership loss and retention failures;
- Redis memory, CPU, latency, rejected connections, replication lag, AOF/RDB
  failures, and Sentinel primary changes.

Retention is capacity planning, not just cleanup. Estimate task record,
payload, result, failure history, event stream, terminal/dead-letter index, and
relationship volume at peak arrival rate. Configure bounded retention and
verify that cleanup throughput stays above creation throughput.

## Health and shutdown

`NodeRedisPool.RedisConnectionHealth` exposes a passive `snapshot` and an
active `readiness` probe. Readiness pings producer, worker, and maintenance
connections. The snapshot contains topology, role state, error/reconnect
counters, and timestamps only; it never includes URLs, ACL users, secrets, or
raw connection errors. A degraded role should remove the instance from ready
traffic while liveness remains healthy long enough for reconnect.

Readiness converts only expected Redis command failures to `false`; defects and
interruption remain observable. Each scoped client removes its event listeners
before shutdown, awaits graceful close, and falls back to forced destroy when
close rejects. If acquisition of a later workload role fails, already-acquired
roles are released by the same scope.

On deployment shutdown, first stop accepting new offers, then stop scheduler
materialization, drain workers within a bounded grace period, and close the
application Effect scope. If the grace period expires, interrupt the process;
leases are fenced and expired attempts are recovered according to
`maxStalledCount`. Never report a handler acknowledgement after its lease has
been lost.

## Failover drill

Before every release candidate:

1. Start a primary, replica, and three Sentinels with production-equivalent
   ACL, TLS, timeout, and persistence settings.
2. Run producers, workers, scheduler, maintenance, and waiters under load.
3. Kill the primary without a clean shutdown.
4. Confirm Sentinel reaches quorum and promotes the expected replica.
5. Confirm all three connection roles reconnect, `NOSCRIPT` reload occurs,
   duplicate offers resolve to the existing generation, and outstanding leases
   either complete under their token or expire safely.
6. Reintroduce the old primary as a replica and confirm replication catches up.
7. Record promotion time, unavailable interval, indeterminate writes,
   duplicates, Redis latency, script reloads, and any operator action.

If the observed loss or recovery window exceeds the service objective, stop
the release. Do not use manual primary promotion as a routine reconnect tool.
