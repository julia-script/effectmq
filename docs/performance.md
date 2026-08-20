# Performance evidence

This is the release-candidate baseline, not a universal capacity claim. It is
committed so regressions and operational choices can be compared against an
exact workload. The runner is `scripts/benchmark-maintenance.ts` and emits
machine-readable JSON including its environment and parameters.

## Method

The throughput scenario measures an end-to-end task lifecycle: atomic create,
acquire with a unique lease token, and successful acknowledgement. Each row
uses 500 tasks on a new queue and reports per-lifecycle p50/p95/p99 plus total
completed lifecycles per second. Concurrency is the number of in-flight
lifecycles; payload bytes are stored in the task payload.

The maintenance scenario creates a fully due delayed backlog, then drains it.
Every Lua invocation asserts `processed <= maintenanceBatchSize`. Latency is
per atomic sweep and throughput is promoted items per second. Large batches
increase Redis's uninterrupted script time; the highest throughput setting is
not automatically the safest production setting.

Baseline environment: 2026-08-19, Node 22.23.2, Effect 4.0.0-beta.107,
Redis 8.0.6 in Docker, node-redis 6.1.x, RESP3, macOS arm64, loopback
connection, persistence disabled. The machine was a developer workstation, so
use these figures for regression detection and shape, not infrastructure
sizing.

## Task lifecycle results

| Payload | Concurrency | tasks/s | p50 ms | p95 ms | p99 ms | max ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 592.3 | 1.59 | 2.30 | 2.64 | 10.56 |
| 64 B | 8 | 2,179.9 | 3.52 | 5.14 | 7.10 | 8.64 |
| 64 B | 32 | 2,922.4 | 10.25 | 17.16 | 22.15 | 23.28 |
| 1 KiB | 1 | 640.8 | 1.44 | 2.10 | 3.82 | 8.25 |
| 1 KiB | 8 | 2,346.9 | 3.24 | 4.61 | 5.26 | 6.15 |
| 1 KiB | 32 | 2,968.0 | 10.67 | 12.66 | 13.27 | 13.63 |
| 16 KiB | 1 | 483.4 | 1.73 | 4.50 | 6.42 | 8.35 |
| 16 KiB | 8 | 1,623.9 | 4.55 | 7.52 | 11.32 | 12.31 |
| 16 KiB | 32 | 2,035.6 | 15.62 | 17.09 | 17.67 | 19.26 |

At this scale, concurrency 8 materially improves throughput with lower latency
than 32, while concurrency 32 provides the highest throughput in every payload
row. Capacity planning should choose between those profiles rather than assume
the highest concurrency is universally best.

## Due-backlog sweep results

| Backlog | Batch | sweeps | items/s | p50 ms | p95 ms | p99 ms | max ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 10 | 10 | 11,688 | 0.74 | 1.45 | 1.45 | 1.45 |
| 100 | 100 | 1 | 36,507 | 2.74 | 2.74 | 2.74 | 2.74 |
| 100 | 1,000 | 1 | 39,097 | 2.56 | 2.56 | 2.56 | 2.56 |
| 1,000 | 10 | 100 | 10,672 | 0.73 | 1.23 | 5.02 | 6.28 |
| 1,000 | 100 | 10 | 29,503 | 3.57 | 4.00 | 4.00 | 4.00 |
| 1,000 | 1,000 | 1 | 33,379 | 29.96 | 29.96 | 29.96 | 29.96 |

The 1,000-item batch occupies Redis for about 30 ms in this loopback baseline.
The default batch of 100 is the safer starting point: it drains substantially
faster than batch 10 while keeping individual atomic sections at 4 ms here.

## Reproduce

Start an isolated Redis with production's `noeviction` policy, then run:

```sh
docker run --rm -p 6391:6379 redis:8.0-alpine \
  redis-server --save '' --appendonly no --maxmemory-policy noeviction

EFFECTMQ_REDIS_URL=redis://127.0.0.1:6391 pnpm bench:maintenance
```

Parameters can be overridden without editing the runner:

```sh
EFFECTMQ_BENCH_COUNT=2000 \
EFFECTMQ_BENCH_PAYLOADS=64,1024,16384 \
EFFECTMQ_BENCH_CONCURRENCY=1,8,32 \
EFFECTMQ_BENCH_BACKLOGS=1000,10000 \
EFFECTMQ_BENCH_BATCHES=10,100,1000 \
pnpm bench:maintenance
```

Use `EFFECTMQ_BENCH_QUICK=1` for a smoke run. Release evidence must use the
full defaults or stricter parameters, a supported Node version, and must record
Redis persistence/TLS settings when they differ from this baseline.
