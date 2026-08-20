# Release-candidate soak evidence

The soak runner is `scripts/soak.ts`. It runs an ordinary managed worker at
bounded concurrency, continuously offers 4 KiB typed payloads, waits for every
accepted task to complete, interrupts the worker and measures drain time, then
promotes a fully due backlog using the maximum supported 1,000-item atomic
maintenance batch.

The runner fails if accepted/completed counts differ, any Redis role reports a
command error, the max batch does not process exactly 1,000 items, Redis grows
by more than 32 MiB, or the forced-GC Node heap grows by more than 64 MiB. CI
runs a short smoke; a release candidate uses at least the default five-minute
duration in its target environment.

## Local release-candidate result

Run: 2026-08-19 on Node 22.23.2, Effect 4.0.0-beta.107, Redis 8.0.6
Docker/loopback, RESP3, macOS arm64. Duration was 300 seconds, concurrency 16,
and payload 4,096 bytes.

| Measure | Result |
| --- | ---: |
| Accepted tasks | 239,757 |
| Completed tasks | 239,757 |
| Sustained rate | 797.5 tasks/s |
| Offer p50 / p95 / p99 / max | 1.05 / 2.40 / 3.31 / 66.20 ms |
| Redis PING p50 / p95 / p99 / max | 0.64 / 1.84 / 2.58 / 88.13 ms |
| Redis memory growth | 15,001,400 bytes |
| Node heap growth after forced GC | 2,869,384 bytes |
| Max batch processed / remaining | 1,000 / 0 |
| Max-batch atomic latency | 26.83 ms |
| Graceful worker shutdown | 1.70 ms |
| Redis role command errors | 0 |

The first five-minute beta.107 trial completed the workload but exposed an
unbounded `Math.max(...samples)` call in the reporter, which overflowed the
JavaScript argument stack at this sample count. The reporter now computes the
maximum iteratively. This full rerun passed every queue, Redis, memory, batch,
and shutdown assertion, proving both the workload and its evidence path at the
release duration.

## Reproduce

```sh
docker run --rm -p 6391:6379 redis:8.0-alpine \
  redis-server --save '' --appendonly no --maxmemory-policy noeviction

EFFECTMQ_REDIS_URL=redis://127.0.0.1:6391 \
  node --expose-gc node_modules/tsx/dist/cli.mjs scripts/soak.ts
```

Override `EFFECTMQ_SOAK_DURATION_MS`, `EFFECTMQ_SOAK_CONCURRENCY`, and
`EFFECTMQ_SOAK_PAYLOAD_BYTES` for a stricter target. Preserve the JSON output
with the exact commit and environment in the release record.
