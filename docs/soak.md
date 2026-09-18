# Release-candidate soak evidence

The soak runner is `scripts/soak.ts`. It runs an ordinary managed worker at
bounded concurrency, continuously offers 4 KiB typed payloads, waits for every
accepted task to complete, interrupts the worker and measures drain time, then
promotes a fully due backlog using the maximum supported 1,000-item atomic
maintenance batch.

Before the measurement window, the runner completes 1,000 warm-up tasks so
connection pools, worker fibers, codecs, script caches, and Effect runtime hot
paths are initialized. Memory growth is measured after that boundary; this
keeps a short CI smoke from misclassifying process startup as a steady-state
leak while preserving the same limits used by the full release soak.

The runner fails if accepted/completed counts differ, any Redis role reports a
command error, the max batch does not process exactly 1,000 items, Redis grows
by more than 32 MiB, or the forced-GC Node heap grows by more than 64 MiB. CI
runs a short smoke; a release candidate uses at least the default five-minute
duration in its target environment.

## Current rc.1 CI smoke

On 2026-09-18, [main CI run 35383526029](https://github.com/julia-script/effectmq/actions/runs/35383526029)
passed at `d014b10d9240b286b6d823bcf5d2485c23d194bf`, using Effect
`4.0.0-rc.115`, Node 22, and Redis 8.0. The 15-second workload completed all
7,058 offered tasks, processed the 1,000-item maximum maintenance batch, and
reported zero Redis command errors. Redis grew by 2,065,528 bytes; Node heap
fell by 2,973,600 bytes after forced GC. Graceful worker shutdown took 3.60 ms.

`pnpm soak` now runs Node with `--expose-gc --import tsx`, so the workload
itself has access to `globalThis.gc`. The runner rejects a missing GC function
instead of silently sampling without collection. This preserves the original
memory limits. A five-minute rc.1 run in the target environment is still
needed for full candidate soak evidence.

## Historical rc.0 local result

Run: 2026-08-19 on Node 22.23.2, Effect 4.0.0-beta.107, Redis 8.0.6
Docker/loopback, RESP3, macOS arm64. Duration was 300 seconds, concurrency 16,
and payload 4,096 bytes.

| Measure | Result |
| --- | ---: |
| Warm-up tasks | 1,000 |
| Accepted tasks | 233,106 |
| Completed tasks | 233,106 |
| Sustained rate | 775.6 tasks/s |
| Offer p50 / p95 / p99 / max | 1.09 / 2.45 / 3.34 / 175.22 ms |
| Redis PING p50 / p95 / p99 / max | 0.69 / 1.89 / 2.86 / 14.79 ms |
| Redis memory growth | 12,818,608 bytes |
| Reported Node heap growth (forced GC unverified) | -45,740,408 bytes |
| Max batch processed / remaining | 1,000 / 0 |
| Max-batch atomic latency | 26.39 ms |
| Graceful worker shutdown | 3.27 ms |
| Redis role command errors | 0 |

The first five-minute beta.107 trial completed the workload but exposed an
unbounded `Math.max(...samples)` call in the reporter, which overflowed the
JavaScript argument stack at this sample count. The reporter now computes the
maximum iteratively. That run reported passing queue, Redis, memory, batch,
and shutdown assertions. Its launcher predates the GC correction above, so
the reported heap delta is not verified forced-GC evidence and should not be
used to certify the current candidate.

The first 15-second CI smoke on the candidate took its heap baseline before
worker startup and reported 123,461,384 bytes of apparent growth. After moving
the baseline behind the bounded warm-up, the same Node 22 smoke completed
11,019 tasks at 708.4 tasks/s with -81,452,312 bytes measured heap growth. The
five-minute result above is the subsequent fresh-container release run.

## Reproduce

Run these commands from a checkout with dependencies installed. Start Redis
in one terminal, then run `pnpm soak` in a second terminal. The package script
sets the GC flag; a bare `tsx scripts/soak.ts` invocation is not sufficient.

```sh
docker run --rm -p 6391:6379 redis:8.0-alpine \
  redis-server --save '' --appendonly no --maxmemory-policy noeviction

EFFECTMQ_REDIS_URL=redis://127.0.0.1:6391 \
  pnpm soak
```

Override `EFFECTMQ_SOAK_DURATION_MS`, `EFFECTMQ_SOAK_CONCURRENCY`,
`EFFECTMQ_SOAK_PAYLOAD_BYTES`, and `EFFECTMQ_SOAK_WARMUP_TASKS` for a stricter
target. Preserve the JSON output with the exact commit and environment in the
release record.
