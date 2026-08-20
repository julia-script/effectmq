import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Effect, Layer } from "effect";
import { NodeRedisPool, RedisPool, TaskEngine } from "../src/index.js";

const redisUrl = process.env.EFFECTMQ_REDIS_URL ?? "redis://127.0.0.1:6379";
const quick = process.env.EFFECTMQ_BENCH_QUICK === "1";
const taskCount = Number(
  process.env.EFFECTMQ_BENCH_COUNT ?? (quick ? 50 : 500),
);

const numbers = (name: string, defaults: ReadonlyArray<number>) => {
  const raw = process.env[name];
  const result = raw === undefined ? defaults : raw.split(",").map(Number);
  if (
    result.length === 0 ||
    result.some((value) => !Number.isSafeInteger(value) || value < 1)
  )
    throw new Error(`Invalid ${name}`);
  return result;
};

const payloadSizes = numbers(
  "EFFECTMQ_BENCH_PAYLOADS",
  quick ? [64, 1_024] : [64, 1_024, 16_384],
);
const concurrencies = numbers(
  "EFFECTMQ_BENCH_CONCURRENCY",
  quick ? [1, 8] : [1, 8, 32],
);
const backlogs = numbers(
  "EFFECTMQ_BENCH_BACKLOGS",
  quick ? [100] : [100, 1_000],
);
const batchSizes = numbers(
  "EFFECTMQ_BENCH_BATCHES",
  quick ? [10, 100] : [10, 100, 1_000],
);
if (!Number.isSafeInteger(taskCount) || taskCount < 1)
  throw new Error("Invalid EFFECTMQ_BENCH_COUNT");

const quantile = (samples: readonly number[], percentile: number) => {
  const sorted = [...samples].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * percentile) - 1)] ?? 0;
};

const latency = (samples: ReadonlyArray<number>) => ({
  p50: quantile(samples, 0.5),
  p95: quantile(samples, 0.95),
  p99: quantile(samples, 0.99),
  max: Math.max(...samples),
});

const program = Effect.gen(function* () {
  const redis = yield* RedisPool.RedisPool;
  const serverInfo = yield* redis.send<string>("INFO", "server");
  const redisVersion =
    /^redis_version:([^\r\n]+)$/m.exec(serverInfo)?.[1] ?? "unknown";
  const runId = crypto.randomUUID();
  const throughput: Array<Record<string, unknown>> = [];
  const maintenance: Array<Record<string, unknown>> = [];
  yield* TaskEngine.setMockTime(2_000_000_000_000);

  for (const payloadBytes of payloadSizes) {
    for (const concurrency of concurrencies) {
      const engine = yield* TaskEngine.makeWithRedis(redis, {
        debugMode: true,
        maintenanceBatchSize: Math.max(...batchSizes),
      });
      const queue = `bench-throughput-${runId}-${payloadBytes}-${concurrency}`;
      const payload = "x".repeat(payloadBytes);
      const samples: number[] = [];
      const started = performance.now();
      yield* Effect.forEach(
        Array.from({ length: taskCount }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            const operationStarted = performance.now();
            yield* engine.createTask({
              prefix: queue,
              id: String(index),
              name: "throughput",
              payload,
              delay: 0,
              maxRetries: 0,
              onSuccessPolicy: "delete",
              onFailurePolicy: "delete",
            });
            const attempt = yield* engine.takeTask(queue, 30_000);
            if (attempt === null) return yield* Effect.die("Expected task");
            yield* engine.writeSuccess(
              queue,
              attempt.task.id,
              attempt.leaseToken,
              null,
            );
            samples.push(performance.now() - operationStarted);
          }),
        { concurrency },
      );
      const elapsedMs = performance.now() - started;
      throughput.push({
        tasks: taskCount,
        payloadBytes,
        concurrency,
        elapsedMs,
        tasksPerSecond: (taskCount * 1_000) / elapsedMs,
        latencyMs: latency(samples),
      });
    }
  }

  for (const backlog of backlogs) {
    for (const batchSize of batchSizes) {
      const engine = yield* TaskEngine.makeWithRedis(redis, {
        debugMode: true,
        maintenanceBatchSize: batchSize,
      });
      const queue = `bench-sweep-${runId}-${backlog}-${batchSize}`;
      for (let index = 0; index < backlog; index++) {
        yield* engine.createTask({
          prefix: queue,
          id: String(index),
          name: "sweep",
          payload: null,
          delay: 1_000,
          maxRetries: 0,
          onSuccessPolicy: "delete",
          onFailurePolicy: "delete",
        });
      }
      yield* TaskEngine.stepMockTime(1_001);
      const samples: number[] = [];
      let processed = 0;
      const started = performance.now();
      while (processed < backlog) {
        const sweepStarted = performance.now();
        const health = yield* engine.maintain(queue);
        samples.push(performance.now() - sweepStarted);
        if (health.processed > batchSize) {
          return yield* Effect.die(
            `processed ${health.processed}; configured maximum ${batchSize}`,
          );
        }
        processed += health.processed;
        if (health.processed === 0 && health.dueBacklog > 0) {
          return yield* Effect.die("maintenance made no progress");
        }
      }
      const elapsedMs = performance.now() - started;
      maintenance.push({
        backlog,
        batchSize,
        sweeps: samples.length,
        elapsedMs,
        itemsPerSecond: (processed * 1_000) / elapsedMs,
        sweepLatencyMs: latency(samples),
      });
    }
  }

  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    environment: {
      node: process.version,
      platform: `${process.platform}/${process.arch}`,
      redis: redisVersion,
      client: "node-redis 6.1.x",
      protocol: "RESP3",
    },
    parameters: {
      taskCount,
      payloadSizes,
      concurrencies,
      backlogs,
      batchSizes,
    },
    throughput,
    maintenance,
  };
});

console.log(
  JSON.stringify(
    await Effect.runPromise(
      program.pipe(
        Effect.provide(
          Layer.merge(NodeRedisPool.layer({ url: redisUrl }), NodeCrypto.layer),
        ),
      ),
    ),
    null,
    2,
  ),
);
