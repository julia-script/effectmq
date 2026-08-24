import { Effect, Fiber, Ref, Schema } from "effect";
import {
  NodeRedisPool,
  RedisPool,
  Task,
  TaskEngine,
  TaskQueue,
  Worker,
} from "../src/index.js";

const durationMs = Number(process.env.EFFECTMQ_SOAK_DURATION_MS ?? 300_000);
const concurrency = Number(process.env.EFFECTMQ_SOAK_CONCURRENCY ?? 16);
const payloadBytes = Number(process.env.EFFECTMQ_SOAK_PAYLOAD_BYTES ?? 4_096);
const warmupTasks = Number(process.env.EFFECTMQ_SOAK_WARMUP_TASKS ?? 1_000);
const redisUrl = process.env.EFFECTMQ_REDIS_URL ?? "redis://127.0.0.1:6379";
for (const [name, value] of Object.entries({
  durationMs,
  concurrency,
  payloadBytes,
  warmupTasks,
})) {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`Invalid ${name}`);
}

const quantile = (samples: ReadonlyArray<number>, p: number) => {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)] ?? 0;
};
const summary = (samples: ReadonlyArray<number>) => ({
  p50: quantile(samples, 0.5),
  p95: quantile(samples, 0.95),
  p99: quantile(samples, 0.99),
  max: samples.reduce((current, sample) => Math.max(current, sample), 0),
});
const infoNumber = (info: string, field: string) =>
  Number(new RegExp(`^${field}:(\\d+)$`, "m").exec(info)?.[1] ?? 0);
const memorySnapshot = () => {
  const gc = (globalThis as { gc?: () => void }).gc;
  gc?.();
  gc?.();
  gc?.();
  return process.memoryUsage();
};

const run = Effect.scoped(
  Effect.gen(function* () {
    const redis = yield* RedisPool.RedisPool;
    const engine = yield* TaskEngine.TaskEngine;
    const connectionHealth = yield* NodeRedisPool.RedisConnectionHealth;
    if (!(yield* connectionHealth.readiness)) {
      return yield* Effect.die("Redis roles are not ready");
    }
    const runId = crypto.randomUUID();
    const task = Task.make({
      name: "soak-task",
      schemaId: "effectmq/soak/v1",
      payload: { id: Schema.String, body: Schema.String },
      success: Schema.String,
      error: Schema.Never,
      idempotencyKey: (payload) => payload.id,
      storageLimits: { maxEventEntries: 1_000 },
      retention: {
        taskRecordMs: 0,
        resultMs: 0,
        terminalIndexMs: 0,
        deadLetterMs: 0,
        eventMs: 60_000,
      },
    });
    const queue = TaskQueue.make(`soak-${runId}`, task);
    const completed = yield* Ref.make(0);
    const worker = Worker.make(
      queue,
      (task) =>
        Ref.update(completed, (count) => count + 1).pipe(
          Effect.as(task.payload.id),
        ),
      {
        concurrency,
        drainTimeout: "30 seconds",
        maintenanceInterval: "10 millis",
        pollInterval: "2 millis",
        processing: {
          lockRefresh: "1 second",
          lockTimeout: "5 seconds",
        },
      },
    );

    const pingSamples: number[] = [];
    const offerSamples: number[] = [];
    const workerFiber = yield* Worker.run(worker).pipe(Effect.forkChild);
    const body = "x".repeat(payloadBytes);

    // Establish connection pools, worker fibers, codecs, script caches, and
    // Effect runtime hot paths before measuring steady-state memory. Without
    // this boundary a short CI smoke run mostly measures process startup,
    // while the five-minute release soak measures the actual queue plateau.
    for (let index = 0; index < warmupTasks; index++) {
      yield* TaskQueue.offer(queue, { id: `warmup-${index}`, body });
    }
    const warmupDeadline = Date.now() + 30_000;
    while (
      (yield* Ref.get(completed)) < warmupTasks &&
      Date.now() < warmupDeadline
    ) {
      yield* Effect.sleep("10 millis");
    }
    const warmed = yield* Ref.get(completed);
    if (warmed !== warmupTasks) {
      return yield* Effect.die(
        `worker warm-up timed out: offered ${warmupTasks}, completed ${warmed}`,
      );
    }
    yield* Ref.set(completed, 0);
    const memoryBefore = yield* redis.send<string>("INFO", "memory");
    const processMemoryBefore = memorySnapshot();

    const startedAt = Date.now();
    let offered = 0;
    while (Date.now() - startedAt < durationMs) {
      const started = performance.now();
      yield* TaskQueue.offer(queue, { id: String(offered), body });
      offerSamples.push(performance.now() - started);
      offered++;
      if (offered % 100 === 0) {
        const pingStarted = performance.now();
        yield* redis.send("PING");
        pingSamples.push(performance.now() - pingStarted);
      }
    }

    const drainDeadline = Date.now() + 30_000;
    while (
      (yield* Ref.get(completed)) < offered &&
      Date.now() < drainDeadline
    ) {
      yield* Effect.sleep("10 millis");
    }
    const completedCount = yield* Ref.get(completed);
    if (completedCount !== offered) {
      return yield* Effect.die(
        `worker drain timed out: offered ${offered}, completed ${completedCount}`,
      );
    }
    const shutdownStarted = performance.now();
    yield* Fiber.interrupt(workerFiber);
    const gracefulShutdownMs = performance.now() - shutdownStarted;

    // Exercise the maximum supported atomic maintenance bound with a full due
    // backlog and assert that one call does not exceed it.
    const batch = TaskEngine.maxMaintenanceBatchSize;
    const stressQueue = `soak-max-batch-${runId}`;
    yield* TaskEngine.setMockTime(3_000_000_000_000);
    for (let index = 0; index < batch; index++) {
      yield* engine.createTask({
        prefix: stressQueue,
        id: String(index),
        name: "soak-max-batch",
        payload: null,
        delay: 1_000,
        maxRetries: 0,
        onSuccessPolicy: "delete",
        onFailurePolicy: "delete",
      });
    }
    yield* TaskEngine.stepMockTime(1_001);
    const sweepStarted = performance.now();
    const maxBatchHealth = yield* engine.maintain(stressQueue);
    const maxBatchLatencyMs = performance.now() - sweepStarted;
    if (maxBatchHealth.processed !== batch) {
      return yield* Effect.die(
        `maximum batch processed ${maxBatchHealth.processed}, expected ${batch}`,
      );
    }

    const memoryAfter = yield* redis.send<string>("INFO", "memory");
    const health = yield* connectionHealth.snapshot;
    for (const role of Object.values(health.roles)) {
      if (role.commandErrors > 0) {
        return yield* Effect.die("Redis command errors occurred during soak");
      }
    }
    const processMemoryAfter = memorySnapshot();
    const redisGrowth =
      infoNumber(memoryAfter, "used_memory") -
      infoNumber(memoryBefore, "used_memory");
    const heapGrowth =
      processMemoryAfter.heapUsed - processMemoryBefore.heapUsed;
    if (redisGrowth > 32 * 1_024 * 1_024) {
      return yield* Effect.die(`Redis memory grew by ${redisGrowth} bytes`);
    }
    if (heapGrowth > 64 * 1_024 * 1_024) {
      return yield* Effect.die(
        `Node heap grew by ${heapGrowth} bytes after GC`,
      );
    }
    return {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      parameters: {
        durationMs,
        concurrency,
        payloadBytes,
        warmupTasks,
        maxBatch: batch,
      },
      workload: {
        offered,
        completed: completedCount,
        tasksPerSecond: (offered * 1_000) / (Date.now() - startedAt),
        offerLatencyMs: summary(offerSamples),
        redisPingLatencyMs: summary(pingSamples),
      },
      memory: {
        redisBeforeBytes: infoNumber(memoryBefore, "used_memory"),
        redisAfterBytes: infoNumber(memoryAfter, "used_memory"),
        redisGrowthBytes: redisGrowth,
        processHeapUsedBefore: processMemoryBefore.heapUsed,
        processHeapUsedAfter: processMemoryAfter.heapUsed,
        processHeapGrowthBytes: heapGrowth,
        processRssBefore: processMemoryBefore.rss,
        processRssAfter: processMemoryAfter.rss,
      },
      maxBatch: {
        processed: maxBatchHealth.processed,
        latencyMs: maxBatchLatencyMs,
        remainingDue: maxBatchHealth.dueBacklog,
      },
      gracefulShutdownMs,
      redisHealth: health,
    };
  }),
);

const layer = TaskEngine.layer({
  engine: {
    debugMode: true,
    maintenanceBatchSize: TaskEngine.maxMaintenanceBatchSize,
  },
  redis: {
    url: redisUrl,
    commandOptions: { timeout: 2_000 },
    pool: { maximum: Math.max(16, concurrency), minimum: 1 },
  },
});

console.log(
  JSON.stringify(
    await Effect.runPromise(run.pipe(Effect.provide(layer))),
    null,
    2,
  ),
);
