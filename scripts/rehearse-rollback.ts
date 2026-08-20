import { Effect } from "effect";
import { NodeRedisPool, RedisPool, TaskEngine } from "../src/index.js";

const redisUrl = process.env.EFFECTMQ_REDIS_URL ?? "redis://127.0.0.1:6379";
const runId = crypto.randomUUID();
const enginePrefix = `~effectmq:v1:rollback-${runId}`;
const queue = "candidate";
const baselineKey = `effectmq:rollback:baseline:${runId}`;

const program = Effect.gen(function* () {
  const redis = yield* RedisPool.RedisPool;
  const engine = yield* TaskEngine.makeWithRedis(redis, {
    prefix: enginePrefix,
  });
  yield* redis.send("SET", baselineKey, "pre-upgrade");
  const baselineDump = yield* redis.sendBinary<Uint8Array>("DUMP", baselineKey);
  if (baselineDump === null) return yield* Effect.die("baseline backup failed");

  yield* engine.createTask({
    prefix: queue,
    id: "candidate-task",
    name: "rollback-rehearsal",
    payload: { release: "candidate" },
    delay: 0,
    maxRetries: 0,
    onSuccessPolicy: "keep",
    onFailurePolicy: "keep",
  });
  const attempt = yield* engine.takeTask(queue, 30_000);
  if (attempt === null)
    return yield* Effect.die("candidate task was not acquired");
  yield* engine.writeSuccess(
    queue,
    attempt.task.id,
    attempt.leaseToken,
    "candidate-result",
  );
  const candidateKeys = yield* redis.send<ReadonlyArray<string>>(
    "KEYS",
    `${enginePrefix}:*`,
  );
  if (candidateKeys.length === 0) {
    return yield* Effect.die("candidate created no versioned keys");
  }
  yield* redis.send("SET", baselineKey, "post-upgrade-change");

  const rollbackStarted = performance.now();
  for (const key of candidateKeys) yield* redis.send("DEL", key);
  yield* redis.send("RESTORE", baselineKey, "0", baselineDump, "REPLACE");
  const rollbackMs = performance.now() - rollbackStarted;
  const restored = yield* redis.send<string>("GET", baselineKey);
  const remainingCandidateKeys = yield* redis.send<ReadonlyArray<string>>(
    "KEYS",
    `${enginePrefix}:*`,
  );
  if (restored !== "pre-upgrade" || remainingCandidateKeys.length !== 0) {
    return yield* Effect.die("rollback verification failed");
  }
  yield* redis.send("DEL", baselineKey);
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    candidateNamespace: enginePrefix,
    candidateKeysRemoved: candidateKeys.length,
    baselineRestored: restored,
    remainingCandidateKeys: remainingCandidateKeys.length,
    rollbackMs,
  };
});

console.log(
  JSON.stringify(
    await Effect.runPromise(
      program.pipe(Effect.provide(NodeRedisPool.layer({ url: redisUrl }))),
    ),
    null,
    2,
  ),
);
