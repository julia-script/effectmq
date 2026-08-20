#!/usr/bin/env node
/** Read-only release gate for pre-v1 EffectMQ keys. */
import process from "node:process";
import { createClient } from "redis";

const url = process.env.EFFECTMQ_REDIS_URL ?? "redis://127.0.0.1:6379";
const assertDrained = process.argv.includes("--assert-drained");
const client = createClient({ url });

client.on("error", (error) => {
  console.error(
    "Redis connection error:",
    error instanceof Error ? error.message : error,
  );
});

await client.connect();
try {
  const legacyKeys: string[] = [];
  for await (const keys of client.scanIterator({
    MATCH: "~effectmq:*",
    COUNT: 500,
  })) {
    for (const key of keys) {
      if (!key.startsWith("~effectmq:v1:")) legacyKeys.push(key);
    }
  }
  legacyKeys.sort();

  console.log(
    JSON.stringify(
      {
        status: legacyKeys.length === 0 ? "drained" : "pre-v1-data-found",
        legacyKeyCount: legacyKeys.length,
        keys: legacyKeys,
      },
      null,
      2,
    ),
  );

  if (assertDrained && legacyKeys.length > 0) process.exitCode = 1;
} finally {
  await client.close();
}
