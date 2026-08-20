/** Test-only compatibility helpers while engine tests migrate to TaskAttempt. */
import { Effect } from "effect";
import type { TaskEngineService } from "../TaskEngine.js";

const tokensByEngine = new WeakMap<TaskEngineService, Map<string, string>>();

const keyOf = (prefix: string, id: string) => `${prefix}\0${id}`;

const tokensFor = (engine: TaskEngineService) => {
  const existing = tokensByEngine.get(engine);
  if (existing) return existing;
  const tokens = new Map<string, string>();
  tokensByEngine.set(engine, tokens);
  return tokens;
};

export const leaseToken = (
  engine: TaskEngineService,
  prefix: string,
  id: string,
) => {
  const token = tokensFor(engine).get(keyOf(prefix, id));
  if (token === undefined) {
    throw new Error(`No acquired attempt for ${prefix}/${id}`);
  }
  return token;
};

export const takeTask = Effect.fnUntraced(function* (
  engine: TaskEngineService,
  prefix: string,
  lockTimeout: number,
) {
  const attempt = yield* engine.takeTask(prefix, lockTimeout);
  if (attempt) {
    tokensFor(engine).set(keyOf(prefix, attempt.task.id), attempt.leaseToken);
  }
  return attempt?.task ?? null;
});

export const writeSuccess = (
  engine: TaskEngineService,
  prefix: string,
  id: string,
  result: unknown,
) => engine.writeSuccess(prefix, id, leaseToken(engine, prefix, id), result);

export const writeError = (
  engine: TaskEngineService,
  prefix: string,
  id: string,
  error: unknown,
  retryAt?: Parameters<TaskEngineService["writeError"]>[4],
) =>
  engine.writeError(prefix, id, leaseToken(engine, prefix, id), error, retryAt);

export const extendLock = (
  engine: TaskEngineService,
  prefix: string,
  id: string,
  lockTimeout: number,
) => engine.extendLock(prefix, id, leaseToken(engine, prefix, id), lockTimeout);

export const removeLock = (
  engine: TaskEngineService,
  prefix: string,
  id: string,
) => engine.removeLock(prefix, id, leaseToken(engine, prefix, id));
