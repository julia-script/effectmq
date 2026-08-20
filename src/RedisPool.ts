/**
 * The Redis service `TaskEngine` depends on: a minimal command and cached
 * script surface over a (possibly pooled) Redis connection. Provide it with
 * {@link NodeRedisPool} or any custom implementation.
 *
 * @module
 */
import { Context, Effect, Metric, Ref } from "effect";
import type * as Redis from "effect/unstable/persistence/Redis";
import * as Observability from "./Observability.js";

export type RedisArgument = string | Uint8Array;

export type RedisSend = <A = unknown>(
  command: string,
  ...args: ReadonlyArray<RedisArgument>
) => Effect.Effect<A, Redis.RedisError>;

export interface RedisScriptOptions {
  /** Number of leading script arguments Redis should expose through `KEYS`. */
  readonly numberOfKeys?: number;
  /** Preserve bulk-string replies as bytes instead of decoding them as utf8. */
  readonly binaryReply?: boolean;
}

export interface RedisPoolService {
  readonly send: RedisSend;
  readonly sendBinary: RedisSend;
  /**
   * Run exact Lua source through Redis' content-addressed script cache.
   *
   * The source is loaded lazily with `SCRIPT LOAD`, invoked with `EVALSHA`,
   * and reloaded once when Redis reports `NOSCRIPT`. Arguments and optional
   * replies remain binary-safe.
   */
  readonly evalScript: <A = unknown>(
    source: string,
    options: RedisScriptOptions,
    ...args: ReadonlyArray<RedisArgument>
  ) => Effect.Effect<A, Redis.RedisError>;
}

export class RedisPool extends Context.Service<RedisPool, RedisPoolService>()(
  "effectmq/RedisPool",
) {}

/** Physically isolated command pools for producer, worker, and maintenance work. */
export interface RedisConnectionRolesService {
  readonly producer: RedisPoolService;
  readonly worker: RedisPoolService;
  readonly maintenance: RedisPoolService;
}

export class RedisConnectionRoles extends Context.Service<
  RedisConnectionRoles,
  RedisConnectionRolesService
>()("effectmq/RedisConnectionRoles") {}

/** Build role routing, typically with three independently managed pools. */
export const makeConnectionRoles = (
  producer: RedisPoolService,
  worker: RedisPoolService,
  maintenance: RedisPoolService,
) => RedisConnectionRoles.of({ producer, worker, maintenance });

const isNoScript = (error: Redis.RedisError) =>
  String(error.cause).includes("NOSCRIPT");

/** Build a RedisPool service from text and binary command senders. */
export const make = Effect.fnUntraced(function* (
  send: RedisSend,
  sendBinary: RedisSend,
) {
  const scriptDigests = yield* Ref.make(new Map<string, string>());
  const observe =
    (transport: RedisSend): RedisSend =>
    <A>(command: string, ...args: ReadonlyArray<RedisArgument>) =>
      transport<A>(command, ...args).pipe(
        Effect.tapError((error) =>
          Effect.all([
            Metric.update(Observability.redisErrors, 1),
            Effect.logError("effectmq Redis command failed", {
              command,
              error,
            }),
          ]).pipe(Effect.asVoid),
        ),
      );
  const observedSend = observe(send);
  const observedSendBinary = observe(sendBinary);

  const load = (source: string, force: boolean) =>
    Effect.gen(function* () {
      const cached = (yield* Ref.get(scriptDigests)).get(source);
      if (cached !== undefined && !force) return cached;

      const digest = yield* observedSend<string>("SCRIPT", "LOAD", source);
      yield* Ref.update(scriptDigests, (current) => {
        const next = new Map(current);
        next.set(source, digest);
        return next;
      });
      return digest;
    });

  const evalScript: RedisPoolService["evalScript"] = <A>(
    source: string,
    options: RedisScriptOptions,
    ...args: ReadonlyArray<RedisArgument>
  ) => {
    const invoke = (digest: string) =>
      (options.binaryReply ? observedSendBinary : observedSend)<A>(
        "EVALSHA",
        digest,
        String(options.numberOfKeys ?? 0),
        ...args,
      );

    return load(source, false).pipe(
      Effect.flatMap(invoke),
      Effect.catchIf(isNoScript, () =>
        Effect.all([
          Metric.update(Observability.scriptReloads, 1),
          Effect.logInfo("effectmq Redis script cache miss; reloading"),
        ]).pipe(
          Effect.flatMap(() => load(source, true)),
          Effect.flatMap(invoke),
        ),
      ),
    );
  };

  return RedisPool.of({
    send: observedSend,
    sendBinary: observedSendBinary,
    evalScript,
  });
});
