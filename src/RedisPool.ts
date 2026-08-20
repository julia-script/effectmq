/**
 * The Redis service `TaskEngine` depends on: a minimal command and cached
 * script surface over a (possibly pooled) Redis connection. Provide it with
 * `NodeRedisPool` or any custom implementation.
 *
 * @module
 */
import { Context, Effect, Metric, Ref } from "effect";
import type * as Redis from "effect/unstable/persistence/Redis";
import * as Observability from "./Observability.js";

/**
 * A text or binary argument accepted by the EffectMQ Redis boundary.
 *
 * @category Models
 * @since 0.3.0
 */
export type RedisArgument = string | Uint8Array;

/**
 * Sends one Redis command and preserves failures as Effect Redis errors.
 *
 * @category Models
 * @since 0.3.0
 */
export type RedisSend = <A = unknown>(
  command: string,
  ...args: ReadonlyArray<RedisArgument>
) => Effect.Effect<A, Redis.RedisError>;

/**
 * Controls key partitioning and reply decoding for a Lua script invocation.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface RedisScriptOptions {
  /** Number of leading script arguments Redis should expose through `KEYS`. */
  readonly numberOfKeys?: number;
  /** Preserve bulk-string replies as bytes instead of decoding them as utf8. */
  readonly binaryReply?: boolean;
}

/**
 * The minimal Redis command and cached-script surface used by EffectMQ.
 *
 * **Details**
 *
 * `evalScript` loads exact Lua source with `SCRIPT LOAD`, caches its digest,
 * invokes it with `EVALSHA`, and reloads once after a `NOSCRIPT` response.
 * Binary arguments are supported by every operation; `sendBinary` and the
 * `binaryReply` option additionally preserve binary replies.
 *
 * @category Services
 * @since 0.3.0
 */
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

/**
 * Effect service tag for the producer-facing Redis command pool.
 *
 * Provide it with `NodeRedisPool.layer` or a custom service built by
 * {@link make}.
 *
 * @category Services
 * @since 0.2.0
 */
export class RedisPool extends Context.Service<RedisPool, RedisPoolService>()(
  "effectmq/RedisPool",
) {}

/**
 * Physically isolated Redis command services for each queue workload.
 *
 * Producer traffic cannot consume the connections reserved for worker
 * acquisition or maintenance sweeps.
 *
 * @category Services
 * @since 0.3.0
 */
export interface RedisConnectionRolesService {
  readonly producer: RedisPoolService;
  readonly worker: RedisPoolService;
  readonly maintenance: RedisPoolService;
}

/**
 * Effect service tag for producer, worker, and maintenance Redis roles.
 *
 * @category Services
 * @since 0.3.0
 */
export class RedisConnectionRoles extends Context.Service<
  RedisConnectionRoles,
  RedisConnectionRolesService
>()("effectmq/RedisConnectionRoles") {}

/**
 * Creates role routing from three independently managed Redis services.
 *
 * @category Constructors
 * @since 0.3.0
 */
export const makeConnectionRoles = (
  producer: RedisPoolService,
  worker: RedisPoolService,
  maintenance: RedisPoolService,
) => RedisConnectionRoles.of({ producer, worker, maintenance });

const isNoScript = (error: Redis.RedisError) =>
  String(error.cause).includes("NOSCRIPT");

/**
 * Creates a {@link RedisPool} service from text and binary command senders.
 *
 * **When to use**
 *
 * Use this constructor when integrating a Redis client other than the bundled
 * node-redis adapter. Most Node.js applications can provide
 * `NodeRedisPool.layer` directly.
 *
 * **Gotchas**
 *
 * The two senders must share the same Redis server and command semantics. The
 * binary sender must preserve bulk-string replies as `Uint8Array`-compatible
 * values.
 *
 * @category Constructors
 * @since 0.3.0
 */
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
