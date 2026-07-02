/**
 * Connection-pooled Redis integration backed by `node-redis`'s
 * `createClientPool`.
 *
 * Provides the {@link RedisPool} service `TaskEngine` requires (plus the
 * generic `Redis` service for interop). The pool connects lazily on first
 * command and is closed when the layer scope ends.
 *
 * @module
 */
import { Context, Effect, Layer, Scope } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import { createClientPool, type RedisClientOptions } from "redis";
import { RedisPool } from "./RedisPool.js";

export type RedisPoolOptions = Omit<RedisClientOptions, "clientSideCache">;

const make = Effect.fnUntraced(function* (options?: RedisPoolOptions) {
  const client = createClientPool(options);

  const scope = yield* Effect.scope;
  yield* Scope.addFinalizer(
    scope,
    Effect.promise(() => client.close()),
  );

  const connect = yield* Effect.tryPromise({
    try: async () => await client.connect(),
    catch: (cause) => new Redis.RedisError({ cause }),
  }).pipe(Effect.cached);

  const send = <A = unknown>(command: string, ...args: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      yield* connect;
      return yield* Effect.tryPromise({
        try: () => client.sendCommand([command, ...args], {}) as Promise<A>,
        catch: (cause) => new Redis.RedisError({ cause }),
      });
    });

  const redis = yield* Redis.make({ send });
  return Context.make(RedisPool, RedisPool.of({ send, eval: redis.eval })).pipe(
    Context.add(Redis.Redis, redis),
  );
});

export const layer = (
  options?: RedisPoolOptions,
): Layer.Layer<RedisPool | Redis.Redis> => Layer.effectContext(make(options));
