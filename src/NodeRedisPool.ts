/**
 * Connection-pooled Redis layer backed by `node-redis`'s `createClientPool`.
 *
 * Provides the generic `Redis` service used by `TaskEngine`. The pool connects
 * lazily on first command and is closed when the layer scope ends.
 *
 * @module
 */
import { Effect, Layer, Scope } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import { createClientPool, type RedisClientOptions } from "redis";

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

  return yield* Redis.make({
    send: <A = unknown>(command: string, ...args: ReadonlyArray<string>) =>
      Effect.gen(function* () {
        yield* connect;
        return yield* Effect.tryPromise({
          try: () => client.sendCommand([command, ...args], {}) as Promise<A>,
          catch: (cause) => new Redis.RedisError({ cause }),
        });
      }),
  });
});

export const layer = (options?: RedisPoolOptions): Layer.Layer<Redis.Redis> =>
  Layer.effect(Redis.Redis, make(options));
