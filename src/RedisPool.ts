/**
 * The Redis service `TaskEngine` depends on: a minimal `send`/`eval` surface
 * over a (possibly pooled) Redis connection. Provide it with
 * {@link NodeRedisPool} or any custom implementation.
 *
 * @module
 */
import { Context, type Effect } from "effect";
import type * as Redis from "effect/unstable/persistence/Redis";

export class RedisPool extends Context.Service<
  RedisPool,
  {
    readonly send: <A = unknown>(
      command: string,
      ...args: ReadonlyArray<string>
    ) => Effect.Effect<A, Redis.RedisError>;

    readonly eval: <
      Config extends {
        readonly params: ReadonlyArray<unknown>;
        readonly result: unknown;
      },
    >(
      script: Redis.Script<Config>,
    ) => (
      ...params: Config["params"]
    ) => Effect.Effect<Config["result"], Redis.RedisError>;
  }
>()("effectmq/RedisPool") {}
