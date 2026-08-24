/**
 * The standard Node.js live graph: node-redis, NodeCrypto, and the task
 * engine.
 *
 * @module
 */

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import type * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import type * as Redis from "effect/unstable/persistence/Redis";
import * as NodeRedisPool from "./NodeRedisPool.js";
import type { RedisConnectionRoles, RedisPool } from "./RedisPool.js";
import * as TaskEngine from "./TaskEngine.js";

/**
 * Configuration for the standard Node.js live service graph.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface LiveConfig {
  readonly engine?: TaskEngine.TaskEngineConfig;
  readonly redis?: NodeRedisPool.RedisConfig;
}

/**
 * Provides a complete Node.js live graph: Redis connections, connection
 * roles and health, Crypto, and the task engine.
 *
 * **Example: Run a program on Node**
 *
 * ```ts
 * import { Effect } from "effect"
 * import { NodeRuntime } from "@effect/platform-node"
 * import { NodeLive } from "@effectmq/core"
 *
 * const program = Effect.void
 * program.pipe(
 *   Effect.provide(NodeLive.layer({ redis: { url: "redis://127.0.0.1:6379" } })),
 *   NodeRuntime.runMain,
 * )
 * ```
 *
 * @category Layers
 * @since 0.3.0
 */
export const layer = (
  config: LiveConfig = {},
): Layer.Layer<
  | TaskEngine.TaskEngine
  | RedisPool
  | RedisConnectionRoles
  | NodeRedisPool.RedisConnectionHealth
  | Redis.Redis
  | Crypto.Crypto,
  | TaskEngine.TaskEngineConfigurationError
  | Redis.RedisError
  | NodeRedisPool.UnsupportedRedisTopology
  | NodeRedisPool.InvalidRedisConfiguration
> =>
  TaskEngine.layer(config.engine).pipe(
    Layer.provideMerge(
      Layer.merge(NodeRedisPool.layer(config.redis), NodeCrypto.layer),
    ),
  );
