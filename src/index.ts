/**
 * `@effectmq/core` — an Effect-based, Redis-backed message queue.
 *
 * Public entry point. Most applications use {@link TaskQueue} (typed enqueue /
 * process) and {@link Scheduler} (cron-driven work); {@link TaskEngine} is the
 * lower-level service they build on, and {@link Task} defines task types.
 *
 * @module
 */

/**
 * Scoped node-redis adapters for standalone Redis and Sentinel.
 *
 * @category Modules
 * @since 0.2.0
 */
export * as NodeRedisPool from "./NodeRedisPool.js";
/**
 * Effect metrics emitted by Redis and queue operations.
 *
 * @category Modules
 * @since 0.3.0
 */
export * as Observability from "./Observability.js";
/** Public schemas and codecs for typed task records. */
export * as TaskRecord from "./TaskRecord.js";
/** Public schemas for queue lifecycle events. */
export * as TaskEvent from "./TaskEvent.js";
/**
 * Minimal Redis command, script-cache, and workload-role services.
 *
 * @category Modules
 * @since 0.2.0
 */
export * as RedisPool from "./RedisPool.js";
/**
 * Durable cron tick materialization into ordinary queue tasks.
 *
 * @category Modules
 * @since 0.1.0
 */
export * as Scheduler from "./Scheduler.js";
/**
 * Versioned, lossless storage envelopes and storage limits.
 *
 * @category Modules
 * @since 0.3.0
 */
export * as StorageProtocol from "./StorageProtocol.js";
/**
 * The schema-bearing definition of one task family.
 *
 * @category Models
 * @since 0.1.0
 */
export type { TaskDefinition } from "./Task.js";
/**
 * Typed task definitions and retention policies.
 *
 * @category Modules
 * @since 0.1.0
 */
export * as Task from "./Task.js";
/**
 * Low-level atomic queue, lease, schedule, and event operations.
 *
 * @category Modules
 * @since 0.1.0
 */
export * as TaskEngine from "./TaskEngine.js";
/**
 * A typed queue task handler.
 *
 * @category Models
 * @since 0.1.0
 */
export type { TaskHandler } from "./TaskQueue.js";
/**
 * High-level typed queue operations and generation-safe handles.
 *
 * @category Modules
 * @since 0.1.0
 */
export * as TaskQueue from "./TaskQueue.js";
/**
 * Managed queue workers with bounded concurrency and graceful draining.
 *
 * @category Modules
 * @since 0.3.0
 */
export * as Worker from "./Worker.js";
