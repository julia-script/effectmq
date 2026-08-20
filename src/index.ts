/**
 * `@effectmq/core` — an Effect-based, Redis-backed message queue.
 *
 * Public entry point. Most applications use {@link TaskQueue} (typed enqueue /
 * process) and {@link Scheduler} (cron-driven work); {@link TaskEngine} is the
 * lower-level service they build on, and {@link Task} defines task types.
 *
 * @module
 */

export * as NodeRedisPool from "./NodeRedisPool.js";
export * as Observability from "./Observability.js";
export * as RedisPool from "./RedisPool.js";
export * as Scheduler from "./Scheduler.js";
export * as StorageProtocol from "./StorageProtocol.js";
export type { TaskDefinition } from "./Task.js";
export * as Task from "./Task.js";
export * as TaskEngine from "./TaskEngine.js";
export type { TaskHandler } from "./TaskQueue.js";
export * as TaskQueue from "./TaskQueue.js";
export * as Worker from "./Worker.js";
