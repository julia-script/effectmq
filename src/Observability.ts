/** Effect metrics emitted by EffectMQ's Redis and queue runtime. @module */
import { Effect, Metric } from "effect";

/**
 * Gauge of runnable, delayed, and leased tasks, attributed by queue.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const queueDepth = Metric.gauge("effectmq_queue_depth", {
  description: "Runnable, delayed, and leased tasks in a queue",
});
/**
 * Gauge of the oldest retained task record's age in milliseconds.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const oldestTaskAgeMs = Metric.gauge("effectmq_oldest_task_age_ms", {
  description: "Age of the oldest retained task record",
});
/**
 * Gauge of the oldest due maintenance item's age in milliseconds.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const maintenanceSweepLagMs = Metric.gauge(
  "effectmq_maintenance_sweep_lag_ms",
  { description: "Age of the oldest due maintenance item" },
);
/**
 * Gauge of delayed tasks currently due for promotion.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const dueBacklog = Metric.gauge("effectmq_due_backlog", {
  description: "Delayed tasks currently due for promotion",
});
/**
 * Gauge of expired leases awaiting a maintenance recovery pass.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const expiredLeaseBacklog = Metric.gauge(
  "effectmq_expired_lease_backlog",
  { description: "Expired leases still awaiting recovery" },
);
/**
 * Gauge of due retention expirations across queue-owned resources.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const retentionBacklog = Metric.gauge("effectmq_retention_backlog", {
  description: "Due task, result, terminal-index, and dead-letter expirations",
});

/**
 * Counter of Redis command and connection errors observed by EffectMQ.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const redisErrors = Metric.counter("effectmq_redis_errors_total", {
  incremental: true,
});
/**
 * Counter of Lua scripts reloaded after Redis reports `NOSCRIPT`.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const scriptReloads = Metric.counter("effectmq_script_reloads_total", {
  incremental: true,
});
/**
 * Counter of reconnect attempts and Sentinel topology changes.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const redisReconnects = Metric.counter(
  "effectmq_redis_reconnects_total",
  { incremental: true },
);
/**
 * Counter of task attempts that lose their lease ownership.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const ownershipLosses = Metric.counter(
  "effectmq_ownership_losses_total",
  { incremental: true },
);
/**
 * Counter of maintenance sweeps that fail before reporting queue health.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const retentionFailures = Metric.counter(
  "effectmq_retention_failures_total",
  { incremental: true },
);

/**
 * Queue health values returned by one bounded maintenance invocation.
 *
 * @category Models
 * @since 0.3.0
 */
export interface QueueHealth {
  readonly depth: number;
  readonly oldestTaskAgeMs: number;
  readonly sweepLagMs: number;
  readonly dueBacklog: number;
  readonly expiredLeaseBacklog: number;
  readonly retentionBacklog: number;
  /** Maintenance records processed by this atomic invocation. */
  readonly processed: number;
}

const forQueue = <Input, State>(
  metric: Metric.Metric<Input, State>,
  queue: string,
) => Metric.withAttributes(metric, { queue });

/**
 * Records a maintenance health snapshot on queue-attributed gauges.
 *
 * @category Metrics
 * @since 0.3.0
 */
export const recordQueueHealth = (queue: string, health: QueueHealth) =>
  Effect.all([
    Metric.update(forQueue(queueDepth, queue), health.depth),
    Metric.update(forQueue(oldestTaskAgeMs, queue), health.oldestTaskAgeMs),
    Metric.update(forQueue(maintenanceSweepLagMs, queue), health.sweepLagMs),
    Metric.update(forQueue(dueBacklog, queue), health.dueBacklog),
    Metric.update(
      forQueue(expiredLeaseBacklog, queue),
      health.expiredLeaseBacklog,
    ),
    Metric.update(forQueue(retentionBacklog, queue), health.retentionBacklog),
  ]).pipe(Effect.asVoid);
