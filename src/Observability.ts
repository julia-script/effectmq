/** Effect metrics emitted by EffectMQ's Redis and queue runtime. @module */
import { Effect, Metric } from "effect";

export const queueDepth = Metric.gauge("effectmq_queue_depth", {
  description: "Runnable, delayed, and leased tasks in a queue",
});
export const oldestTaskAgeMs = Metric.gauge("effectmq_oldest_task_age_ms", {
  description: "Age of the oldest retained task record",
});
export const maintenanceSweepLagMs = Metric.gauge(
  "effectmq_maintenance_sweep_lag_ms",
  { description: "Age of the oldest due maintenance item" },
);
export const dueBacklog = Metric.gauge("effectmq_due_backlog", {
  description: "Delayed tasks currently due for promotion",
});
export const expiredLeaseBacklog = Metric.gauge(
  "effectmq_expired_lease_backlog",
  { description: "Expired leases still awaiting recovery" },
);
export const retentionBacklog = Metric.gauge("effectmq_retention_backlog", {
  description: "Due task, result, terminal-index, and dead-letter expirations",
});

export const redisErrors = Metric.counter("effectmq_redis_errors_total", {
  incremental: true,
});
export const scriptReloads = Metric.counter("effectmq_script_reloads_total", {
  incremental: true,
});
export const redisReconnects = Metric.counter(
  "effectmq_redis_reconnects_total",
  { incremental: true },
);
export const ownershipLosses = Metric.counter(
  "effectmq_ownership_losses_total",
  { incremental: true },
);
export const retentionFailures = Metric.counter(
  "effectmq_retention_failures_total",
  { incremental: true },
);

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
