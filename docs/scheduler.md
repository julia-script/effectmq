# Durable scheduling

`Scheduler` is a durable tick materializer, not a handler runner. A definition
combines a stable name, Effect `Cron`, target `TaskQueue`, payload constructor,
and missed-tick policy. For each selected nominal time it offers an ordinary
task whose id is `<schedule-name>/<ISO tick time>`.

Because task identity is deterministic, competing scheduler processes and a
crash after offer can safely repeat materialization. The scheduler advances its
cursor only after the offer. A normal `Worker` executes the task with the
queue's leases, retries, failure policy, and at-least-once delivery.

```ts
import { Scheduler, Task, TaskQueue } from "@effectmq/core"
import { Cron, Schema } from "effect"

const reportTask = Task.make({
  name: "nightly-report-task",
  payload: { scheduledAt: Schema.String },
  success: Schema.Void,
  error: Schema.Never
})
const reports = TaskQueue.make("nightly-reports", reportTask)

const schedule = Scheduler.make({
  name: "nightly-report",
  cron: Cron.parseUnsafe("0 2 * * *", "America/Sao_Paulo"),
  queue: reports,
  payload: (tick) => ({ scheduledAt: tick.scheduledAt.toISOString() }),
  missed: { _tag: "backfill", maxBackfill: 7 }
})
```

Construction is pure. An invalid `maxBackfill` is reported as a defect when
the scheduler first materializes work, before it reads or writes Redis state.

Use an explicit IANA time zone. Nominal tick identity includes the resulting
instant, so daylight-saving gaps and overlaps follow Effect Cron's time-zone
rules. Test the boundaries used by the deployment.

Missed policies are bounded:

- `skip` advances past downtime without creating old work;
- `coalesce` creates one task for the most recent due tick and includes the
  missed interval in its `Tick` metadata;
- `backfill` creates the most recent `maxBackfill` due ticks in order.

Run at least two scheduler instances when tick materialization must tolerate a
process loss. A scheduler outage does not lose the Redis cursor; on restart the
configured missed policy decides what to materialize. Scheduler availability
does not imply worker availability, and the reverse is also true. Monitor both
cursor lag and target queue age.

When `materializeDue` is called without `now`, it reads Effect's `Clock` when
the Effect executes. Tests may pass an explicit instant; construction time and
ambient `Date.now` do not influence materialization.
