# Upgrade and rollback

Every deployment must remain within the declared storage compatibility window.
The current release reads and writes protocol v1; the only rolling pair is a v1
reader with a v1 writer. Run the read-only inspector before the first v1 write:

```sh
EFFECTMQ_REDIS_URL=redis://redis.internal:6379 \
  pnpm storage:inspect -- --assert-drained
```

## Rolling upgrade

1. Back up Redis and complete a restore drill.
2. Confirm `noeviction`, persistence, replication health, capacity headroom,
   compatible Node/Redis/Effect versions, and zero pre-v1 keys.
3. Deploy producers first. Retry any indeterminate offers with the same id.
4. Deploy workers and wait for old attempts either to settle or expire under
   their original tokens. Do not reuse process-level lease identities.
5. Deploy schedulers last so new scheduled payloads are not materialized before
   compatible workers are ready.
6. Observe error/reconnect/reload/ownership metrics, queue age, maintenance lag,
   event cursors, and result decoding through at least the longest normal task.

Lua scripts are content addressed. Mixed application versions can each load
their own script digest; startup never replaces a global Redis function.

## Rollback

Rollback is permitted only while the previous version can read every value the
new version may have written. Stop schedulers first, then producers, drain or
interrupt workers, and deploy the previous version in the reverse order. Keep
Redis data: do not flush the database to make a rollback “clean.”

If the new release wrote an unsupported protocol, a rolling rollback is unsafe.
Stop all application traffic, restore the pre-upgrade backup or run the
documented forward-compatible migration, inspect the isolated result, and only
then restart. Reconcile accepted upstream requests against task identities
because restoring Redis can remove acknowledged recent offers.

The release rehearsal must record exact commits, package integrity, backup and
restore points, start/stop order, queue/result samples, duration, and whether
any write was indeterminate. See the [operations runbook](./operations.md).
