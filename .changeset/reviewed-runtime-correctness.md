---
"@effectmq/core": patch
---

Fix queue correctness and boundary validation found during a repository-wide
review:

- allow `Schema.Void` tasks to persist and recover successful completion;
- keep `wait` and `execute` subscribed across retryable failures;
- reject queue/task descriptors that do not match a persisted handle;
- validate numeric offer, lease, and retry-timestamp inputs before mutating
  Redis;
- validate decoded storage values, preserve prototype-sensitive object keys,
  and reject corrupt Redis numbers and cursors in typed error channels;
- schema-validate built-in failure events and keep their public type precise;
- keep stalled-attempt history out of handler retry schedules, settle attempts
  when retry-policy evaluation fails, stop safely when retained history is too
  short to replay, and retain terminal failures even when error history is
  disabled;
- preserve retry-policy interruption for lease recovery and avoid full
  wait-list scans on creation, acquisition, and non-waiting transitions; and
- reject unsafe worker concurrency, timing, and lease supervision options
  before acquiring work or starting fibers.
