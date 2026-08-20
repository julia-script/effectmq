/**
 * The low-level task engine: a Redis-backed service implementing the queue
 * primitives (create/take/complete/fail, locking, delayed and cron schedules,
 * and a per-queue event stream) as an atomic Lua script (see
 * `src/lua/taskEngine.lua`, loaded by content and invoked via `EVALSHA`).
 * Most consumers should use the higher-level `TaskQueue`/`Scheduler` APIs
 * rather than calling the engine directly.
 *
 * @module
 */
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import taskEngineScript from "./lua/taskEngine.js";
import * as Observability from "./Observability.js";
import { RedisPool, type RedisPoolService } from "./RedisPool.js";
import {
  type EngineTask,
  type EngineTaskInsert,
  EngineTaskSchema,
  type EngineTerminalResult,
  EngineTerminalResultSchema,
  type Event,
  EventSchema,
  UnknownFromMsgpack,
} from "./Schemas.js";

const TypeId = "~effectmq/TaskEngine" as const;

/**
 * Configures Redis key namespacing, deterministic test time, and sweep bounds.
 *
 * `maintenanceBatchSize` defaults to 100 and cannot exceed
 * {@link maxMaintenanceBatchSize}. The default key prefix is `~effectmq:v1`.
 *
 * @category Configuration
 * @since 0.3.0
 */
export type TaskEngineConfig = {
  debugMode?: boolean;
  prefix?: string;
  maintenanceBatchSize?: number;
};
/**
 * Largest supported number of records processed by one atomic maintenance call.
 *
 * @category Configuration
 * @since 0.3.0
 */
export const maxMaintenanceBatchSize = 1_000;
/**
 * Wraps a Redis, script, encoding, or decoding failure at the engine boundary.
 *
 * @category Errors
 * @since 0.1.0
 */
export class TaskEngineError extends Data.TaggedError("TaskEngineError")<{
  readonly message?: string;
  readonly cause: unknown;
}> {
  static of(message: string) {
    return (cause: unknown) => new TaskEngineError({ cause, message });
  }
}

/**
 * Reports whether an offer created a generation or returned an existing one.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskCreateResult {
  readonly status: "created" | "existing";
  readonly cursor: string;
  readonly task: EngineTask;
}

/**
 * One acquired execution attempt and its opaque ownership credential.
 *
 * The token belongs to this specific acquisition, not to the worker or task.
 * Every ownership-sensitive transition must present it unchanged.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskAttempt {
  readonly task: EngineTask;
  readonly leaseToken: string;
}

/**
 * Indicates that an attempt no longer owns the task generation it tried to mutate.
 *
 * @category Errors
 * @since 0.3.0
 */
export class LeaseLost extends Data.TaggedError("LeaseLost")<{
  readonly prefix: string;
  readonly taskId: string;
  readonly cause: TaskEngineError;
}> {}

/**
 * Redis Stream cursor bounds for one queue's retained lifecycle events.
 *
 * @category Models
 * @since 0.3.0
 */
export interface EventCursors {
  readonly first: string;
  readonly earliest: string;
  readonly latest: string;
}

/**
 * A task index that can be inspected through `TaskEngine.listTasks`.
 *
 * @category Models
 * @since 0.3.0
 */
export type TaskList = "wait" | "scheduled" | "active" | "failed" | "success";

/**
 * One bounded page of task identifiers from an engine index.
 *
 * @category Models
 * @since 0.3.0
 */
export interface TaskListPage {
  /** Task ids in FIFO order for `wait`, otherwise ascending score then id. */
  readonly items: readonly string[];
  /** Opaque cursor for the next page; absent when this page is final. */
  readonly nextCursor?: string;
}

/**
 * Indicates that event retention trimmed the stream position a reader requested.
 *
 * Resume from `earliest` when skipping the missing interval is acceptable.
 *
 * @category Errors
 * @since 0.3.0
 */
export class CursorExpired extends Data.TaggedError("CursorExpired")<{
  readonly requested: string;
  readonly earliest: string;
}> {}

const causeText = (cause: unknown, depth = 0): string => {
  if (depth >= 6) return String(cause);
  if (typeof cause !== "object" || cause === null) return String(cause);

  const parts = [String(cause)];
  if ("message" in cause) parts.push(String(cause.message));
  if ("cause" in cause) parts.push(causeText(cause.cause, depth + 1));
  return parts.join(" ");
};

const isLeaseLost = (error: TaskEngineError) =>
  causeText(error.cause).includes("LEASE_LOST");

const compareStreamIds = (left: string, right: string): number => {
  const [leftTime = "0", leftSequence = "0"] = left.split("-");
  const [rightTime = "0", rightSequence = "0"] = right.split("-");
  const timeDifference = BigInt(leftTime) - BigInt(rightTime);
  if (timeDifference !== 0n) return timeDifference < 0n ? -1 : 1;
  const sequenceDifference = BigInt(leftSequence) - BigInt(rightSequence);
  return sequenceDifference === 0n ? 0 : sequenceDifference < 0n ? -1 : 1;
};

/**
 * Low-level atomic queue, lease, retention, schedule, and event operations.
 *
 * Most applications should use `TaskQueue`, `Worker`, and `Scheduler`. Use the
 * engine directly for administration, inspection, or custom runtimes that can
 * uphold its generation and lease-token invariants.
 *
 * **Gotchas**
 *
 * A successful Redis write followed by a lost connection can be indeterminate.
 * Ownership-sensitive operations fail with {@link LeaseLost} when their exact
 * per-attempt token no longer owns the generation.
 *
 * @category Services
 * @since 0.1.0
 */
export class TaskEngine extends Context.Service<
  TaskEngine,
  {
    readonly [TypeId]: typeof TypeId;
    /** Compatibility offer that returns only the created or existing task. */
    readonly createTask: (
      task: EngineTaskInsert,
    ) => Effect.Effect<EngineTask, TaskEngineError>;
    /** Idempotently offers a task and reports whether its generation was new. */
    readonly offerTask: (
      task: EngineTaskInsert,
    ) => Effect.Effect<TaskCreateResult, TaskEngineError>;
    /** Reads a task generation, or `null` when no task record remains. */
    readonly getTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<EngineTask | null, TaskEngineError>;
    /** Reads the latest generation number for a task identity. */
    readonly getGeneration: (
      prefix: string,
      id: string,
    ) => Effect.Effect<number, TaskEngineError>;
    /** Reads a retained terminal result for an exact generation. */
    readonly getResult: (
      prefix: string,
      id: string,
      generation: number,
    ) => Effect.Effect<EngineTerminalResult | null, TaskEngineError>;
    /** Lists at most 1,000 task ids using an opaque pagination cursor. */
    readonly listTasks: (
      prefix: string,
      list: TaskList,
      options?: { readonly cursor?: string; readonly limit?: number },
    ) => Effect.Effect<TaskListPage, TaskEngineError>;
    /** Runs one bounded promotion, lease-recovery, and retention sweep. */
    readonly maintain: (
      prefix: string,
    ) => Effect.Effect<Observability.QueueHealth, TaskEngineError>;
    /** Reads the first, earliest-retained, and latest event stream cursors. */
    readonly eventCursors: (
      prefix: string,
    ) => Effect.Effect<EventCursors, TaskEngineError>;

    /** Settles an owned attempt successfully using its exact lease token. */
    readonly writeSuccess: (
      prefix: string,
      id: string,
      leaseToken: string,
      result: unknown,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Records an owned attempt failure and optionally schedules its retry. */
    readonly writeError: (
      prefix: string,
      id: string,
      leaseToken: string,
      error: unknown,
      retryAt?: Duration.Input,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Renews an owned attempt's lease for `lockTimeout` milliseconds. */
    readonly extendLock: (
      prefix: string,
      id: string,
      leaseToken: string,
      lockTimeout: number,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Voluntarily releases an owned attempt and returns it to runnable work. */
    readonly removeLock: (
      prefix: string,
      id: string,
      leaseToken: string,
    ) => Effect.Effect<void, TaskEngineError | LeaseLost>;
    /** Acquires the next runnable task with a fresh lease token. */
    readonly takeTask: (
      prefix: string,
      lockTimeout: number,
    ) => Effect.Effect<TaskAttempt | null, TaskEngineError>;
    /** Removes an unretained task generation and all queue memberships. */
    readonly removeTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;
    /** Administratively remove a generation even while results are retained. */
    readonly forceRemoveTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;

    /** Initializes a durable schedule cursor without moving an existing cursor. */
    readonly setSchedule: (
      id: string,
      next: Date,
    ) => Effect.Effect<Date, TaskEngineError>;
    /** Compare-and-advances a durable schedule cursor. */
    readonly consumeSchedule: (
      name: string,
      toConsume: Date,
      next: Date,
    ) => Effect.Effect<{ consumed: boolean; next?: Date }, TaskEngineError>;
    /**
     * Stream lifecycle events for a queue, read from its Redis Stream
     * (`<prefix>:<name>:events`) via `XREAD`. Starts from `cursor` (defaulting
     * to now) and advances the cursor past each yielded event. Events are raw
     * {@link Event}s; `TaskQueue.stream` decodes their payloads against the
     * queue's schemas.
     */
    readonly stream: (
      name: string,
      options?: {
        cursor?: string;
        pollInterval?: Duration.Duration;
      },
    ) => Stream.Stream<
      Event,
      TaskEngineError | CursorExpired | Schema.SchemaError
    >;
  }
>()("TaskEngine") {}

/**
 * The service interface represented by the {@link TaskEngine} tag.
 *
 * @category Services
 * @since 0.1.0
 */
export type TaskEngineService = TaskEngine["Service"];
// must match MOCKTIME_KEY in src/lua/taskEngine.lua
const MOCKTIME_KEY = "$$$effectmq/debug/mocktime";

/**
 * Override the engine's notion of "now" (only honored when the engine is built
 * with `debugMode`). Intended for deterministic tests of delays and schedules.
 *
 * **Gotchas**
 *
 * The mock clock is a Redis-global debug key, not a queue- or prefix-local
 * clock. Never enable `debugMode` in production.
 *
 * @category Testing
 * @since 0.1.0
 */
export const setMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* RedisPool;
    yield* redis.send("SET", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.of("Failed to set mock time")));

/**
 * Advances the Redis-global mock clock by a duration.
 *
 * Only engines built with `debugMode` read this clock. See {@link setMockTime}.
 *
 * @category Testing
 * @since 0.1.0
 */
export const stepMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* RedisPool;
    yield* redis.send("INCRBY", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.of("Failed to step mock time")));

const asText = (value: unknown): string =>
  typeof value === "string"
    ? value
    : Buffer.from(value as Uint8Array).toString("utf8");

/** Fold a flat `[k1, v1, k2, v2, ...]` reply into a record, keys as utf8. */
const entriesToRecord = (entries: ReadonlyArray<unknown>) => {
  const record: Record<string, unknown> = {};
  for (let i = 0; i < entries.length; i += 2) {
    record[asText(entries[i])] = entries[i + 1];
  }
  return record;
};

/** Decode a flat `["id", id, "name", name, ...]` raw-entry reply from Lua. */
const parseTask = (task: ReadonlyArray<unknown>) =>
  Schema.decodeUnknownEffect(EngineTaskSchema)(entriesToRecord(task)).pipe(
    Effect.mapError(TaskEngineError.of("Failed to decode task")),
  );

const parseTerminalResult = (result: ReadonlyArray<unknown>) =>
  Schema.decodeUnknownEffect(EngineTerminalResultSchema)(
    entriesToRecord(result),
  ).pipe(
    Effect.mapError(TaskEngineError.of("Failed to decode terminal result")),
  );

const packUnknown = Schema.encodeEffect(UnknownFromMsgpack);
/** Encode a structured value as msgpack bytes for a script argument. */
const pack = (value: unknown) =>
  packUnknown(value).pipe(
    Effect.mapError(TaskEngineError.of("Failed to encode value")),
  );

const decodeEvents = Schema.decodeUnknownEffect(Schema.Array(EventSchema));

/**
 * Joins Redis key namespace segments with a colon.
 *
 * Segments are not escaped; callers should avoid embedded colons when they
 * need unambiguous composition.
 *
 * @category Utilities
 * @since 0.1.0
 */
export const makePrefix = (...prefixes: string[]) => prefixes.join(":");

/**
 * Builds a {@link TaskEngine} implementation against a concrete Redis service.
 *
 * The script is loaded lazily by exact content and
 * invoked through cached `EVALSHA`, with one transparent `NOSCRIPT` recovery.
 * `debugMode` enables the mockable clock (see {@link setMockTime});
 * `prefix` namespaces all keys. Every acquisition creates a fresh opaque lease
 * token; callers must present it for every ownership-sensitive transition.
 *
 * @category Constructors
 * @since 0.3.0
 */
export const makeWithRedis = (
  redis: RedisPoolService,
  {
    debugMode = false,
    prefix = "~effectmq:v1",
    maintenanceBatchSize = 100,
  }: TaskEngineConfig = {},
) =>
  Effect.gen(function* () {
    if (
      !Number.isSafeInteger(maintenanceBatchSize) ||
      maintenanceBatchSize < 1 ||
      maintenanceBatchSize > maxMaintenanceBatchSize
    ) {
      return yield* Effect.die(
        new RangeError(
          `maintenanceBatchSize must be an integer between 1 and ${maxMaintenanceBatchSize}`,
        ),
      );
    }
    const debugFlag = debugMode ? "1" : "0";
    const withPrefix = (key: string) => `${prefix}:${key}`;

    // Every operation receives its name followed by the debug flag and its
    // own arguments. The Lua dispatcher preserves the operation-local layout.
    const call =
      <A = unknown>(name: string, message: string) =>
      (...args: ReadonlyArray<string | Uint8Array>) =>
        redis
          .evalScript<A>(
            taskEngineScript,
            {},
            name,
            debugFlag,
            String(maintenanceBatchSize),
            ...args,
          )
          .pipe(Effect.mapError(TaskEngineError.of(message)));

    // binary replies: these functions return msgpack-encoded tasks
    const callBinary =
      <A = unknown>(name: string, message: string) =>
      (...args: ReadonlyArray<string | Uint8Array>) =>
        redis
          .evalScript<A>(
            taskEngineScript,
            { binaryReply: true },
            name,
            debugFlag,
            String(maintenanceBatchSize),
            ...args,
          )
          .pipe(Effect.mapError(TaskEngineError.of(message)));

    const createTaskFn = callBinary<
      readonly [unknown, unknown, ReadonlyArray<unknown>]
    >("effectmq_createTask", "Failed to create task");
    const getTaskFn = callBinary<ReadonlyArray<unknown> | null>(
      "effectmq_getTask",
      "Failed to get task",
    );
    const getGenerationFn = call<number>(
      "effectmq_getGeneration",
      "Failed to read task generation",
    );
    const getResultFn = callBinary<ReadonlyArray<unknown> | null>(
      "effectmq_getResult",
      "Failed to get terminal result",
    );
    const takeTaskFn = callBinary<
      readonly [unknown, ReadonlyArray<unknown>] | null
    >("effectmq_takeTask", "Failed to take task");
    const writeSuccessFn = call(
      "effectmq_writeSuccess",
      "Failed to write success result",
    );
    const writeErrorFn = call(
      "effectmq_writeError",
      "Failed to write error result",
    );
    const removeTaskFn = call("effectmq_removeTask", "Failed to remove task");
    const forceRemoveTaskFn = call(
      "effectmq_forceRemoveTask",
      "Failed to force-remove task",
    );
    const extendLockFn = call("effectmq_extendLock", "Failed to extend lock");
    const removeLockFn = call("effectmq_removeLock", "Failed to remove lock");
    const setScheduleFn = call<number>(
      "effectmq_setSchedule",
      "Failed to set schedule",
    );
    const consumeScheduleFn = call<[0 | 1, number | null]>(
      "effectmq_consumeSchedule",
      "Failed to consume schedule",
    );
    const listTasksFn = call<readonly [string, ...string[]]>(
      "effectmq_listTasks",
      "Failed to list tasks",
    );
    const maintainFn = call<readonly number[]>(
      "effectmq_maintain",
      "Failed to maintain queue",
    );
    const eventCursorsFn = call<readonly [string, string, string]>(
      "effectmq_eventCursors",
      "Failed to read event cursors",
    );

    const withLeaseFence = <A>(
      operation: Effect.Effect<A, TaskEngineError>,
      queue: string,
      taskId: string,
    ) =>
      operation.pipe(
        Effect.tapError((error) =>
          isLeaseLost(error)
            ? Effect.all([
                Effect.logWarning("effectmq task ownership lost", {
                  queue,
                  taskId,
                }),
                Metric.update(
                  Metric.withAttributes(Observability.ownershipLosses, {
                    queue,
                  }),
                  1,
                ),
              ]).pipe(Effect.asVoid)
            : Effect.void,
        ),
        Effect.mapError((cause) =>
          isLeaseLost(cause)
            ? new LeaseLost({ cause, prefix: queue, taskId })
            : cause,
        ),
      );

    const offerTask = Effect.fnUntraced(function* (task: EngineTaskInsert) {
      const retentionHolder = task.retentionHolder
        ? yield* pack({
            ...task.retentionHolder,
            queue: withPrefix(task.retentionHolder.queue),
          })
        : "";
      const creator = task.creator
        ? yield* pack({
            ...task.creator,
            queue: withPrefix(task.creator.queue),
          })
        : "";
      const reply = yield* createTaskFn(
        withPrefix(task.prefix),
        "0", // throwOnExists
        task.id,
        task.name,
        yield* pack(task.payload),
        String(task.delay),
        String(task.maxRetries ?? -1),
        task.onSuccessPolicy,
        task.onFailurePolicy,
        retentionHolder,
        creator,
        task.onDuplicate ?? "return-existing",
        String(task.maxStalledCount ?? 1),
        task.schemaId ?? task.name,
        String(task.maxErrorEntries ?? 100),
        String(task.maxRelationships ?? 1000),
        String(task.maxEventEntries ?? 10_000),
        String(task.taskRecordRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
        String(task.resultRetentionMs ?? 24 * 60 * 60 * 1000),
        String(task.terminalIndexRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
        String(task.deadLetterRetentionMs ?? 30 * 24 * 60 * 60 * 1000),
        String(task.eventRetentionMs ?? 7 * 24 * 60 * 60 * 1000),
      );
      return {
        status: asText(reply[0]) as TaskCreateResult["status"],
        cursor: asText(reply[1]),
        task: yield* parseTask(reply[2]),
      } satisfies TaskCreateResult;
    });

    return TaskEngine.of({
      [TypeId]: TypeId,
      offerTask,
      createTask: (task) =>
        offerTask(task).pipe(Effect.map((result) => result.task)),

      getTask: Effect.fnUntraced(function* (prefix: string, id: string) {
        const reply = yield* getTaskFn(withPrefix(prefix), id);
        return reply ? yield* parseTask(reply) : null;
      }),
      getGeneration: (prefix, id) => getGenerationFn(withPrefix(prefix), id),
      getResult: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        generation: number,
      ) {
        const reply = yield* getResultFn(
          withPrefix(prefix),
          id,
          String(generation),
        );
        return reply ? yield* parseTerminalResult(reply) : null;
      }),
      writeSuccess: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        leaseToken: string,
        result: unknown,
      ) {
        yield* withLeaseFence(
          writeSuccessFn(
            withPrefix(prefix),
            leaseToken,
            id,
            yield* pack(result),
          ),
          prefix,
          id,
        );
      }),
      writeError: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        leaseToken: string,
        error: unknown,
        retryAt?: Duration.Input,
      ) {
        yield* withLeaseFence(
          writeErrorFn(
            withPrefix(prefix),
            leaseToken,
            id,
            yield* pack(error),
            String(retryAt ? Duration.toMillis(retryAt) : -1),
          ),
          prefix,
          id,
        );
      }),
      listTasks: Effect.fnUntraced(function* (
        prefix: string,
        list: TaskList,
        options: { readonly cursor?: string; readonly limit?: number } = {},
      ) {
        const cursor = options.cursor ?? "0";
        const offset = Number(cursor);
        const limit = options.limit ?? 100;
        if (
          !Number.isSafeInteger(offset) ||
          offset < 0 ||
          !Number.isSafeInteger(limit) ||
          limit < 1 ||
          limit > 1_000
        ) {
          return yield* new TaskEngineError({
            message: "Invalid task-list page",
            cause: new RangeError(
              "cursor must be a non-negative integer and limit must be between 1 and 1000",
            ),
          });
        }
        const [nextCursor, ...items] = yield* listTasksFn(
          withPrefix(prefix),
          list,
          cursor,
          String(limit),
        );
        return {
          items,
          nextCursor: nextCursor === "" ? undefined : nextCursor,
        };
      }),
      maintain: Effect.fnUntraced(function* (prefix: string) {
        const values = yield* maintainFn(withPrefix(prefix)).pipe(
          Effect.tapError((error) =>
            Effect.all([
              Metric.update(
                Metric.withAttributes(Observability.retentionFailures, {
                  queue: prefix,
                }),
                1,
              ),
              Effect.logError("effectmq maintenance sweep failed", {
                queue: prefix,
                error,
              }),
            ]).pipe(Effect.asVoid),
          ),
        );
        const health: Observability.QueueHealth = {
          depth: Number(values[0] ?? 0),
          oldestTaskAgeMs: Number(values[1] ?? 0),
          sweepLagMs: Number(values[2] ?? 0),
          dueBacklog: Number(values[3] ?? 0),
          expiredLeaseBacklog: Number(values[4] ?? 0),
          retentionBacklog: Number(values[5] ?? 0),
          processed: Number(values[6] ?? 0),
        };
        yield* Observability.recordQueueHealth(prefix, health);
        return health;
      }),
      eventCursors: (prefix) =>
        eventCursorsFn(withPrefix(prefix)).pipe(
          Effect.map(([first, earliest, latest]) => ({
            first: asText(first),
            earliest: asText(earliest),
            latest: asText(latest),
          })),
        ),
      takeTask: Effect.fnUntraced(function* (
        prefix: string,
        lockTimeout: number,
      ) {
        const leaseToken = `lease/${crypto.randomUUID()}`;
        const reply = yield* takeTaskFn(
          withPrefix(prefix),
          leaseToken,
          String(lockTimeout),
        );

        return reply
          ? ({
              leaseToken: asText(reply[0]),
              task: yield* parseTask(reply[1]),
            } satisfies TaskAttempt)
          : null;
      }),
      removeTask: (prefix, id) =>
        removeTaskFn(withPrefix(prefix), id).pipe(Effect.asVoid),
      forceRemoveTask: (prefix, id) =>
        forceRemoveTaskFn(withPrefix(prefix), id).pipe(Effect.asVoid),

      extendLock: (prefix, id, leaseToken, lockTimeout) =>
        withLeaseFence(
          extendLockFn(withPrefix(prefix), leaseToken, id, String(lockTimeout)),
          prefix,
          id,
        ).pipe(Effect.asVoid),
      removeLock: (prefix, id, leaseToken) =>
        withLeaseFence(
          removeLockFn(withPrefix(prefix), leaseToken, id),
          prefix,
          id,
        ).pipe(Effect.asVoid),
      setSchedule: (name, next) => {
        return setScheduleFn(prefix, name, String(next.getTime())).pipe(
          Effect.map((next) => new Date(next)),
        );
      },
      consumeSchedule: (name, toConsume, next) => {
        return consumeScheduleFn(
          prefix,
          name,
          String(toConsume.getTime()),
          String(next.getTime()),
        ).pipe(
          Effect.map(([consumed, next]) => ({
            consumed: consumed === 1,
            next: next ? new Date(next) : undefined,
          })),
        );
      },

      stream: (
        name,
        options: {
          cursor?: string;
          pollInterval?: Duration.Duration;
        } = {},
      ) => {
        const streamKey = `${withPrefix(name)}:events`;
        const blockMilliseconds = Duration.toMillis(
          options.pollInterval ?? Duration.seconds(2),
        );

        // XREAD reply shapes vary by client and RESP version: ioredis
        // replies with [stream, entries] tuples, node-redis with a Map
        // (binary type mapping) or an object keyed by stream name.
        // Normalize to the entry list of the single stream we read.
        const entriesOf = (
          reply: unknown,
        ): ReadonlyArray<[unknown, ReadonlyArray<unknown>]> => {
          if (reply instanceof Map) return [...reply.values()][0];
          if (Array.isArray(reply)) return reply[0][1];
          return Object.values(reply as object)[0];
        };

        const readFrom = (cursor: string) =>
          Stream.paginate(cursor, (cursor) =>
            Effect.gen(function* () {
              // BLOCK makes the server hold the read until an event arrives
              // (or 2s pass), so the repeat below re-issues immediately on an
              // empty reply without a client-side polling schedule.
              const reply = yield* redis
                .sendBinary(
                  "XREAD",
                  "BLOCK",
                  String(blockMilliseconds),
                  "STREAMS",
                  streamKey,
                  cursor,
                )
                .pipe(
                  Effect.mapError(TaskEngineError.of("Failed to poll stream")),
                  Effect.repeat({
                    until: (value) => !!value,
                  }),
                );

              // reassemble each entry's flat fields into an event record:
              // "new:"/"existing:" prefixed fields are task snapshots, the
              // rest is the tag-specific payload (values stay raw bytes — the
              // event schema decodes scalars and msgpack blobs per field)
              const records = entriesOf(reply).map(([entryId, fields]) => {
                const flat: Record<string, unknown> = {};
                const newTask: Record<string, unknown> = {};
                const existingTask: Record<string, unknown> = {};
                for (let i = 0; i < fields.length; i += 2) {
                  const key = asText(fields[i]);
                  const value = fields[i + 1];
                  if (key.startsWith("new:")) {
                    newTask[key.slice("new:".length)] = value;
                  } else if (key.startsWith("existing:")) {
                    existingTask[key.slice("existing:".length)] = value;
                  } else {
                    flat[key] = value;
                  }
                }
                const {
                  taskId,
                  generation,
                  protocolVersion,
                  schemaId,
                  _tag,
                  ...payloadFields
                } = flat;
                const tag = asText(_tag);
                const payload =
                  tag === "task.created"
                    ? { ...payloadFields, newTask }
                    : tag === "task.updated"
                      ? { ...payloadFields, existingTask, newTask }
                      : payloadFields;
                return {
                  id: asText(entryId),
                  taskId: asText(taskId),
                  generation,
                  protocolVersion,
                  schemaId,
                  _tag: tag,
                  payload,
                };
              });

              const events = yield* decodeEvents(records).pipe(
                Effect.tapError((error) => Effect.log(error.toString())),
              );

              const nextCursor = events[events.length - 1].id;

              return [events, Option.some(nextCursor)] as const;
            }),
          );

        return Stream.unwrap(
          Effect.gen(function* () {
            const [first, earliest, latest] = yield* eventCursorsFn(
              withPrefix(name),
            );
            const cursors = {
              first: asText(first),
              earliest: asText(earliest),
              latest: asText(latest),
            };
            const cursor = options.cursor ?? cursors.latest;
            const streamWasTrimmed =
              cursors.first !== "0-0" &&
              compareStreamIds(cursors.earliest, cursors.first) > 0;
            const requestedTrimmedEvent =
              cursor === "0" ||
              (compareStreamIds(cursor, cursors.first) >= 0 &&
                compareStreamIds(cursor, cursors.earliest) < 0);
            if (streamWasTrimmed && requestedTrimmedEvent) {
              return yield* new CursorExpired({
                requested: cursor,
                earliest: cursors.earliest,
              });
            }
            return readFrom(cursor);
          }),
        );
      },
    });
  });

/**
 * Builds a task engine from the ambient producer {@link RedisPool} service.
 *
 * @category Constructors
 * @since 0.1.0
 */
export const make = (config?: TaskEngineConfig) =>
  Effect.gen(function* () {
    const redis = yield* RedisPool;
    return yield* makeWithRedis(redis, config);
  });

/**
 * Provides {@link TaskEngine} from an ambient {@link RedisPool} service.
 *
 * @category Layers
 * @since 0.1.0
 */
export const layer = (config?: TaskEngineConfig) =>
  Layer.effect(TaskEngine, make(config));
