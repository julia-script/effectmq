/**
 * The low-level task engine: a Redis-backed service implementing the queue
 * primitives (create/take/complete/fail, locking, delayed and cron schedules)
 * as atomic Lua scripts. Most consumers should use the higher-level
 * `TaskQueue`/`Scheduler` APIs rather than calling the engine directly.
 *
 * @module
 */
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Redis from "effect/unstable/persistence/Redis";
import {
  type EngineTask,
  type EngineTaskInsert,
  EngineTaskSchema,
} from "./Schemas.js";

const TypeId = "~effectmq/TaskEngine" as const;

type TaskEngineConfig = {
  debugMode?: boolean;
  prefix?: string;
  workerId?: string;
};
export class TaskEngineError extends Data.TaggedError("TaskEngineError")<{
  readonly message?: string;
  readonly cause: unknown;
}> {
  static of(message: string) {
    return (cause: unknown) => new TaskEngineError({ cause, message });
  }
}

/**
 * The task engine service. Provides the atomic queue operations (create, take,
 * write success/error, lock management) and schedule coordination, backed by
 * Redis. Obtain an implementation via {@link layer}.
 */
export class TaskEngine extends Context.Service<
  TaskEngine,
  {
    readonly [TypeId]: typeof TypeId;
    readonly createTask: (
      task: EngineTaskInsert,
    ) => Effect.Effect<EngineTask, TaskEngineError>;
    readonly getTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<EngineTask | null, TaskEngineError>;
    readonly getList: (
      prefix: string,
      list: "wait" | "scheduled" | "active" | "failed" | "success",
    ) => Effect.Effect<string[], TaskEngineError>;

    readonly writeSuccess: (
      prefix: string,
      id: string,
      result: string,
    ) => Effect.Effect<void, TaskEngineError>;
    readonly writeError: (
      prefix: string,
      id: string,
      error: string,
    ) => Effect.Effect<void, TaskEngineError>;
    readonly extendLock: (
      prefix: string,
      id: string,
      lockTimeout: number,
    ) => Effect.Effect<void, TaskEngineError>;
    readonly removeLock: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;
    readonly takeTask: (
      prefix: string,
      lockTimeout: number,
    ) => Effect.Effect<EngineTask | null, TaskEngineError>;
    readonly removeTask: (
      prefix: string,
      id: string,
    ) => Effect.Effect<void, TaskEngineError>;

    readonly setSchedule: (
      id: string,
      next: Date,
    ) => Effect.Effect<Date, TaskEngineError>;
    readonly consumeSchedule: (
      name: string,
      toConsume: Date,
      next: Date,
    ) => Effect.Effect<{ consumed: boolean; next?: Date }, TaskEngineError>;
  }
>()("TaskEngine") {}

const importMap = {
  scheduleHash: /*lua*/ `local function scheduleHash(prefix, name) return prefix .. ":schedule:" .. name end`,
  taskHash: /*lua*/ `local function taskHash(prefix, id) return prefix .. ":task:" .. id end`,
  delayedList: /*lua*/ `local function delayedList(prefix) return prefix .. ":scheduled" end`,
  waitList: /*lua*/ `local function waitList(prefix) return prefix .. ":wait" end`,
  successList: /*lua*/ `local function successList(prefix) return prefix .. ":success" end`,
  failedList: /*lua*/ `local function failedList(prefix) return prefix .. ":failed" end`,
  activeList: /*lua*/ `local function activeList(prefix) return prefix .. ":active" end`,
  lockHash: /*lua*/ `local function lockHash(prefix, id) return prefix .. ":lock:" .. id end`,
  removeFromFailedList: /*lua*/ `local function removeFromFailedList(prefix, id) return redis.call("ZREM", failedList(prefix), id) end`,
  removeFromSuccessList: /*lua*/ `local function removeFromSuccessList(prefix, id) return redis.call("ZREM", successList(prefix), id) end`,
  removeFromWaitList: /*lua*/ `local function removeFromWaitList(prefix, id) return redis.call("LREM", waitList(prefix), 0, id) end`,
  removeFromDelayedList: /*lua*/ `local function removeFromDelayedList(prefix, id) return redis.call("ZREM", delayedList(prefix), id) end`,
  removeFromActiveLists: /*lua*/ `local function removeFromActiveLists(prefix, id) return redis.call("ZREM", activeList(prefix), id) end`,
  removeFromAllLists: /*lua*/ `local function removeFromAllLists(prefix, id) 
    removeFromFailedList(prefix, id)
    removeFromSuccessList(prefix, id)
    removeFromWaitList(prefix, id)
    removeFromDelayedList(prefix, id)
    removeFromActiveLists(prefix, id)
  end`,
  addToActiveLists: /*lua*/ `local function addToActiveLists(prefix, id) 
    removeFromAllLists(prefix, id)
    return redis.call("ZADD", activeList(prefix), now, id) 
  end`,
  addToWaitList: /*lua*/ `local function addToWaitList(prefix, id) 
    removeFromAllLists(prefix, id)
    return redis.call("RPUSH", waitList(prefix), id) 
  end`,
  addToDelayedList: /*lua*/ `local function addToDelayedList(prefix, id, readyAt)
    removeFromAllLists(prefix, id)
    return redis.call("ZADD", delayedList(prefix), readyAt, id)
  end`,
  addToSuccessList: /*lua*/ `local function addToSuccessList(prefix, id) 
    removeFromAllLists(prefix, id)
    return redis.call("ZADD", successList(prefix), now, id) 
  end`,
  addToFailedList: /*lua*/ `local function addToFailedList(prefix, id) 
    removeFromAllLists(prefix, id)
    return redis.call("ZADD", failedList(prefix), now, id) 
  end`,
  deleteTask: /*lua*/ `local function deleteTask(prefix, id) 
    removeFromAllLists(prefix, id)
    return redis.call("DEL", taskHash(prefix, id)) 
  end`,

  popWaitList: /*lua*/ `local function popWaitList(prefix) return redis.call("LPOP", waitList(prefix)) end`,

  getActiveList: /*lua*/ `local function getActiveList(prefix) return redis.call("ZRANGE", activeList(prefix), 0, -1) end`,

  getTask: /*lua*/ `local function getTask(prefix, id) return {"id", id,  unpack(redis.call("HGETALL", taskHash(prefix, id))) } end`,
  getTaskField: /*lua*/ `local function getTaskField(prefix, id, field) return redis.call("HGET", taskHash(prefix, id), field) end`,
  getTaskErrors: /*lua*/ `local function getTaskErrors(prefix, id) 
    return cjson.decode(redis.call("HGET", taskHash(prefix, id), "errors")) 
  end`,
  setTask: /*lua*/ `local function setTask(prefix, id, ...) return redis.call("HSET", taskHash(prefix, id), "updatedAt", now, ...) end`,
  setTaskErrors: /*lua*/ `local function setTaskErrors(prefix, id, errors) return setTask(prefix, id, "errors", cjson.encode(errors)) end`,
  appendTaskError: /*lua*/ `local function appendTaskError(prefix, id, error) 
		local errorsList = getTaskErrors(prefix, id)
		errorsList[#errorsList + 1] = error
		setTaskErrors(prefix, id, errorsList)
		return errorsList
	end`,
  failTask: /*lua*/ `local function failTask(prefix, id, error)
		-- error arrives as a JSON string (writeError) or a Lua table (syncLocks); normalize to a table
		-- so the stored errors list holds objects, not double-encoded strings
		local errorObj = error
		if type(error) == "string" then
			local ok, decoded = pcall(cjson.decode, error)
			errorObj = ok and decoded or error
		end
		local errorsList = appendTaskError(prefix, id, errorObj)
	   local onFailurePolicy = getTaskField(prefix, id, "onFailurePolicy")
		 local maxRetries = tonumber(getTaskField(prefix, id, "maxRetries"))

		 local errorTag = type(errorObj) == "table" and errorObj._tag or nil

		 if errorTag ~= "~effectmq/Error/Canceled" and #errorsList < maxRetries then
			  addToWaitList(prefix, id)
			  return
			end
      if onFailurePolicy == "delete" then
        deleteTask(prefix, id)
      elseif onFailurePolicy == "mark-as-failure" then
        addToFailedList(prefix, id)
      elseif onFailurePolicy == "keep" then
        removeFromAllLists(prefix, id)
      end
	end`,
  exists: /*lua*/ `local function exists(key) return redis.call("EXISTS", key) end`,
  lockTask: /*lua*/ `local function lockTask(prefix, id, workerId, lockTimeout) 
    addToActiveLists(prefix, id)
    return redis.call("SET", lockHash(prefix, id), workerId, "EX", lockTimeout) 
  end`,
  unlockTask: /*lua*/ `local function unlockTask(prefix, id) return redis.call("DEL", lockHash(prefix, id)) end`,
  isLocked: /*lua*/ `local function isLocked(prefix, id) return redis.call("EXISTS", lockHash(prefix, id)) > 0 end`,
  getLockId: /*lua*/ `local function getLockId(prefix, id) return redis.call("GET", lockHash(prefix, id)) end`,
  isLockedBy: /*lua*/ `local function isLockedBy(prefix, id, workerId) return getLockId(prefix, id) == workerId end`,
  syncLocks: /*lua*/ `local function syncLocks(prefix)
    -- clear expired locks. Locks will be removed automatically when the lock expires,
    -- but they could remain in the active list if the task is not completed
    local activeList = getActiveList(prefix)
    for i, id in ipairs(activeList) do
      if not isLocked(prefix, id) then
				failTask(prefix, id, {
					_tag = "Stalled",
					timestamp = now,
				})
      end
    end
  end`,
  syncDelayed: /*lua*/ `local function syncDelayed(prefix)
  -- we lazily move tasks from the delayed list to the wait list so we need to sync
  -- before performing any other operations
    local delayedList = prefix .. ":scheduled"
    local items = redis.call("ZRANGEBYSCORE", delayedList, 0, now)
    for i, item in ipairs(items) do
      redis.call("ZREM", delayedList, item)
      redis.call("RPUSH", prefix .. ":wait", item)
    end
  end`,
  syncAll: /*lua*/ `local function syncAll(prefix)
    syncDelayed(prefix)
    syncLocks(prefix)
  end`,
};
export type TaskEngineService = TaskEngine["Service"];
const MOCKTIME_KEY = "$$$effectmq/debug/mocktime";
const declare = (code: string, debugMode: boolean = false) => {
  let result: string = `-- code\n${code}`;

  for (const [key, value] of Object.entries(importMap).reverse()) {
    const reg = new RegExp(`\\b${key}\\b`, "g");
    if (reg.test(result)) {
      result = `${value}\n${result}\n`;
    }
  }
  if (debugMode) {
    result = /*lua*/ `local now = tonumber(redis.call("GET", "${MOCKTIME_KEY}") or (1000 * tonumber(redis.call("TIME")[1])))\n${result}`;
  } else {
    result = /*lua*/ `local now = 1000 * tonumber(redis.call("TIME")[1])\n${result}`;
  }

  result = `-- imports\n${result}\n`;

  return result;
};

/**
 * Override the engine's notion of "now" (only honored when the engine is built
 * with `debugMode`). Intended for deterministic tests of delays and schedules.
 */
export const setMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* Redis.Redis;
    yield* redis.send("SET", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.of("Failed to set mock time")));

/** Advance the mock clock by `time` (debug-mode only). See {@link setMockTime}. */
export const stepMockTime = (time: Duration.Input) =>
  Effect.gen(function* () {
    const redis = yield* Redis.Redis;
    yield* redis.send("INCRBY", MOCKTIME_KEY, String(Duration.toMillis(time)));
  }).pipe(Effect.mapError(TaskEngineError.of("Failed to step mock time")));

const buildScripts = (debugMode: boolean) => {
  const CreateOrUpdateTaskScript = Redis.script(
    (params: EngineTaskInsert, throwOnExists: boolean = false) => [
      params.prefix,
      throwOnExists,
      params.id,
      params.name,
      params.payload,
      params.delay,
      params.maxRetries,
      params.onSuccessPolicy,
      params.onFailurePolicy,
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
      local prefix = ARGV[1]
      syncAll(prefix)
      local throwOnExists = ARGV[2]

      local id = ARGV[3]

      local name = ARGV[4]
      local payload = ARGV[5]
      local delay = tonumber(ARGV[6])
      local maxRetries = tonumber(ARGV[7])
      local onSuccessPolicy = ARGV[8]
      local onFailurePolicy = ARGV[9]
      local hash = taskHash(prefix, id)

      if throwOnExists == true and exists(hash) then
        return redis.error_reply("task already exists")
      end

      setTask(
        prefix, id, 
        "name", name, 
				"createdAt", now,
        "payload", payload, 
        "delay", delay, 
        "maxRetries", maxRetries, 
        "onSuccessPolicy", onSuccessPolicy, 
        "onFailurePolicy", onFailurePolicy, 
        "errors", "[]"
      )

      if delay > 0 then
        addToDelayedList(prefix, id, now + delay)
      else
        addToWaitList(prefix, id)
      end

      return getTask(prefix, id)

      `,
        debugMode,
      ),
    },
  ).withReturnType<string[]>();

  const WriteSuccessResultScript = Redis.script(
    (prefix: string, workerId: string, id: string, result: unknown) => [
      prefix,
      workerId,
      id,
      JSON.stringify(result),
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
      local prefix = ARGV[1]
			local workerId = ARGV[2]
      local id = ARGV[3]
      local result = ARGV[4]
      local hash = taskHash(prefix, id)

      syncAll(prefix)

      if not exists(hash) then
        return redis.error_reply("Task not found")
      end
			if not isLockedBy(prefix, id, workerId) then
				return redis.error_reply("Task is locked by another worker")
			end
      setTask(prefix, id, "success", result)
      local task = getTask(prefix, id)
      local successPolicy = getTaskField(prefix, id, "onSuccessPolicy")

    
      if successPolicy == "delete" then
        deleteTask(prefix, id)
      elseif successPolicy == "mark-as-success" then
        addToSuccessList(prefix, id)
      elseif successPolicy == "keep" then
        removeFromAllLists(prefix, id)
      end
      unlockTask(prefix, id)
      return task
    `,
        debugMode,
      ),
    },
  );

  const WriteErrorResultScript = Redis.script(
    (prefix: string, workerId: string, id: string, error: unknown) => [
      prefix,
      workerId,
      id,
      JSON.stringify(error),
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
      local prefix = ARGV[1]
      syncAll(prefix)
			local workerId = ARGV[2]
      local id = ARGV[3]
      local error = ARGV[4]
      local hash = taskHash(prefix, id)


      if not exists(hash) then
        return redis.error_reply("Task not found")
      end
			if not isLockedBy(prefix, id, workerId) then
				return redis.error_reply("Task is locked by another worker")
			end


      local task = getTask(prefix, id)
			failTask(prefix, id, error)
			unlockTask(prefix, id)
			return task
    `,
        debugMode,
      ),
    },
  );

  const RemoveTaskScript = Redis.script(
    (prefix: string, workerId: string, id: string) => [prefix, workerId, id],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
      local prefix = ARGV[1]
      syncAll(prefix)
      local workerId = ARGV[2]
      local id = ARGV[3]

			local lock = getLockId(prefix, id)
	
			if lock and lock ~= workerId then
				return redis.error_reply("Task is locked by another worker")
			end
      deleteTask(prefix, id)
      return
      `,
        debugMode,
      ),
    },
  );

  const TakeTaskScript = Redis.script(
    (prefix: string, workerId: string, lockTimeout: number) => [
      prefix,
      workerId,
      lockTimeout,
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
  local prefix = ARGV[1]
  syncAll(prefix)
	local workerId = ARGV[2]
	local lockTimeout = tonumber(ARGV[3])

	local taskId = popWaitList(prefix)

	-- LPOP returns false (not nil) on an empty list
	if not taskId then
		return nil
	end
		-- sanity check: tasks on wait list should never be locked, but just in case
	if isLocked(prefix, taskId) then
		addToActiveLists(prefix, taskId)
		return redis.error_reply("Task is locked by another worker")
	end

	local lock = lockTask(prefix, taskId, workerId, lockTimeout)
	if lock == nil then
		return nil
	end
	local task = getTask(prefix, taskId)
	return task

  `,
        debugMode,
      ),
    },
  );

  const ExtendLockScript = Redis.script(
    (prefix: string, workerId: string, id: string, lockTimeout: number) => [
      prefix,
      workerId,
      id,
      lockTimeout,
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
			local prefix = ARGV[1]
			local workerId = ARGV[2]
			local id = ARGV[3]
			local lockTimeout = tonumber(ARGV[4])
			local lock = getLockId(prefix, id)
			-- GET returns false (not nil) when the lock key is absent
			if not lock then
				return
			end
			if lock ~= workerId then
				return redis.error_reply("Task is locked by another worker")
			end
			lockTask(prefix, id, workerId, lockTimeout)
			return
			`,
        debugMode,
      ),
    },
  );

  const RemoveLockScript = Redis.script(
    (prefix: string, workerId: string, id: string) => [prefix, workerId, id],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
			local prefix = ARGV[1]
			local workerId = ARGV[2]
			local id = ARGV[3]
			local lock = getLockId(prefix, id)
			-- GET returns false (not nil) when the lock key is absent
			if not lock then
			  return
		  end
			if lock ~= workerId then
				return redis.error_reply("Task is locked by another worker")
			end
			unlockTask(prefix, id)
			return
			`,
        debugMode,
      ),
    },
  );

  const SetScheduleScript = Redis.script(
    (prefix: string, name: string, next: string) => [prefix, name, next],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
			local prefix = ARGV[1]
			local name = ARGV[2]
			local next = tonumber(ARGV[3])
			local hash = scheduleHash(prefix, name)
			redis.call("HSETNX", hash, "next", next)

			return tonumber(redis.call("HGET", hash, "next"))
			
			`,
        debugMode,
      ),
    },
  ).withReturnType<number>();

  const ConsumeScheduleScript = Redis.script(
    (prefix: string, name: string, current: number, next: number) => [
      prefix,
      name,
      current,
      next,
    ],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
				local prefix = ARGV[1]
				local name = ARGV[2]
				local currentToConsume = tonumber(ARGV[3])
				local nextToSet = tonumber(ARGV[4])
				local hash = scheduleHash(prefix, name)

				local currentSchedule = tonumber(redis.call("HGET", hash, "next"))
				-- if schedule is not set, we return nil
				if not currentSchedule then
					return { false, nil } 
				end
				redis.log(redis.LOG_WARNING,  now)
				redis.log(redis.LOG_WARNING,  currentSchedule)
				redis.log(redis.LOG_WARNING,  next)

				-- if the expected current schedule is not equal to the current schedule, 
				-- we assume the "next" schedule has been calculated relative to the wrong time
				-- so we discard it and send the acual current schedule so the worker can use it to try again
				if currentSchedule ~= currentToConsume then
					return { false, currentSchedule }
				end

				-- if the current schedule match, but the vent is still in the future, we also discard it
				if now < currentSchedule then
			  	return { false, currentSchedule }
				end

				-- if the event is in the past, we can consume it and schedule the next event
				if currentSchedule < nextToSet then
					redis.call("HSET", hash, "next", nextToSet)
					return { true, nextToSet }
				end

				return { false, currentSchedule }

				`,
      ),
    },
  ).withReturnType<[boolean, number | null]>();

  const GetListScript = Redis.script(
    (
      prefix: string,
      list: "wait" | "scheduled" | "active" | "failed" | "success",
    ) => [prefix, list],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
				local prefix = ARGV[1]
				syncAll(prefix)
				local list = ARGV[2]
				if list == "scheduled" then
					return redis.call("ZRANGEBYSCORE", delayedList(prefix), "0", "inf")
				elseif list == "wait" then
					return redis.call("LRANGE", waitList(prefix), 0, -1)
				elseif list == "active" then
					return redis.call("ZRANGEBYSCORE", activeList(prefix), 0, "inf")
				elseif list == "failed" then
					return redis.call("ZRANGEBYSCORE", failedList(prefix), 0, "inf")
				elseif list == "success" then
					return redis.call("ZRANGEBYSCORE", successList(prefix), 0, "inf")
				end
				return redis.error_reply("Invalid list")
				`,
        debugMode,
      ),
    },
  ).withReturnType<string[]>();
  return {
    CreateOrUpdateTaskScript,
    WriteSuccessResultScript,
    WriteErrorResultScript,
    RemoveTaskScript,
    TakeTaskScript,
    ExtendLockScript,
    RemoveLockScript,
    SetScheduleScript,
    ConsumeScheduleScript,
    GetListScript,
  };
};

const parseTask = (task: string[]) => {
  const result: Record<string, unknown> = {};
  for (let i = 0; i < task.length; i += 2) {
    const key = task[i];
    let value: unknown = task[i + 1];
    if (key === "errors") {
      value = JSON.parse(value as string);
    }
    if (key === "createdAt" || key === "updatedAt") {
      value = new Date(Number(value));
    }
    if (key === "delay" || key === "maxRetries") {
      value = Number(value);
    }

    result[key] = value;
  }
  return Schema.decodeUnknownEffect(EngineTaskSchema)(result).pipe(
    Effect.mapError(TaskEngineError.of("Failed to decode task")),
  );
};
export const makePrefix = (...prefixes: string[]) => prefixes.join(":");

/**
 * Build a {@link TaskEngine} implementation against the ambient `Redis`
 * service. `debugMode` enables the mockable clock (see {@link setMockTime});
 * `prefix` namespaces all keys; `workerId` identifies this worker for locking.
 * Usually consumed via {@link layer}.
 */
export const make = ({
  debugMode = false,
  prefix = "@@effectmq",
  workerId = `worker/${crypto.randomUUID()}`,
}: TaskEngineConfig = {}) =>
  Effect.gen(function* () {
    const redis = yield* Redis.Redis;
    const scripts = buildScripts(debugMode);
    const withPrefix = (key: string) => `${prefix}:${key}`;

    const createTask = redis.eval(scripts.CreateOrUpdateTaskScript);
    const writeSuccessResult = redis.eval(scripts.WriteSuccessResultScript);
    const writeErrorResult = redis.eval(scripts.WriteErrorResultScript);
    const takeTask = redis.eval(
      scripts.TakeTaskScript.withReturnType<string[]>(),
    );
    const removeTask = redis.eval(scripts.RemoveTaskScript);
    const extendLock = redis.eval(scripts.ExtendLockScript);
    const removeLock = redis.eval(scripts.RemoveLockScript);

    const setSchedule = redis.eval(scripts.SetScheduleScript);
    const consumeSchedule = redis.eval(scripts.ConsumeScheduleScript);

    const getList = redis.eval(scripts.GetListScript);

    return TaskEngine.of({
      [TypeId]: TypeId,
      createTask: (task: EngineTaskInsert) =>
        createTask(
          {
            ...task,
            prefix: withPrefix(task.prefix),
          },
          false,
        ).pipe(
          Effect.flatMap(parseTask),
          Effect.mapError(TaskEngineError.of("Failed to create task")),
        ),

      getTask: (prefix: string, id: string) =>
        redis
          .send<string[]>("HGETALL", withPrefix(`${prefix}:task:${id}`))
          .pipe(
            Effect.flatMap((fields) =>
              // the task hash does not store its own id, so inject it like the Lua getTask does
              fields.length > 0
                ? parseTask(["id", id, ...fields])
                : Effect.succeed(null),
            ),
            Effect.mapError(TaskEngineError.of("Failed to get task")),
          ),
      writeSuccess: (prefix: string, id: string, result: string) => {
        return writeSuccessResult(
          withPrefix(prefix),
          workerId,
          id,
          result,
        ).pipe(
          Effect.mapError(TaskEngineError.of("Failed to write success result")),
        );
      },
      writeError: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        error: string,
      ) {
        return yield* writeErrorResult(
          withPrefix(prefix),
          workerId,
          id,
          error,
        ).pipe(
          Effect.mapError(TaskEngineError.of("Failed to write error result")),
        );
      }),
      getList: (
        prefix: string,
        list: "wait" | "scheduled" | "active" | "failed" | "success",
      ) =>
        Effect.gen(function* () {
          return yield* getList(withPrefix(prefix), list).pipe(
            Effect.mapError(TaskEngineError.of("Failed to get list")),
          );
        }),
      takeTask: Effect.fnUntraced(function* (
        prefix: string,
        lockTimeout: number,
      ) {
        const result = yield* takeTask(
          withPrefix(prefix),
          workerId,
          lockTimeout,
        ).pipe(Effect.mapError(TaskEngineError.of("Failed to take task")));

        return result ? yield* parseTask(result) : null;
      }),
      removeTask: (prefix, id) => {
        return removeTask(withPrefix(prefix), workerId, id).pipe(
          Effect.mapError(TaskEngineError.of("Failed to remove task")),
        );
      },

      extendLock: (prefix, id, lockTimeout) => {
        return extendLock(withPrefix(prefix), workerId, id, lockTimeout).pipe(
          Effect.mapError(TaskEngineError.of("Failed to extend lock")),
        );
      },
      removeLock: (prefix, id) => {
        return removeLock(withPrefix(prefix), workerId, id).pipe(
          Effect.mapError(TaskEngineError.of("Failed to remove lock")),
        );
      },
      setSchedule: (name, next) => {
        return setSchedule(prefix, name, String(next.getTime())).pipe(
          Effect.map((next) => new Date(next)),
          Effect.mapError(TaskEngineError.of("Failed to set schedule")),
        );
      },
      consumeSchedule: (name, toConsume, next) => {
        return consumeSchedule(
          prefix,
          name,
          toConsume.getTime(),
          next.getTime(),
        ).pipe(
          Effect.map(([consumed, next]) => ({
            consumed,
            next: next ? new Date(next) : undefined,
          })),
          Effect.mapError(TaskEngineError.of("Failed to consume schedule")),
        );
      },
    });
  });

/** A `Layer` providing the {@link TaskEngine} service; requires a `Redis` service. */
export const layer = (config?: TaskEngineConfig) =>
  Layer.effect(TaskEngine, make(config));
