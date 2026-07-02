/**
 * The low-level task engine: a Redis-backed service implementing the queue
 * primitives (create/take/complete/fail, locking, delayed and cron schedules,
 * and a per-queue event stream) as atomic Lua scripts. Most consumers should
 * use the higher-level `TaskQueue`/`Scheduler` APIs rather than calling the
 * engine directly.
 *
 * @module
 */
import { Schedule } from "effect";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Redis from "effect/unstable/persistence/Redis";
import {
  type EngineTask,
  EngineTaskFromRedisEntriesSchema,
  type EngineTaskInsert,
  type Event,
  IncomingRedisEventSchema,
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
      result: unknown,
    ) => Effect.Effect<void, TaskEngineError>;
    readonly writeError: (
      prefix: string,
      id: string,
      error: unknown,
      retryAt?: Duration.Input,
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
    /**
     * Stream lifecycle events for a queue, read from its Redis Stream
     * (`<prefix>:<name>:events`) via `XREAD`. Starts from `cursor` (defaulting
     * to now) and polls every `pollInterval` (default 1s), advancing the cursor
     * past each yielded event. Events are raw {@link Event}s; `TaskQueue.stream`
     * decodes their payloads against the queue's schemas.
     */
    readonly stream: (
      name: string,
      options?: {
        cursor?: string;
        pollInterval?: Duration.Duration;
      },
    ) => Stream.Stream<Event, TaskEngineError | Schema.SchemaError>;
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
  eventStream: /*lua*/ `local function eventStream(prefix) return prefix .. ":events" end`,
  lockHash: /*lua*/ `local function lockHash(prefix, id) return prefix .. ":lock:" .. id end`,

  removeFromFailedList: /*lua*/ `local function removeFromFailedList(prefix, id) return redis.call("ZREM", failedList(prefix), id) end`,
  removeFromSuccessList: /*lua*/ `local function removeFromSuccessList(prefix, id) return redis.call("ZREM", successList(prefix), id) end`,
  removeFromWaitList: /*lua*/ `local function removeFromWaitList(prefix, id) return redis.call("LREM", waitList(prefix), 0, id) end`,
  removeFromDelayedList: /*lua*/ `local function removeFromDelayedList(prefix, id) return redis.call("ZREM", delayedList(prefix), id) end`,
  removeFromActiveLists: /*lua*/ `local function removeFromActiveLists(prefix, id) return redis.call("ZREM", activeList(prefix), id) end`,

  publishEvent: /*lua*/ `local function publishEvent(prefix, id, eventType, payload) return redis.call("XADD", eventStream(prefix), "*", "taskId", id, "_tag", eventType, "payload", cjson.encode(payload)) end`,
  indexOf: /*lua*/ `local function indexOf(list, id) 
    if list == "wait" then
      return redis.call("LPOS", list, id)
    end
    return redis.call("ZRANK", list, id) 
  end
  `,
  findTaskList: /*lua*/ `local function findTaskList(prefix, id)
    if indexOf(waitList(prefix), id) ~= nil then
      return "wait"
    end
    if indexOf(delayedList(prefix), id) ~= nil then
      return "scheduled"
    end
    if indexOf(activeList(prefix), id) ~= nil then
      return "active"
    end
    if indexOf(failedList(prefix), id) ~= nil then
      return "failed"
    end
    if indexOf(successList(prefix), id) ~= nil then
      return "success"
    end
    return nil
  end
  `,
  getListSize: /*lua*/ `local function getListSize(list) 
  if list == "wait" then
    return redis.call("LLEN", list)
  end
  if list == "scheduled" then
    return redis.call("ZCARD", list)
  end
  if list == "active" then
    return redis.call("ZCARD", list)
  end
  if list == "failed" then
    return redis.call("ZCARD", list)
  end
  if list == "success" then
    return redis.call("ZCARD", list)
  end
  return 0
  end`,
  removeFromCurrentLists: /*lua*/ `local function removeFromCurrentLists(prefix, id) 
     if removeFromWaitList(prefix, id) > 0 then
      return "wait"
    end
    if removeFromDelayedList(prefix, id) > 0 then
      return "scheduled"
    end
    if removeFromActiveLists(prefix, id)  > 0 then
      return "active"
    end
    if removeFromFailedList(prefix, id) > 0 then
      return "failed"
    end
    if removeFromSuccessList(prefix, id) > 0 then
      return "success"
    end
    return nil
  end`,

  addToActiveLists: /*lua*/ `local function addToActiveLists(prefix, id)
    return redis.call("ZADD", activeList(prefix), now, id)
  end`,
  addToWaitList: /*lua*/ `local function addToWaitList(prefix, id)
    return redis.call("RPUSH", waitList(prefix), id)
  end`,
  addToDelayedList: /*lua*/ `local function addToDelayedList(prefix, id, readyAt)
    return redis.call("ZADD", delayedList(prefix), readyAt, id)
  end`,
  addToSuccessList: /*lua*/ `local function addToSuccessList(prefix, id)
    return redis.call("ZADD", successList(prefix), now, id)
  end`,
  addToFailedList: /*lua*/ `local function addToFailedList(prefix, id)
    return redis.call("ZADD", failedList(prefix), now, id)
  end`,

  // the add* helpers no longer clear other lists; moveToList is the single entry
  // point that removes from the current list, adds to the target, and emits task.moved
  moveToList: /*lua*/ `local function moveToList(prefix, id, list, readyAt)
    local currentList = removeFromCurrentLists(prefix, id)
    if currentList == list then
      return
    end
    if list == "wait" then
      addToWaitList(prefix, id)
    elseif list == "scheduled" then
      addToDelayedList(prefix, id, readyAt)
    elseif list == "active" then
      addToActiveLists(prefix, id)
    elseif list == "failed" then
      addToFailedList(prefix, id)
    elseif list == "success" then
      addToSuccessList(prefix, id)
    end

    publishEvent(prefix, id, "task.moved", { from = currentList, to = list })
  end`,
  deleteTask: /*lua*/ `local function deleteTask(prefix, id) 
    moveToList(prefix, id, nil)
    return redis.call("DEL", taskHash(prefix, id)) 
  end`,
  popWaitList: /*lua*/ `local function popWaitList(prefix) return redis.call("LINDEX", waitList(prefix), 0) end`,

  getActiveList: /*lua*/ `local function getActiveList(prefix) return redis.call("ZRANGE", activeList(prefix), 0, -1) end`,

  getTask: /*lua*/ `local function getTask(prefix, id) 
   local fields = redis.call("HGETALL", taskHash(prefix, id))
   if #fields > 0 then
    return {"id", id,  unpack(fields) }
   end
   return nil
  end`,
  getTaskField: /*lua*/ `local function getTaskField(prefix, id, field) return redis.call("HGET", taskHash(prefix, id), field) end`,
  getTaskErrors: /*lua*/ `local function getTaskErrors(prefix, id) 
    return cjson.decode(redis.call("HGET", taskHash(prefix, id), "errors")) 
  end`,
  setTask: /*lua*/ `local function setTask(prefix, id, ...) return redis.call("HSET", taskHash(prefix, id), "updatedAt", now, ...) end`,
  setTaskErrors: /*lua*/ `local function setTaskErrors(prefix, id, errors) return setTask(prefix, id, "errors", cjson.encode(errors)) end`,
  appendTaskError: /*lua*/ `local function appendTaskError(prefix, id, error, retryAt) 
		local errorsList = getTaskErrors(prefix, id)
		errorsList[#errorsList + 1] = {error = error, timestamp = now, retryAt = retryAt}
		setTaskErrors(prefix, id, errorsList)
		return errorsList
	end`,

  exists: /*lua*/ `local function exists(key) return redis.call("EXISTS", key) end`,
  lockTask: /*lua*/ `local function lockTask(prefix, id, workerId, lockTimeout) 
    moveToList(prefix, id, "active")
    return redis.call("SET", lockHash(prefix, id), workerId, "EX", lockTimeout) 
  end`,
  unlockTask: /*lua*/ `local function unlockTask(prefix, id) return redis.call("DEL", lockHash(prefix, id)) end`,
  isLocked: /*lua*/ `local function isLocked(prefix, id) return redis.call("EXISTS", lockHash(prefix, id)) > 0 end`,
  getLockId: /*lua*/ `local function getLockId(prefix, id) return redis.call("GET", lockHash(prefix, id)) end`,
  isLockedBy: /*lua*/ `local function isLockedBy(prefix, id, workerId) return getLockId(prefix, id) == workerId end`,
  failTask: /*lua*/ `
  local function failTask(prefix, id, error, retryAt) 
		-- error arrives as a JSON string (writeError) or a Lua table (syncLocks); normalize to a table
		-- so the stored errors list holds objects, not double-encoded strings
    
    unlockTask(prefix, id)

		local errorObj = error
		if type(error) == "string" then
			local ok, decoded = pcall(cjson.decode, error)
			errorObj = ok and decoded or error
		end
			-- retryAt arrives as -1 (or nil) when no retry is scheduled; normalize
			-- to nil so it is omitted from the stored error and the event payload
			if (retryAt or -1) < 0 then retryAt = nil end
		 appendTaskError(prefix, id, errorObj, retryAt)
	   local onFailurePolicy = getTaskField(prefix, id, "onFailurePolicy")

		 local errorTag = type(errorObj) == "table" and type(errorObj.error) == "table" and errorObj.error._tag or nil

     local willRetry = errorTag ~= "~effectmq/Error/Canceled" and retryAt ~= nil
      publishEvent(prefix, id, "task.failed", { 
        error = errorObj, 
        policy = onFailurePolicy,  
        retryAt = retryAt
      })
		 if willRetry then
        if retryAt > now then

            moveToList(prefix, id, "scheduled", retryAt)

          else
            moveToList(prefix, id, "wait")
          end
			  return
			end
      if onFailurePolicy == "delete" then
        deleteTask(prefix, id)
      elseif onFailurePolicy == "mark-as-failure" then
        moveToList(prefix, id, "failed")
      elseif onFailurePolicy == "keep" then
        moveToList(prefix, id, nil)
      end
	end`,
  syncLocks: /*lua*/ `local function syncLocks(prefix)
    -- clear expired locks. Locks will be removed automatically when the lock expires,
    -- but they could remain in the active list if the task is not completed
    local activeList = getActiveList(prefix)
    for i, id in ipairs(activeList) do
      if not isLocked(prefix, id) then
				failTask(prefix, id, {
					_tag = "Stalled",
					timestamp = now,
				}, 0)
      end
    end
  end`,

  syncDelayed: /*lua*/ `local function syncDelayed(prefix)
  -- we lazily move tasks from the delayed list to the wait list so we need to sync
  -- before performing any other operations
    local delayedList = delayedList(prefix)
    local items = redis.call("ZRANGEBYSCORE", delayedList, 0, now)
    for i, item in ipairs(items) do
      moveToList(prefix, item, "wait")
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
  let header = ``;

  for (const [key, value] of Object.entries(importMap).reverse()) {
    const reg = new RegExp(`\\b${key}\\b`, "g");
    if (reg.test(code) || reg.test(header)) {
      header = `${value}\n${header}\n`;
    }
  }
  let now = ``;
  if (debugMode) {
    now = /*lua*/ `local now = tonumber(redis.call("GET", "${MOCKTIME_KEY}") or (1000 * tonumber(redis.call("TIME")[1])))\n`;
  } else {
    now = /*lua*/ `local now = 1000 * tonumber(redis.call("TIME")[1])\n`;
  }

  const result = `${now}\n${header}\n${code}`;
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
      JSON.stringify(params.payload),
      params.delay,
      params.maxRetries ?? -1,
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
      local existingTask = getTask(prefix, id)

      if throwOnExists == true and existingTask ~= nil then
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
      local newTask = getTask(prefix, id)
      if existingTask ~= nil then
        publishEvent(prefix, id, "task.updated", { existingTask = existingTask, newTask = newTask})
      else
        publishEvent(prefix, id, "task.created", { newTask = newTask})
      end

      if delay > 0 then
        moveToList(prefix, id, "scheduled", now + delay)
      else
        moveToList(prefix, id, "wait")
      end

      return newTask

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

      -- result arrives as a JSON string; normalize to a Lua value for the event
      -- payload so publishEvent's cjson.encode wraps it exactly once (mirrors the
      -- errorObj handling in failTask), not double-encoded
      local successObj = result
      if type(result) == "string" then
        local ok, decoded = pcall(cjson.decode, result)
        successObj = ok and decoded or result
      end

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

      publishEvent(prefix, id, "task.completed", {
        success = successObj,
        policy = successPolicy
      })


    
      if successPolicy == "delete" then
        deleteTask(prefix, id)
      elseif successPolicy == "mark-as-success" then
        moveToList(prefix, id, "success")
      elseif successPolicy == "keep" then
        moveToList(prefix, id, nil)
      end
      return task
    `,
        debugMode,
      ),
    },
  );

  const WriteErrorResultScript = Redis.script(
    (
      prefix: string,
      workerId: string,
      id: string,
      error: unknown,
      retryAt: number,
    ) => [prefix, workerId, id, JSON.stringify(error), retryAt],
    {
      numberOfKeys: 0,
      lua: declare(
        /*lua*/ `
      local prefix = ARGV[1]
      syncAll(prefix)
			local workerId = ARGV[2]
      local id = ARGV[3]
      local error = cjson.decode(ARGV[4])
      local retryAt = tonumber(ARGV[5]) or -1
      local hash = taskHash(prefix, id)



      if not exists(hash) then
        return redis.error_reply("Task not found")
      end
			if not isLockedBy(prefix, id, workerId) then
				return redis.error_reply("Task is locked by another worker")
			end


      local task = getTask(prefix, id)
			failTask(prefix, id, error, retryAt)
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
		moveToList(prefix, taskId, "active")
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

/** Decode a flat `["id", id, "name", name, ...]` field list from Redis into an {@link EngineTask}. */
const parseTask = (task: string[]) =>
  Schema.decodeEffect(EngineTaskFromRedisEntriesSchema)(task).pipe(
    Effect.mapError(TaskEngineError.of("Failed to decode task")),
  );
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
    const ev = <
      Config extends {
        readonly params: ReadonlyArray<unknown>;
        readonly result: unknown;
      },
    >(
      script: Redis.Script<Config>,
      message: string,
    ) => {
      const fn = redis.eval(script);
      // the numbered Lua source is a debugging aid; keep it out of production errors
      const detail = debugMode
        ? `${message}\n${script.lua
            .split("\n")
            .map((line, i) => `[${i}] ${line}`)
            .join("\n")}`
        : message;
      return (...params: Config["params"]) =>
        fn(...params).pipe(Effect.mapError(TaskEngineError.of(detail)));
    };

    const createTask = ev(
      scripts.CreateOrUpdateTaskScript,
      "Failed to create task",
    );
    const writeSuccessResult = ev(
      scripts.WriteSuccessResultScript,
      "Failed to write success result",
    );
    const writeErrorResult = ev(
      scripts.WriteErrorResultScript,
      "Failed to write error result",
    );
    const takeTask = ev(
      scripts.TakeTaskScript.withReturnType<string[]>(),
      "Failed to take task",
    );
    const removeTask = ev(scripts.RemoveTaskScript, "Failed to remove task");
    const extendLock = ev(scripts.ExtendLockScript, "Failed to extend lock");
    const removeLock = ev(scripts.RemoveLockScript, "Failed to remove lock");
    const setSchedule = ev(scripts.SetScheduleScript, "Failed to set schedule");
    const consumeSchedule = ev(
      scripts.ConsumeScheduleScript,
      "Failed to consume schedule",
    );
    const getList = ev(scripts.GetListScript, "Failed to get list");

    return TaskEngine.of({
      [TypeId]: TypeId,
      createTask: (task: EngineTaskInsert) => {
        return createTask(
          {
            ...task,

            prefix: withPrefix(task.prefix),
          },
          false,
        ).pipe(Effect.flatMap(parseTask));
      },

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
      writeSuccess: (prefix: string, id: string, result: unknown) => {
        return writeSuccessResult(withPrefix(prefix), workerId, id, result);
      },
      writeError: Effect.fnUntraced(function* (
        prefix: string,
        id: string,
        error: unknown,
        retryAt?: Duration.Input,
      ) {
        return yield* writeErrorResult(
          withPrefix(prefix),
          workerId,
          id,
          error,
          retryAt ? Duration.toMillis(retryAt) : -1,
        );
      }),
      getList: (
        prefix: string,
        list: "wait" | "scheduled" | "active" | "failed" | "success",
      ) =>
        Effect.gen(function* () {
          return yield* getList(withPrefix(prefix), list);
        }),
      takeTask: Effect.fnUntraced(function* (
        prefix: string,
        lockTimeout: number,
      ) {
        const result = yield* takeTask(
          withPrefix(prefix),
          workerId,
          lockTimeout,
        );

        return result ? yield* parseTask(result) : null;
      }),
      removeTask: (prefix, id) => {
        return removeTask(withPrefix(prefix), workerId, id);
      },

      extendLock: (prefix, id, lockTimeout) => {
        return extendLock(withPrefix(prefix), workerId, id, lockTimeout);
      },
      removeLock: (prefix, id) => {
        return removeLock(withPrefix(prefix), workerId, id);
      },
      setSchedule: (name, next) => {
        return setSchedule(prefix, name, String(next.getTime())).pipe(
          Effect.map((next) => new Date(next)),
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
        );
      },

      stream: (
        name,
        options: { cursor?: string; pollInterval?: Duration.Duration } = {},
      ) => {
        const { cursor = `${Date.now()}`, pollInterval = Duration.seconds(1) } =
          options;
        const responseSchema = Schema.Array(
          Schema.Tuple([Schema.String, Schema.Array(IncomingRedisEventSchema)]),
        );
        const decode = Schema.decodeUnknownEffect(responseSchema);

        const streamKey = `${withPrefix(name)}:events`;
        return Stream.paginate(cursor, (cursor) =>
          Effect.gen(function* () {
            const reply = yield* redis
              .send("XREAD", "STREAMS", streamKey, cursor)
              .pipe(
                Effect.mapError(TaskEngineError.of("Failed to poll stream")),
                Effect.repeat({
                  schedule: Schedule.spaced(pollInterval),
                  until: (value) => !!value,
                }),
              );
            if (!reply) {
              yield* Effect.sleep(pollInterval);
            }

            const entries = yield* decode(reply).pipe(
              Effect.tapError((error) => Effect.log(error.toString())),
            );
            const events = entries[0][1];

            const nextCursor = events[events.length - 1].id;

            return [events, Option.some(nextCursor)] as const;
          }),
        );
      },
    });
  });

/** A `Layer` providing the {@link TaskEngine} service; requires a `Redis` service. */
export const layer = (config?: TaskEngineConfig) =>
  Layer.effect(TaskEngine, make(config));
