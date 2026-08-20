-- The effectmq task engine as one content-addressed Redis script. The caller
-- invokes it through SCRIPT LOAD/EVALSHA with an operation name, the debug
-- flag ("1"/"0"), then the operation's own arguments.
--
-- Structured values (payload, errors, creator, success, event
-- payloads) travel and rest as MessagePack: packed once at the Node
-- boundary, unpacked once at the function entry point (cmsgpack here,
-- effect's Msgpack codec on the Node side).

local MOCKTIME_KEY = "$$$effectmq/debug/mocktime"
local EMPTY_LIST = nil
local maintenanceBatchSize = 100
local maintenanceRemaining = 100

-- set once per script invocation; helpers close over it
local now = 0

local function redisNow()
  local time = redis.call("TIME")
  return (1000 * tonumber(time[1])) + math.floor(tonumber(time[2]) / 1000)
end

local function getNow(debug)
  if debug == "1" then
    return tonumber(redis.call("GET", MOCKTIME_KEY) or redisNow())
  end
  return redisNow()
end

-- key helpers ---------------------------------------------------------------

local function scheduleHash(prefix, name) return prefix .. ":schedule:" .. name end
local function taskHash(prefix, id) return prefix .. ":task:" .. id end
local function generationHash(prefix) return prefix .. ":generations" end
local function createdList(prefix) return prefix .. ":created" end
local function delayedList(prefix) return prefix .. ":scheduled" end
local function waitList(prefix) return prefix .. ":wait" end
local function successList(prefix) return prefix .. ":success" end
local function failedList(prefix) return prefix .. ":failed" end
local function activeList(prefix) return prefix .. ":active" end
local function eventStream(prefix) return prefix .. ":events" end
local function eventMetadata(prefix) return prefix .. ":events:metadata" end
local function lockHash(prefix, id) return prefix .. ":lock:" .. id end
local function resultHash(prefix, id, generation)
  return prefix .. ":result:" .. id .. ":" .. generation
end
local function taskExpiryIndex(prefix) return prefix .. ":expiry:tasks" end
local function resultExpiryIndex(prefix) return prefix .. ":expiry:results" end
local function terminalExpiryIndex(prefix) return prefix .. ":expiry:terminal-indexes" end
local function deadLetterList(prefix) return prefix .. ":dead-letter" end
local function deadLetterExpiryIndex(prefix) return prefix .. ":expiry:dead-letter" end
local function retentionHoldersKey(prefix, id, generation)
  return prefix .. ":task:" .. id .. ":" .. generation .. ":retained-by"
end
local function retainedTasksKey(prefix, id, generation)
  return prefix .. ":task:" .. id .. ":" .. generation .. ":retains"
end
local function retentionContinuationKey(prefix) return prefix .. ":retention-release-continuations" end
local function maintenanceCursorKey(prefix) return prefix .. ":maintenance:cursor" end

-- list membership -----------------------------------------------------------

local function removeFromFailedList(prefix, id) return redis.call("ZREM", failedList(prefix), id) end
local function removeFromSuccessList(prefix, id) return redis.call("ZREM", successList(prefix), id) end
local function removeFromWaitList(prefix, id) return redis.call("LREM", waitList(prefix), 0, id) end
local function removeFromDelayedList(prefix, id) return redis.call("ZREM", delayedList(prefix), id) end
local function removeFromActiveLists(prefix, id) return redis.call("ZREM", activeList(prefix), id) end

-- fields is a flat {k1, v1, k2, v2, ...} list. Event data is published as
-- individual stream fields (not one packed document) so that raw msgpack
-- values (payload, success) are carried as binary-safe bulk strings — nesting
-- them inside another msgpack document would corrupt them on decode.
local function publishEvent(prefix, id, eventType, fields)
  local generation = redis.call("HGET", taskHash(prefix, id), "generation") or "0"
  local protocolVersion = redis.call("HGET", taskHash(prefix, id), "protocolVersion") or "1"
  local schemaId = redis.call("HGET", taskHash(prefix, id), "schemaId") or "unknown"
  local maxEventEntries = redis.call("HGET", taskHash(prefix, id), "maxEventEntries") or "10000"
  local eventRetentionMs = tonumber(
    redis.call("HGET", taskHash(prefix, id), "eventRetentionMs") or "604800000"
  )
  local args = {
    "XADD", eventStream(prefix), "MAXLEN", "~", maxEventEntries, "*",
    "taskId", id,
    "generation", generation,
    "protocolVersion", protocolVersion,
    "schemaId", schemaId,
    "_tag", eventType,
  }
  for i = 1, #fields do
    args[#args + 1] = fields[i]
  end
  local eventId = redis.call(unpack(args))
  local cutoff = redisNow() - eventRetentionMs
  if cutoff < 0 then cutoff = 0 end
  redis.call("XTRIM", eventStream(prefix), "MINID", "~", string.format("%.0f-0", cutoff))
  redis.call("HSETNX", eventMetadata(prefix), "firstEventId", eventId)
  return eventId
end

local function latestEventCursor(prefix)
  local entries = redis.call("XREVRANGE", eventStream(prefix), "+", "-", "COUNT", 1)
  return entries[1] and entries[1][1] or "0-0"
end

local function eventCursors(prefix)
  local earliest = redis.call("XRANGE", eventStream(prefix), "-", "+", "COUNT", 1)
  local latest = redis.call("XREVRANGE", eventStream(prefix), "+", "-", "COUNT", 1)
  local firstEventId = redis.call("HGET", eventMetadata(prefix), "firstEventId") or "0-0"
  return {
    firstEventId,
    earliest[1] and earliest[1][1] or "0-0",
    latest[1] and latest[1][1] or "0-0",
  }
end

local function removeFromCurrentLists(prefix, id)
  local currentList = nil
  if removeFromWaitList(prefix, id) > 0 then
    currentList = currentList or "wait"
  end
  if removeFromDelayedList(prefix, id) > 0 then
    currentList = currentList or "scheduled"
  end
  if removeFromActiveLists(prefix, id) > 0 then
    currentList = currentList or "active"
  end
  if removeFromFailedList(prefix, id) > 0 then
    currentList = currentList or "failed"
  end
  if removeFromSuccessList(prefix, id) > 0 then
    currentList = currentList or "success"
  end
  return currentList
end

local function addToActiveLists(prefix, id, expiresAt)
  return redis.call("ZADD", activeList(prefix), expiresAt, id)
end
local function addToWaitList(prefix, id)
  return redis.call("RPUSH", waitList(prefix), id)
end
local function addToDelayedList(prefix, id, readyAt)
  return redis.call("ZADD", delayedList(prefix), readyAt, id)
end
local function addToSuccessList(prefix, id)
  return redis.call("ZADD", successList(prefix), now, id)
end
local function addToFailedList(prefix, id)
  return redis.call("ZADD", failedList(prefix), now, id)
end

local function executionState(prefix, id, list)
  if list == "wait" then return "waiting" end
  if list == "active" then return "leased" end
  if list == "success" then return "succeeded" end
  if list == "failed" then return "failed" end
  if list == "scheduled" then
    local handlerFailures = tonumber(redis.call("HGET", taskHash(prefix, id), "handlerFailureCount") or "0")
    local stalledAttempts = tonumber(redis.call("HGET", taskHash(prefix, id), "stalledAttemptCount") or "0")
    return (handlerFailures + stalledAttempts) > 0 and "retry-scheduled" or "delayed"
  end
  local outcome = redis.call("HGET", taskHash(prefix, id), "outcome")
  if outcome == "success" then return "succeeded" end
  if outcome == "failure" then return "failed" end
  return nil
end

-- the add* helpers do not clear other lists; moveToList is the single entry
-- point that removes from the current list, adds to the target, and emits task.moved
local function moveToList(prefix, id, list, readyAt)
  local currentList = removeFromCurrentLists(prefix, id)
  if list == "wait" then
    addToWaitList(prefix, id)
  elseif list == "scheduled" then
    addToDelayedList(prefix, id, readyAt)
  elseif list == "active" then
    addToActiveLists(prefix, id, readyAt)
  elseif list == "failed" then
    addToFailedList(prefix, id)
  elseif list == "success" then
    addToSuccessList(prefix, id)
  end

  -- Removing first repairs duplicate or cross-state membership. Re-add the
  -- target even when it is unchanged, then avoid publishing a fake move.
  if currentList == list then
    return
  end

  local fields = {}
  local previousState = executionState(prefix, id, currentList)
  local newState = executionState(prefix, id, list)
  if currentList then
    fields[#fields + 1] = "from"
    fields[#fields + 1] = currentList
  end
  if list then
    fields[#fields + 1] = "to"
    fields[#fields + 1] = list
  end
  if previousState then
    fields[#fields + 1] = "previousState"
    fields[#fields + 1] = previousState
  end
  if newState then
    fields[#fields + 1] = "newState"
    fields[#fields + 1] = newState
  end
  fields[#fields + 1] = "attempt"
  fields[#fields + 1] = redis.call("HGET", taskHash(prefix, id), "attempt") or "0"
  fields[#fields + 1] = "handlerFailureCount"
  fields[#fields + 1] = redis.call("HGET", taskHash(prefix, id), "handlerFailureCount") or "0"
  fields[#fields + 1] = "stalledAttemptCount"
  fields[#fields + 1] = redis.call("HGET", taskHash(prefix, id), "stalledAttemptCount") or "0"
  publishEvent(prefix, id, "task.moved", fields)
end

local function deleteTask(prefix, id)
  local generation = redis.call("HGET", taskHash(prefix, id), "generation")
  moveToList(prefix, id, nil)
  if generation then
    local member = cmsgpack.pack({ prefix, id, tonumber(generation) })
    redis.call("ZREM", taskExpiryIndex(prefix), member)
    redis.call("ZREM", terminalExpiryIndex(prefix), member)
  end
  redis.call("ZREM", createdList(prefix), id)
  return redis.call("DEL", taskHash(prefix, id))
end

local function popWaitList(prefix) return redis.call("LINDEX", waitList(prefix), 0) end

local function getExpiredActiveList(prefix)
  if maintenanceRemaining <= 0 then return {} end
  return redis.call(
    "ZRANGEBYSCORE", activeList(prefix), 0, now,
    "LIMIT", 0, maintenanceRemaining
  )
end

-- task hash -----------------------------------------------------------------

-- flat {"id", id, k1, v1, ...} entry list with raw hash values: structured
-- fields stay msgpack bytes and are decoded on the Node side only — Lua never
-- unpacks the payload, so it round-trips byte-exact (nested nulls included)
local function getTask(prefix, id)
  local fields = redis.call("HGETALL", taskHash(prefix, id))
  if #fields > 0 then
    return { "id", id, unpack(fields) }
  end
  return nil
end

local function getResult(prefix, id, generation)
  local fields = redis.call("HGETALL", resultHash(prefix, id, generation))
  if #fields > 0 then return fields end
  return nil
end

-- append a task's entries to an event field list, prefixing each key
local function appendTaskFields(fields, keyPrefix, entries)
  for i = 1, #entries, 2 do
    fields[#fields + 1] = keyPrefix .. entries[i]
    fields[#fields + 1] = entries[i + 1]
  end
end

local function getTaskField(prefix, id, field) return redis.call("HGET", taskHash(prefix, id), field) end
local function getTaskErrors(prefix, id)
  local raw = getTaskField(prefix, id, "errors")
  return raw and cmsgpack.unpack(raw) or {}
end
local function setTask(prefix, id, ...) return redis.call("HSET", taskHash(prefix, id), "updatedAt", now, ...) end
local function setTaskErrors(prefix, id, errors) return setTask(prefix, id, "errors", cmsgpack.pack(errors)) end
local function appendTaskError(prefix, id, error, retryAt)
  local errorsList = getTaskErrors(prefix, id)
  local maxErrorEntries = tonumber(getTaskField(prefix, id, "maxErrorEntries") or "100")
  while #errorsList >= maxErrorEntries and #errorsList > 0 do
    table.remove(errorsList, 1)
  end
  if maxErrorEntries > 0 then
    errorsList[#errorsList + 1] = { error = error, timestamp = now, retryAt = retryAt }
  end
  setTaskErrors(prefix, id, errorsList)
  return errorsList
end

-- locks ----------------------------------------------------------------------

local function exists(key) return redis.call("EXISTS", key) end
local function lockTask(prefix, id, leaseToken, lockTimeout)
  moveToList(prefix, id, "active", now + lockTimeout)
  redis.call("HINCRBY", taskHash(prefix, id), "attempt", 1)
  return redis.call("SET", lockHash(prefix, id), leaseToken, "PX", lockTimeout)
end
local function unlockTask(prefix, id) return redis.call("DEL", lockHash(prefix, id)) end
local function isLocked(prefix, id) return redis.call("EXISTS", lockHash(prefix, id)) > 0 end
local function getLockId(prefix, id) return redis.call("GET", lockHash(prefix, id)) end
local function isLockedBy(prefix, id, leaseToken) return getLockId(prefix, id) == leaseToken end

-- explicit result retention --------------------------------------------------

-- Relationships live in generation-keyed Redis sets. The member format is a
-- canonical msgpack tuple so SADD is idempotent without relying on map order.
local function identityMember(queue, id, generation)
  return cmsgpack.pack({ queue, id, tonumber(generation) })
end
local function decodeIdentity(member)
  local identity = cmsgpack.unpack(member)
  return {
    queue = identity[1],
    id = identity[2],
    generation = tonumber(identity[3]),
  }
end
local function currentGeneration(prefix, id)
  return tonumber(getTaskField(prefix, id, "generation"))
end
local function hasRetentionHolds(prefix, id, generation)
  return redis.call("SCARD", retentionHoldersKey(prefix, id, generation)) > 0
end

local function scheduleExpiry(index, member, retentionMs)
  redis.call("ZADD", index, now + tonumber(retentionMs), member)
end

local function persistTerminalResult(prefix, id)
  local generation = currentGeneration(prefix, id)
  local member = identityMember(prefix, id, generation)
  local outcome = getTaskField(prefix, id, "outcome")
  local hash = resultHash(prefix, id, generation)
  redis.call(
    "HSET",
    hash,
    "protocolVersion", getTaskField(prefix, id, "protocolVersion"),
    "schemaId", getTaskField(prefix, id, "schemaId"),
    "generation", generation,
    "outcome", outcome,
    "settledAt", now
  )
  if outcome == "success" then
    redis.call("HSET", hash, "success", getTaskField(prefix, id, "success"))
  else
    local errors = getTaskErrors(prefix, id)
    local failure = errors[#errors] and errors[#errors].error or nil
    if failure ~= nil then redis.call("HSET", hash, "failure", cmsgpack.pack(failure)) end
    redis.call("ZADD", deadLetterList(prefix), now, member)
    scheduleExpiry(
      deadLetterExpiryIndex(prefix),
      member,
      getTaskField(prefix, id, "deadLetterRetentionMs")
    )
  end
  scheduleExpiry(
    resultExpiryIndex(prefix),
    member,
    getTaskField(prefix, id, "resultRetentionMs")
  )
end

local function scheduleTerminalRetention(prefix, id)
  local generation = currentGeneration(prefix, id)
  local member = identityMember(prefix, id, generation)
  scheduleExpiry(
    taskExpiryIndex(prefix),
    member,
    getTaskField(prefix, id, "taskRecordRetentionMs")
  )
  local outcome = getTaskField(prefix, id, "outcome")
  local policy = outcome == "success"
    and getTaskField(prefix, id, "onSuccessPolicy")
    or getTaskField(prefix, id, "onFailurePolicy")
  if policy == "mark-as-success" or policy == "mark-as-failure" then
    scheduleExpiry(
      terminalExpiryIndex(prefix),
      member,
      getTaskField(prefix, id, "terminalIndexRetentionMs")
    )
  end
end
local function validateLiveHolder(holder)
  if not holder or not holder.queue or not holder.id or not holder.generation then
    return "invalid retention holder"
  end
  if exists(taskHash(holder.queue, holder.id)) == 0 then
    return "retention holder not found"
  end
  if currentGeneration(holder.queue, holder.id) ~= tonumber(holder.generation) then
    return "retention holder generation does not match"
  end
  if getTaskField(holder.queue, holder.id, "outcome") then
    return "retention holder is settled"
  end
  return nil
end
local function acquireRetentionHold(holder, retainedPrefix, retainedId, retainedGeneration)
  local holderMember = identityMember(holder.queue, holder.id, holder.generation)
  local retainedMember = identityMember(retainedPrefix, retainedId, retainedGeneration)
  local holdersKey = retentionHoldersKey(retainedPrefix, retainedId, retainedGeneration)
  local retainedKey = retainedTasksKey(holder.queue, holder.id, holder.generation)

  -- Replay of the same relationship remains idempotent even at the cap.
  if redis.call("SISMEMBER", holdersKey, holderMember) == 1 then return nil end

  local holderLimit = tonumber(getTaskField(holder.queue, holder.id, "maxRelationships") or "1000")
  local retainedLimit = tonumber(getTaskField(retainedPrefix, retainedId, "maxRelationships") or "1000")
  if redis.call("SCARD", retainedKey) >= holderLimit then
    return "STORAGE_RELATIONSHIP_LIMIT holder " .. holderLimit
  end
  if redis.call("SCARD", holdersKey) >= retainedLimit then
    return "STORAGE_RELATIONSHIP_LIMIT retained " .. retainedLimit
  end
  redis.call(
    "SADD",
    holdersKey,
    holderMember
  )
  redis.call(
    "SADD",
    retainedKey,
    retainedMember
  )
  return nil
end

-- Settlement is visible immediately through outcome and policy-selected
-- terminal membership. A delete policy disposes the record only after the
-- final explicit hold is gone.
local function applyCompletionPolicy(prefix, id)
  if exists(taskHash(prefix, id)) == 0 then return end
  local generation = currentGeneration(prefix, id)
  local outcome = getTaskField(prefix, id, "outcome")
  if not outcome then return end
  local policy = outcome == "success"
    and getTaskField(prefix, id, "onSuccessPolicy")
    or getTaskField(prefix, id, "onFailurePolicy")

  if policy == "mark-as-success" then
    moveToList(prefix, id, "success")
  elseif policy == "mark-as-failure" then
    moveToList(prefix, id, "failed")
  else
    moveToList(prefix, id, nil)
  end

  if policy == "delete" and not hasRetentionHolds(prefix, id, generation) then
    deleteTask(prefix, id)
  end
end

-- Release at most one bounded batch. If work remains, the generation identity
-- stays in a durable per-queue continuation index that later sync calls drain.
local function releaseOwnedHolds(holderPrefix, holderId, holderGeneration)
  local key = retainedTasksKey(holderPrefix, holderId, holderGeneration)
  local holderMember = identityMember(holderPrefix, holderId, holderGeneration)
  local retainedMembers = maintenanceRemaining > 0
    and redis.call("SPOP", key, maintenanceRemaining)
    or {}
  maintenanceRemaining = maintenanceRemaining - #retainedMembers
  for i, retainedMember in ipairs(retainedMembers) do
    local retained = decodeIdentity(retainedMember)
    redis.call(
      "SREM",
      retentionHoldersKey(retained.queue, retained.id, retained.generation),
      holderMember
    )
    if exists(taskHash(retained.queue, retained.id)) == 1
      and currentGeneration(retained.queue, retained.id) == retained.generation
      and getTaskField(retained.queue, retained.id, "outcome")
      and not hasRetentionHolds(retained.queue, retained.id, retained.generation)
    then
      applyCompletionPolicy(retained.queue, retained.id)
    end
  end

  local continuation = retentionContinuationKey(holderPrefix)
  if redis.call("SCARD", key) > 0 then
    redis.call("ZADD", continuation, now, holderMember)
  else
    redis.call("DEL", key)
    redis.call("ZREM", continuation, holderMember)
  end
end

local function settleTask(prefix, id)
  local generation = currentGeneration(prefix, id)
  persistTerminalResult(prefix, id)
  scheduleTerminalRetention(prefix, id)
  applyCompletionPolicy(prefix, id)
  releaseOwnedHolds(prefix, id, generation)
end

local function failTask(prefix, id, error, retryAt, failureKind)
  unlockTask(prefix, id)

  if failureKind == "stall" then
    redis.call("HINCRBY", taskHash(prefix, id), "stalledAttemptCount", 1)
  else
    redis.call("HINCRBY", taskHash(prefix, id), "handlerFailureCount", 1)
  end

  -- retryAt arrives as -1 (or nil) when no retry is scheduled; normalize
  -- to nil so it is omitted from the stored error and the event payload
  if (retryAt or -1) < 0 then retryAt = nil end
  appendTaskError(prefix, id, error, retryAt)
  local onFailurePolicy = getTaskField(prefix, id, "onFailurePolicy")

  local errorTag = type(error) == "table" and error._tag or nil

  local willRetry = errorTag ~= "~effectmq/Error/Canceled" and retryAt ~= nil
  local fields = {
    "error", cmsgpack.pack(error),
    "policy", onFailurePolicy,
    "failureKind", failureKind,
    "attempt", getTaskField(prefix, id, "attempt") or "0",
    "terminal", willRetry and "0" or "1",
  }
  if retryAt then
    fields[#fields + 1] = "retryAt"
    fields[#fields + 1] = retryAt
  end
  publishEvent(prefix, id, "task.failed", fields)
  if willRetry then
    if retryAt > now then
      moveToList(prefix, id, "scheduled", retryAt)
    else
      moveToList(prefix, id, "wait")
    end
    return
  end
  setTask(prefix, id, "outcome", "failure")
  settleTask(prefix, id)
end

-- sync -----------------------------------------------------------------------

local function syncLocks(prefix)
  local activeIds = getExpiredActiveList(prefix)
  maintenanceRemaining = maintenanceRemaining - #activeIds
  for i, id in ipairs(activeIds) do
    if not isLocked(prefix, id) then
      local nextStalledCount = tonumber(getTaskField(prefix, id, "stalledAttemptCount") or "0") + 1
      local maxStalledCount = tonumber(getTaskField(prefix, id, "maxStalledCount") or "1")
      local retryAt = nextStalledCount > maxStalledCount and -1 or 0
      failTask(prefix, id, {
        _tag = "~effectmq/Error/Stalled",
        timestamp = now,
      }, retryAt, "stall")
    end
  end
end

local function syncDelayed(prefix)
  -- we lazily move tasks from the delayed list to the wait list so we need to sync
  -- before performing any other operations
  if maintenanceRemaining <= 0 then return end
  local items = redis.call(
    "ZRANGEBYSCORE", delayedList(prefix), 0, now,
    "LIMIT", 0, maintenanceRemaining
  )
  maintenanceRemaining = maintenanceRemaining - #items
  for i, item in ipairs(items) do
    moveToList(prefix, item, "wait")
  end
end

local function syncRetentionContinuations(prefix)
  if maintenanceRemaining <= 0 then return end
  local holders = redis.call(
    "ZRANGE",
    retentionContinuationKey(prefix),
    0,
    0
  )
  for i, holderMember in ipairs(holders) do
    local holder = decodeIdentity(holderMember)
    releaseOwnedHolds(holder.queue, holder.id, holder.generation)
  end
end

local function dueExpiryMembers(index)
  if maintenanceRemaining <= 0 then return {} end
  return redis.call(
    "ZRANGEBYSCORE", index, 0, now,
    "LIMIT", 0, maintenanceRemaining
  )
end

local function syncTaskExpiry(prefix)
  local index = taskExpiryIndex(prefix)
  local members = dueExpiryMembers(index)
  maintenanceRemaining = maintenanceRemaining - #members
  for i, member in ipairs(members) do
    local task = decodeIdentity(member)
    if exists(taskHash(prefix, task.id)) == 1
      and currentGeneration(prefix, task.id) == task.generation
      and getTaskField(prefix, task.id, "outcome")
    then
      if hasRetentionHolds(prefix, task.id, task.generation) then
        redis.call("ZADD", index, now + 1000, member)
      else
        deleteTask(prefix, task.id)
        redis.call("DEL", retentionHoldersKey(prefix, task.id, task.generation))
        redis.call("ZREM", index, member)
      end
    else
      redis.call("ZREM", index, member)
    end
  end
end

local function syncResultExpiry(prefix)
  local index = resultExpiryIndex(prefix)
  local members = dueExpiryMembers(index)
  maintenanceRemaining = maintenanceRemaining - #members
  for i, member in ipairs(members) do
    local result = decodeIdentity(member)
    if hasRetentionHolds(prefix, result.id, result.generation) then
      redis.call("ZADD", index, now + 1000, member)
    else
      redis.call("DEL", resultHash(prefix, result.id, result.generation))
      redis.call("ZREM", index, member)
    end
  end
end

local function syncTerminalExpiry(prefix)
  local index = terminalExpiryIndex(prefix)
  local members = dueExpiryMembers(index)
  maintenanceRemaining = maintenanceRemaining - #members
  for i, member in ipairs(members) do
    local task = decodeIdentity(member)
    if exists(taskHash(prefix, task.id)) == 1
      and currentGeneration(prefix, task.id) == task.generation
    then
      removeFromSuccessList(prefix, task.id)
      removeFromFailedList(prefix, task.id)
    end
    redis.call("ZREM", index, member)
  end
end

local function syncDeadLetterExpiry(prefix)
  local index = deadLetterExpiryIndex(prefix)
  local members = dueExpiryMembers(index)
  maintenanceRemaining = maintenanceRemaining - #members
  for i, member in ipairs(members) do
    redis.call("ZREM", deadLetterList(prefix), member)
    redis.call("ZREM", index, member)
  end
end

local function syncAll(prefix)
  local syncers = {
    syncDelayed,
    syncLocks,
    syncRetentionContinuations,
    syncTaskExpiry,
    syncResultExpiry,
    syncTerminalExpiry,
    syncDeadLetterExpiry,
  }
  local cursor = tonumber(redis.call("GET", maintenanceCursorKey(prefix)) or "0")
  for offset = 0, #syncers - 1 do
    if maintenanceRemaining <= 0 then break end
    local index = ((cursor + offset) % #syncers) + 1
    syncers[index](prefix)
  end
  redis.call("SET", maintenanceCursorKey(prefix), (cursor + 1) % #syncers)
end

local function firstScore(key)
  local entry = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
  return entry[2] and tonumber(entry[2]) or nil
end

local function maintenanceSnapshot(prefix)
  local depth = redis.call("LLEN", waitList(prefix))
    + redis.call("ZCARD", delayedList(prefix))
    + redis.call("ZCARD", activeList(prefix))
  local oldestCreated = firstScore(createdList(prefix))
  local oldestAge = oldestCreated and math.max(0, now - oldestCreated) or 0
  local dueBacklog = redis.call("ZCOUNT", delayedList(prefix), 0, now)
  local expiredLeases = redis.call("ZCOUNT", activeList(prefix), 0, now)
  local retentionBacklog = redis.call("ZCOUNT", taskExpiryIndex(prefix), 0, now)
    + redis.call("ZCOUNT", resultExpiryIndex(prefix), 0, now)
    + redis.call("ZCOUNT", terminalExpiryIndex(prefix), 0, now)
    + redis.call("ZCOUNT", deadLetterExpiryIndex(prefix), 0, now)
  local oldestDue = nil
  local dueIndexes = {
    delayedList(prefix), activeList(prefix), taskExpiryIndex(prefix),
    resultExpiryIndex(prefix), terminalExpiryIndex(prefix),
    deadLetterExpiryIndex(prefix), retentionContinuationKey(prefix),
  }
  for i, index in ipairs(dueIndexes) do
    local score = firstScore(index)
    if score and score <= now and (not oldestDue or score < oldestDue) then
      oldestDue = score
    end
  end
  return {
    depth,
    oldestAge,
    oldestDue and math.max(0, now - oldestDue) or 0,
    dueBacklog,
    expiredLeases,
    retentionBacklog,
    maintenanceBatchSize - maintenanceRemaining,
  }
end

-- entry points ---------------------------------------------------------------

local operations = {}
local function register(name, fn)
  operations[name] = fn
end

register("effectmq_createTask", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local throwOnExists = args[3] == "1"

  local id = args[4]
  local name = args[5]
  local payload = args[6]
  local delay = tonumber(args[7])
  local maxRetries = tonumber(args[8])
  local onSuccessPolicy = args[9]
  local onFailurePolicy = args[10]
  -- Relationship identities arrive with fully-qualified queue prefixes.
  local retentionHolder = args[11] ~= "" and cmsgpack.unpack(args[11]) or nil
  local creator = args[12]
  local onDuplicate = args[13] or "return-existing"
  local maxStalledCount = tonumber(args[14]) or 1
  local maxErrorEntries = tonumber(args[16]) or 100
  local maxRelationships = tonumber(args[17]) or 1000
  local maxEventEntries = tonumber(args[18]) or 10000
  local taskRecordRetentionMs = tonumber(args[19]) or 604800000
  local resultRetentionMs = tonumber(args[20]) or 86400000
  local terminalIndexRetentionMs = tonumber(args[21]) or 604800000
  local deadLetterRetentionMs = tonumber(args[22]) or 2592000000
  local eventRetentionMs = tonumber(args[23]) or 604800000
  local existingTask = getTask(prefix, id)
  local replacedTask = nil

  if retentionHolder then
    local holderError = validateLiveHolder(retentionHolder)
    if holderError then return redis.error_reply(holderError) end
  end

  if existingTask ~= nil then
    if throwOnExists then
      return redis.error_reply("task already exists")
    end
    if onDuplicate == "return-existing" then
      if retentionHolder then
        local retentionError = acquireRetentionHold(
          retentionHolder,
          prefix,
          id,
          currentGeneration(prefix, id)
        )
        if retentionError then return redis.error_reply(retentionError) end
      end
      return { "existing", latestEventCursor(prefix), existingTask }
    end
    if onDuplicate ~= "new-generation" then
      return redis.error_reply("invalid duplicate mode")
    end
    if not getTaskField(prefix, id, "outcome") then
      return redis.error_reply("cannot create a new generation for a non-terminal task")
    end
    if hasRetentionHolds(prefix, id, currentGeneration(prefix, id)) then
      return redis.error_reply("cannot replace a retained task generation")
    end
    replacedTask = existingTask
    deleteTask(prefix, id)
    unlockTask(prefix, id)
    existingTask = nil
  end

  local generation = redis.call("HINCRBY", generationHash(prefix), id, 1)
  setTask(
    prefix, id,
    "generation", generation,
    "protocolVersion", 1,
    "schemaId", args[15] or name,
    "name", name,
    "createdAt", now,
    "payload", payload,
    "delay", delay,
    "maxRetries", maxRetries,
    "maxStalledCount", maxStalledCount,
    "maxErrorEntries", maxErrorEntries,
    "maxRelationships", maxRelationships,
    "maxEventEntries", maxEventEntries,
    "taskRecordRetentionMs", taskRecordRetentionMs,
    "resultRetentionMs", resultRetentionMs,
    "terminalIndexRetentionMs", terminalIndexRetentionMs,
    "deadLetterRetentionMs", deadLetterRetentionMs,
    "eventRetentionMs", eventRetentionMs,
    "attempt", 0,
    "handlerFailureCount", 0,
    "stalledAttemptCount", 0,
    "onSuccessPolicy", onSuccessPolicy,
    "onFailurePolicy", onFailurePolicy,
    "errors", EMPTY_LIST
  )
  redis.call("ZADD", createdList(prefix), now, id)
  if creator ~= "" then
    setTask(prefix, id, "creator", creator)
  end
  if retentionHolder then
    local retentionError = acquireRetentionHold(retentionHolder, prefix, id, generation)
    if retentionError then
      deleteTask(prefix, id)
      if redis.call("HINCRBY", generationHash(prefix), id, -1) == 0 then
        redis.call("HDEL", generationHash(prefix), id)
      end
      return redis.error_reply(retentionError)
    end
  end
  local newTask = getTask(prefix, id)
  local fields = {}
  if replacedTask then appendTaskFields(fields, "existing:", replacedTask) end
  appendTaskFields(fields, "new:", newTask)
  fields[#fields + 1] = "state"
  fields[#fields + 1] = delay > 0 and "delayed" or "waiting"
  publishEvent(prefix, id, replacedTask and "task.updated" or "task.created", fields)

  if delay > 0 then
    moveToList(prefix, id, "scheduled", now + delay)
  else
    moveToList(prefix, id, "wait")
  end

  return { "created", latestEventCursor(prefix), getTask(prefix, id) }
end)

register("effectmq_getTask", function(args)
  local prefix = args[2]
  local id = args[3]
  return getTask(prefix, id)
end)

register("effectmq_getGeneration", function(args)
  return tonumber(redis.call("HGET", generationHash(args[2]), args[3]) or "0")
end)

register("effectmq_getResult", function(args)
  return getResult(args[2], args[3], tonumber(args[4]))
end)

register("effectmq_maintain", function(args)
  syncAll(args[2])
  return maintenanceSnapshot(args[2])
end)

register("effectmq_eventCursors", function(args)
  return eventCursors(args[2])
end)

register("effectmq_writeSuccess", function(args)
  local prefix = args[2]
  local leaseToken = args[3]
  local id = args[4]
  local result = args[5]
  local hash = taskHash(prefix, id)

  syncAll(prefix)

  if exists(hash) == 0 then
    return redis.error_reply("Task not found")
  end
  if not isLockedBy(prefix, id, leaseToken) then
    return redis.error_reply("LEASE_LOST")
  end
  unlockTask(prefix, id)
  setTask(prefix, id, "success", result, "outcome", "success")
  local successPolicy = getTaskField(prefix, id, "onSuccessPolicy")

  -- result stays raw msgpack bytes end-to-end; publish it as its own field
  publishEvent(prefix, id, "task.completed", { "success", result, "policy", successPolicy })

  settleTask(prefix, id)
  return
end)

register("effectmq_writeError", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local leaseToken = args[3]
  local id = args[4]
  local error = cmsgpack.unpack(args[5])
  local retryAt = tonumber(args[6]) or -1
  local hash = taskHash(prefix, id)

  if exists(hash) == 0 then
    return redis.error_reply("Task not found")
  end
  if not isLockedBy(prefix, id, leaseToken) then
    return redis.error_reply("LEASE_LOST")
  end

  failTask(prefix, id, error, retryAt, "handler")
  return
end)

register("effectmq_removeTask", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local id = args[3]

  local lock = getLockId(prefix, id)

  if lock then
    return redis.error_reply("cannot remove a leased task")
  end
  if exists(taskHash(prefix, id)) == 0 then return end
  local generation = currentGeneration(prefix, id)
  if hasRetentionHolds(prefix, id, generation) then
    return redis.error_reply("task has active retention holds")
  end
  releaseOwnedHolds(prefix, id, generation)
  deleteTask(prefix, id)
  return
end)

register("effectmq_forceRemoveTask", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local id = args[3]
  if exists(taskHash(prefix, id)) == 0 then return end
  local generation = currentGeneration(prefix, id)

  -- Administrative removal may revoke the current attempt and inbound result
  -- holds. Holder-side set entries are harmless tombstones and are removed in
  -- bounded batches when those holders settle or are removed.
  unlockTask(prefix, id)
  releaseOwnedHolds(prefix, id, generation)
  redis.call("DEL", retentionHoldersKey(prefix, id, generation))
  local member = identityMember(prefix, id, generation)
  redis.call("DEL", resultHash(prefix, id, generation))
  redis.call("ZREM", taskExpiryIndex(prefix), member)
  redis.call("ZREM", resultExpiryIndex(prefix), member)
  redis.call("ZREM", terminalExpiryIndex(prefix), member)
  redis.call("ZREM", deadLetterList(prefix), member)
  redis.call("ZREM", deadLetterExpiryIndex(prefix), member)
  deleteTask(prefix, id)
  return
end)

register("effectmq_takeTask", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local leaseToken = args[3]
  local lockTimeout = tonumber(args[4])

  local taskId = popWaitList(prefix)

  -- LINDEX returns false (not nil) on an empty list
  if not taskId then
    return nil
  end
  -- sanity check: tasks on wait list should never be locked, but just in case
  if isLocked(prefix, taskId) then
    moveToList(prefix, taskId, "active", now + lockTimeout)
    return redis.error_reply("Task is locked by another worker")
  end

  local lock = lockTask(prefix, taskId, leaseToken, lockTimeout)
  if lock == nil then
    return nil
  end
  return { leaseToken, getTask(prefix, taskId) }
end)

register("effectmq_extendLock", function(args)
  local prefix = args[2]
  local leaseToken = args[3]
  local id = args[4]
  local lockTimeout = tonumber(args[5])
  local lock = getLockId(prefix, id)
  if not lock or lock ~= leaseToken then
    return redis.error_reply("LEASE_LOST")
  end
  redis.call("PEXPIRE", lockHash(prefix, id), lockTimeout)
  -- Renewal is a same-state transition, but still runs through the canonical
  -- mover so a partially corrupt cross-index record is repaired atomically.
  moveToList(prefix, id, "active", now + lockTimeout)
  return
end)

register("effectmq_removeLock", function(args)
  local prefix = args[2]
  local leaseToken = args[3]
  local id = args[4]
  local lock = getLockId(prefix, id)
  if not lock or lock ~= leaseToken then
    return redis.error_reply("LEASE_LOST")
  end
  unlockTask(prefix, id)
  -- A voluntary release is not a crash. Return the exact owned attempt to
  -- wait immediately without consuming the task's stalled-attempt budget.
  moveToList(prefix, id, "wait")
  return
end)

register("effectmq_setSchedule", function(args)
  local prefix = args[2]
  local name = args[3]
  local next = tonumber(args[4])
  local hash = scheduleHash(prefix, name)
  redis.call("HSETNX", hash, "next", next)

  return tonumber(redis.call("HGET", hash, "next"))
end)

register("effectmq_consumeSchedule", function(args)
  local prefix = args[2]
  local name = args[3]
  local currentToConsume = tonumber(args[4])
  local nextToSet = tonumber(args[5])
  local hash = scheduleHash(prefix, name)

  -- consumed is reported as 1/0 rather than a boolean: Redis converts a
  -- Lua false to a null reply, which truncates the returned array
  local currentSchedule = tonumber(redis.call("HGET", hash, "next"))
  -- if schedule is not set, we return nil
  if not currentSchedule then
    return { 0 }
  end

  -- if the expected current schedule is not equal to the current schedule,
  -- we assume the "next" schedule has been calculated relative to the wrong time
  -- so we discard it and send the acual current schedule so the worker can use it to try again
  if currentSchedule ~= currentToConsume then
    return { 0, currentSchedule }
  end

  -- if the current schedule match, but the vent is still in the future, we also discard it
  if now < currentSchedule then
    return { 0, currentSchedule }
  end

  -- if the event is in the past, we can consume it and schedule the next event
  if currentSchedule < nextToSet then
    redis.call("HSET", hash, "next", nextToSet)
    return { 1, nextToSet }
  end

  return { 0, currentSchedule }
end)

register("effectmq_listTasks", function(args)
  local prefix = args[2]
  syncAll(prefix)
  local list = args[3]
  local offset = tonumber(args[4]) or 0
  local limit = tonumber(args[5]) or 100
  local items
  if list == "scheduled" then
    items = redis.call("ZRANGE", delayedList(prefix), offset, offset + limit)
  elseif list == "wait" then
    items = redis.call("LRANGE", waitList(prefix), offset, offset + limit)
  elseif list == "active" then
    items = redis.call("ZRANGE", activeList(prefix), offset, offset + limit)
  elseif list == "failed" then
    items = redis.call("ZRANGE", failedList(prefix), offset, offset + limit)
  elseif list == "success" then
    items = redis.call("ZRANGE", successList(prefix), offset, offset + limit)
  else
    return redis.error_reply("Invalid list")
  end
  local hasMore = #items > limit
  if hasMore then table.remove(items, #items) end
  local result = { hasMore and tostring(offset + limit) or "" }
  for i, item in ipairs(items) do result[#result + 1] = item end
  return result
end)

local operation = ARGV[1]
local fn = operations[operation]
if not fn then
  return redis.error_reply("Unknown effectmq operation: " .. tostring(operation))
end

-- Preserve the operation-local layout while the universal maintenance batch
-- travels beside the debug flag: args[1] is debug, args[2...] are operation
-- arguments, and ARGV[3] never leaks into an operation.
local args = { ARGV[2] }
for i = 4, #ARGV do
  args[#args + 1] = ARGV[i]
end
-- cmsgpack.pack({}) encodes an ambiguous empty map. 0x90 is the canonical
-- MessagePack empty-array representation and must fail if read as another type.
EMPTY_LIST = string.char(0x90)
now = getNow(args[1])
maintenanceBatchSize = tonumber(ARGV[3]) or 100
maintenanceRemaining = maintenanceBatchSize
return fn(args)
