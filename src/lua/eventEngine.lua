-- Durable application events. All keys belong to the queue's Redis hash slot.
local root = KEYS[1]
local op = ARGV[1]
local policyJson = ARGV[2]
local input = cjson.decode(ARGV[3])
local batch = tonumber(ARGV[4])
local policy = cjson.decode(policyJson)
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local null = cjson.null
local subscriptions = root .. ':subscriptions'
local removed = root .. ':removed'
local retired = root .. ':retired'
local deadlines = root .. ':deadlines'
local archives = root .. ':archives'
local archiveDeadlines = root .. ':archive-deadlines'

local function ok(value) return cjson.encode({ ok = true, value = value }) end
local function fail(code, message) return cjson.encode({ ok = false, code = code, message = message }) end
local function recordKey(id) return root .. ':event:' .. id end
local function deliveryKey(generation) return root .. ':delivery:' .. generation end
local function save(record) redis.call('SET', recordKey(record.id), cjson.encode(record)) end
local function load(id)
  local raw = redis.call('GET', recordKey(id))
  if not raw then return nil end
  local decoded, record = pcall(cjson.decode, raw)
  if not decoded or type(record) ~= 'table' then error('EVENT_CORRUPT: Invalid event JSON') end
  if record.version ~= 1 or record.id ~= id or type(record.recipients) ~= 'table'
    or type(record.payload) ~= 'string' or type(record.createdAt) ~= 'number'
    or type(record.resolvedAt) ~= 'number' or type(record.queue) ~= 'string'
    or (record.settledAt ~= null and type(record.settledAt) ~= 'number')
    or (record.archiveUntil ~= null and type(record.archiveUntil) ~= 'number')
    or (record.status ~= 'active' and record.status ~= 'completed' and record.status ~= 'expired')
    or (record.expiresAt ~= null and type(record.expiresAt) ~= 'number') then
    error('EVENT_CORRUPT: Invalid event record')
  end
  for generation, recipient in pairs(record.recipients) do
    if type(recipient) ~= 'table' or recipient.generation ~= generation
      or type(recipient.name) ~= 'string'
      or (recipient.leaseToken ~= null and type(recipient.leaseToken) ~= 'string')
      or (recipient.leaseUntil ~= null and type(recipient.leaseUntil) ~= 'number')
      or (recipient.status ~= 'pending' and recipient.status ~= 'acknowledged' and recipient.status ~= 'waived') then
      error('EVENT_CORRUPT: Invalid event recipient')
    end
  end
  return record
end
local function purge(record)
  for generation, _ in pairs(record.recipients) do
    redis.call('ZREM', deliveryKey(generation), record.id)
  end
  redis.call('DEL', recordKey(record.id))
  redis.call('ZREM', deadlines, record.id)
  redis.call('ZREM', archives, record.id)
  redis.call('ZREM', archiveDeadlines, record.id)
end
local function settle(record, status, at)
  record.status = status
  record.settledAt = at
  for generation, recipient in pairs(record.recipients) do
    redis.call('ZREM', deliveryKey(generation), record.id)
    recipient.leaseUntil = null
  end
  redis.call('ZREM', deadlines, record.id)
  if policy.onCompletion == 'delete' then
    purge(record)
  else
    if policy.archiveRetentionMs ~= null then
      record.archiveUntil = at + policy.archiveRetentionMs
    end
    if record.archiveUntil ~= null and record.archiveUntil <= now then
      purge(record)
    else
      save(record)
      redis.call('ZADD', archives, at, record.id)
      if record.archiveUntil ~= null then
        redis.call('ZADD', archiveDeadlines, record.archiveUntil, record.id)
      end
    end
  end
end
local function normalize(record)
  if record.status ~= 'active' then
    if record.archiveUntil ~= null and record.archiveUntil <= now then purge(record) end
    return
  end
  local pending = 0
  local changed = false
  for generation, recipient in pairs(record.recipients) do
    if recipient.status == 'pending' then
      local removedAt = tonumber(redis.call('HGET', removed, generation))
      if removedAt and (record.expiresAt == null or removedAt < record.expiresAt) then
        recipient.status = 'waived'
        recipient.leaseToken = null
        recipient.leaseUntil = null
        record.resolvedAt = math.max(record.resolvedAt, removedAt)
        redis.call('ZREM', deliveryKey(generation), record.id)
        changed = true
      else
        pending = pending + 1
      end
    end
  end
  if pending == 0 then
    settle(record, 'completed', record.resolvedAt)
  elseif record.expiresAt ~= null and record.expiresAt <= now then
    settle(record, 'expired', record.expiresAt)
  elseif changed then
    save(record)
  end
end
local function current(id)
  local record = load(id)
  if record then
    normalize(record)
    if redis.call('EXISTS', recordKey(id)) == 0 then return nil end
  end
  return record
end
local function registered()
  return redis.call('HGET', subscriptions, input.name) == input.generation
end
local function maintain()
  local processed = 0
  -- Allocate a budget to each kind, so a removal backlog cannot starve expiry.
  for _, id in ipairs(redis.call('ZRANGEBYSCORE', deadlines, '-inf', now, 'LIMIT', 0, batch)) do
    local record = load(id)
    if record then normalize(record) else redis.call('ZREM', deadlines, id) end
    processed = processed + 1
  end
  for _, id in ipairs(redis.call('ZRANGEBYSCORE', archiveDeadlines, '-inf', now, 'LIMIT', 0, batch)) do
    local record = load(id)
    if record then purge(record) else redis.call('ZREM', archiveDeadlines, id); redis.call('ZREM', archives, id) end
    processed = processed + 1
  end
  local remaining = batch
  for _, generation in ipairs(redis.call('ZRANGE', retired, 0, batch - 1)) do
    if remaining <= 0 then break end
    local ids = redis.call('ZRANGE', deliveryKey(generation), 0, remaining - 1)
    for _, id in ipairs(ids) do
      local record = load(id)
      if record then normalize(record) end
      -- A terminal event or a waiver no longer needs this generation's index.
      redis.call('ZREM', deliveryKey(generation), id)
      remaining = remaining - 1
      processed = processed + 1
    end
    if #ids == 0 then remaining = remaining - 1 end
    if redis.call('ZCARD', deliveryKey(generation)) == 0 then
      redis.call('ZREM', retired, generation)
      redis.call('HDEL', removed, generation)
    end
  end
  return { processed = processed, pending = redis.call('ZCARD', retired) > 0
    or redis.call('ZCOUNT', deadlines, '-inf', now) > 0
    or redis.call('ZCOUNT', archiveDeadlines, '-inf', now) > 0 }
end

local function dispatch()
  local existingPolicy = redis.call('GET', root .. ':config')
  if existingPolicy and existingPolicy ~= policyJson then
    return fail('ConfigurationConflict', 'Queue already exists with a different policy')
  end
  if not existingPolicy then redis.call('SET', root .. ':config', policyJson) end

  if op == 'subscribe' then
    local generation = redis.call('HGET', subscriptions, input.name)
    if not generation then
      if redis.call('HLEN', subscriptions) >= 1000 then return fail('CapacityExceeded', 'Queue has 1000 active subscriptions') end
      generation = input.generation
      redis.call('HSET', subscriptions, input.name, generation)
    end
    return ok({ name = input.name, generation = generation })
  elseif op == 'unsubscribe' then
    if not registered() then return ok(false) end
    redis.call('HDEL', subscriptions, input.name)
    redis.call('HSET', removed, input.generation, now)
    redis.call('ZADD', retired, now, input.generation)
    maintain()
    return ok(true)
  elseif op == 'emit' then
    if redis.call('EXISTS', recordKey(input.id)) == 1 then return fail('InvalidInput', 'Event id already exists') end
    local record = { version = 1, id = input.id, queue = input.queue, payload = input.payload,
      createdAt = now, resolvedAt = now, expiresAt = null, settledAt = null, archiveUntil = null,
      status = 'active', recipients = {} }
    if input.ttlMs ~= null then record.expiresAt = now + input.ttlMs end
    local members = redis.call('HGETALL', subscriptions)
    for i = 1, #members, 2 do
      local name, generation = members[i], members[i + 1]
      record.recipients[generation] = { name = name, generation = generation, status = 'pending', leaseToken = null, leaseUntil = null }
      redis.call('ZADD', deliveryKey(generation), now, input.id)
    end
    save(record)
    if record.expiresAt ~= null then redis.call('ZADD', deadlines, record.expiresAt, input.id) end
    normalize(record)
    return ok(record)
  elseif op == 'get' then
    return ok(current(input.id) or null)
  elseif op == 'take' then
    if not registered() then return fail('SubscriptionMissing', 'Subscription generation is no longer registered') end
    for _, id in ipairs(redis.call('ZRANGEBYSCORE', deliveryKey(input.generation), '-inf', now, 'LIMIT', 0, batch)) do
      local record = current(id)
      if record and record.status == 'active' then
        local recipient = record.recipients[input.generation]
        if recipient and recipient.status == 'pending' then
          recipient.leaseToken = input.token
          recipient.leaseUntil = now + input.leaseMs
          if record.expiresAt ~= null then recipient.leaseUntil = math.min(recipient.leaseUntil, record.expiresAt) end
          redis.call('ZADD', deliveryKey(input.generation), recipient.leaseUntil, id)
          save(record)
          return ok({ event = record, leaseToken = input.token })
        end
      end
      redis.call('ZREM', deliveryKey(input.generation), id)
    end
    return ok(null)
  elseif op == 'acknowledge' or op == 'renew' or op == 'release' then
    local record = current(input.id)
    if not record then
      if op == 'acknowledge' then return ok('gone') end
      return fail('LeaseLost', 'Event is no longer retained')
    end
    local recipient = record.recipients[input.generation]
    if not recipient or recipient.name ~= input.name or recipient.leaseToken ~= input.token then
      return fail('LeaseLost', 'Delivery token is not current')
    end
    if op == 'acknowledge' and recipient.status == 'acknowledged' then return ok('already-acknowledged') end
    if record.status ~= 'active' then return fail('EventNotActive', 'Event has settled') end
    if recipient.status ~= 'pending' or recipient.leaseUntil == null or recipient.leaseUntil <= now or not registered() then
      return fail('LeaseLost', 'Delivery lease has expired or subscription was removed')
    end
    if op == 'acknowledge' then
      recipient.status = 'acknowledged'
      recipient.leaseUntil = null
      record.resolvedAt = now
      redis.call('ZREM', deliveryKey(input.generation), input.id)
      save(record)
      normalize(record)
      return ok('acknowledged')
    elseif op == 'renew' then
      recipient.leaseUntil = now + input.leaseMs
      if record.expiresAt ~= null then recipient.leaseUntil = math.min(recipient.leaseUntil, record.expiresAt) end
      redis.call('ZADD', deliveryKey(input.generation), recipient.leaseUntil, input.id)
    else
      recipient.leaseToken = null
      recipient.leaseUntil = null
      redis.call('ZADD', deliveryKey(input.generation), now + input.delayMs, input.id)
    end
    save(record)
    return ok(true)
  elseif op == 'maintain' then
    return ok(maintain())
  elseif op == 'listArchived' then
    maintain()
    local minimum = '-inf'
    if policy.archiveRetentionMs ~= null then minimum = '(' .. tostring(now - policy.archiveRetentionMs) end
    local ids = redis.call('ZRANGEBYSCORE', archives, minimum, '+inf', 'LIMIT', input.offset, input.limit)
    -- cjson encodes empty Lua tables as objects; preserve the array reply explicitly.
    if #ids == 0 then return '{"ok":true,"value":[]}' end
    return ok(ids)
  end
  return fail('InvalidInput', 'Unknown event operation')

end
local success, result = pcall(dispatch)
if success then return result end
if string.find(tostring(result), 'EVENT_CORRUPT:', 1, true) then
  return fail('CorruptStorage', tostring(result))
end
-- Preserve unexpected script failures as errors; writes before them may have committed.
return redis.error_reply(tostring(result))
