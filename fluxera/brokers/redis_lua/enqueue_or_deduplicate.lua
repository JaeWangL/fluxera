local rcall = redis.call

local messageKeyPrefix = KEYS[1]
local streamKey = KEYS[2]
local delayedKey = KEYS[3]
local dedupeKey = KEYS[4]
local queueRegistryKey = KEYS[5]
local messageRefKeyPrefix = KEYS[6]

local messageId = ARGV[1]
local encodedMessage = ARGV[2]
local nowMs = tonumber(ARGV[3])
local deliverAtMs = tonumber(ARGV[4])
local mode = ARGV[5]
local ttlMs = tonumber(ARGV[6])
local extend = tonumber(ARGV[7])
local replace = tonumber(ARGV[8])
local messageTtlMs = tonumber(ARGV[9])
local queueName = ARGV[10]

rcall("SADD", queueRegistryKey, queueName)

local function messageKey(message_id)
    return messageKeyPrefix .. message_id
end

local function messageRefKey(message_id)
    return messageRefKeyPrefix .. message_id
end

local function storeMessage()
    local payloadKey = messageKey(messageId)
    local refKey = messageRefKey(messageId)
    if rcall("EXISTS", payloadKey) == 1 and rcall("EXISTS", refKey) == 0 then
        rcall("SET", refKey, 1, "PX", messageTtlMs)
    end
    rcall("SET", payloadKey, encodedMessage, "PX", messageTtlMs)
    rcall("INCR", refKey)
    rcall("PEXPIRE", refKey, messageTtlMs)
end

local function releaseMessage(message_id)
    local payloadKey = messageKey(message_id)
    local refKey = messageRefKey(message_id)
    local refs = rcall("GET", refKey)
    if not refs then
        rcall("DEL", payloadKey)
        return
    end

    local remaining = rcall("DECR", refKey)
    if remaining <= 0 then
        rcall("DEL", payloadKey, refKey)
    end
end

local function enqueueMessage()
    rcall("XADD", streamKey, "*", "message_id", messageId)
end

local function scheduleMessage()
    rcall("ZADD", delayedKey, deliverAtMs, messageId)
end

if mode == "none" then
    storeMessage()
    if deliverAtMs <= nowMs then
        enqueueMessage()
        return {"enqueued", messageId}
    end
    scheduleMessage()
    return {"scheduled", messageId}
end

if mode == "simple" then
    local existing = rcall("GET", dedupeKey)
    if existing then
        return {"deduplicated", existing}
    end
    rcall("SET", dedupeKey, messageId)
    storeMessage()
    if deliverAtMs <= nowMs then
        enqueueMessage()
        return {"enqueued", messageId}
    end
    scheduleMessage()
    return {"scheduled", messageId}
end

if mode == "throttle" then
    local accepted = rcall("SET", dedupeKey, messageId, "PX", ttlMs, "NX")
    if not accepted then
        local existing = rcall("GET", dedupeKey)
        return {"deduplicated", existing}
    end
    storeMessage()
    if deliverAtMs <= nowMs then
        enqueueMessage()
        return {"enqueued", messageId}
    end
    scheduleMessage()
    return {"scheduled", messageId}
end

if mode == "debounce" then
    if deliverAtMs <= nowMs then
        return {"error", "debounce_requires_delay"}
    end

    if replace ~= 1 then
        return {"error", "replace_requires_debounce"}
    end

    local existing = rcall("GET", dedupeKey)
    if existing then
        rcall("ZREM", delayedKey, existing)
        releaseMessage(existing)
        if ttlMs > 0 then
            if extend == 1 then
                rcall("SET", dedupeKey, messageId, "PX", ttlMs)
            else
                rcall("SET", dedupeKey, messageId, "KEEPTTL")
            end
        else
            rcall("SET", dedupeKey, messageId)
        end
        storeMessage()
        scheduleMessage()
        return {"replaced", messageId, existing}
    end

    if ttlMs > 0 then
        rcall("SET", dedupeKey, messageId, "PX", ttlMs)
    else
        rcall("SET", dedupeKey, messageId)
    end
    storeMessage()
    scheduleMessage()
    return {"scheduled", messageId}
end

return {"error", "invalid_mode"}
