local streamKey = KEYS[1]
local payloadKey = KEYS[2]
local payloadRefKey = KEYS[3]

local groupName = ARGV[1]
local transportId = ARGV[2]

local acknowledged = redis.call("XACK", streamKey, groupName, transportId)
local deleted = redis.call("XDEL", streamKey, transportId)
local payloadDeleted = 0
local remainingRefs = -1

if deleted > 0 then
    local refs = redis.call("GET", payloadRefKey)
    if not refs then
        payloadDeleted = redis.call("DEL", payloadKey)
    else
        remainingRefs = redis.call("DECR", payloadRefKey)
        if remainingRefs <= 0 then
            payloadDeleted = redis.call("DEL", payloadKey)
            redis.call("DEL", payloadRefKey)
            remainingRefs = 0
        end
    end
end

return {acknowledged, deleted, payloadDeleted, remainingRefs}
