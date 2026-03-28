local rcall = redis.call

local delayedKey = KEYS[1]
local streamKey = KEYS[2]

local nowMs = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local messageIds = rcall("ZRANGEBYSCORE", delayedKey, "-inf", nowMs, "LIMIT", 0, limit)
local promoted = 0

for _, messageId in ipairs(messageIds) do
    if rcall("ZREM", delayedKey, messageId) > 0 then
        rcall("XADD", streamKey, "*", "message_id", messageId)
        promoted = promoted + 1
    end
end

return promoted
