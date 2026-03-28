local rcall = redis.call

local dedupeKey = KEYS[1]
local messageId = ARGV[1]

local existing = rcall("GET", dedupeKey)
if existing and existing == messageId then
    return rcall("DEL", dedupeKey)
end

return 0
