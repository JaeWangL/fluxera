-- Inspect and claim in one transaction, excluding acquisitions still queued or
-- executing in this consumer. XAUTOCLAIM followed by an in-process filter would
-- already have changed their ownership/generation before that filter ran.
local excluded = {}
for i = 7, #ARGV do excluded[ARGV[i]] = true end
local start = ARGV[4] == '0-0' and '-' or '(' .. ARGV[4]
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], 'IDLE', ARGV[3], start, '+', ARGV[6])
local entries = {}
local cursor = '0-0'
for _, row in ipairs(pending) do
    cursor = row[1]
    if not excluded[row[1]] then
        local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], row[1])
        -- Redis 6.2 returns a nil entry for an XDEL'ed pending ID; Redis 7
        -- removes that PEL row and returns no entries. Advance past both.
        if #claimed == 1 and claimed[1] then
            local current = redis.call('XPENDING', KEYS[1], ARGV[1], row[1], row[1], 1)
            table.insert(entries, {claimed[1][1], claimed[1][2], current[1][4]})
            if #entries >= tonumber(ARGV[5]) then return {cursor, entries} end
        end
    end
end
if #pending < tonumber(ARGV[6]) then cursor = '0-0' end
return {cursor, entries}
