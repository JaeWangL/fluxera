-- Check and renew the entire batch without a check/claim race. Never claim a
-- delivery belonging to somebody else or a newer acquisition by this consumer.
local accepted = {}
local ids = {}
local generations = {}
for i = 3, #ARGV, 2 do
    local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[i], ARGV[i], 1)
    if #pending == 1 and pending[1][2] == ARGV[2]
       and tonumber(pending[1][4]) == tonumber(ARGV[i + 1]) then
        table.insert(ids, ARGV[i])
        generations[ARGV[i]] = ARGV[i + 1]
    end
end
if #ids > 0 then
    local command = {'XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0}
    for _, id in ipairs(ids) do table.insert(command, id) end
    table.insert(command, 'IDLE'); table.insert(command, 0); table.insert(command, 'JUSTID')
    local renewed = redis.call(unpack(command))
    for _, id in ipairs(renewed) do
        table.insert(accepted, {id, generations[id]})
    end
end
return accepted
