local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 1 and pending[1][2] == ARGV[3]
   and tonumber(pending[1][4]) == tonumber(ARGV[4]) then
    return 1
end
return 0
