local registryKey = KEYS[1]
local queueName = ARGV[1]

for index = 2, #KEYS do
    if redis.call("EXISTS", KEYS[index]) == 1 then
        return 0
    end
end

return redis.call("SREM", registryKey, queueName)
