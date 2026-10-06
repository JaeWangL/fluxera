-- Fence every attempt mutation, including retry enqueue and dead-letter writes.
local streamKey, payloadKey, refKey = KEYS[1], KEYS[2], KEYS[3]
local groupName, transportId = ARGV[1], ARGV[2]
local pending = redis.call('XPENDING', streamKey, groupName, transportId, transportId, 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[3]
   or tonumber(pending[1][4]) ~= tonumber(ARGV[4]) then
    return 0
end
local mode, messageId = ARGV[5], ARGV[6]
if mode == 'retry' then
    local ttl = tonumber(ARGV[8])
    -- Keep the original reference until its successor has been enqueued.
    if redis.call('EXISTS', refKey) == 0 then redis.call('SET', refKey, 1, 'PX', ttl) end
    redis.call('SET', payloadKey, ARGV[7], 'PX', ttl)
    redis.call('INCR', refKey)
    redis.call('PEXPIRE', refKey, ttl)
    redis.call('SADD', KEYS[6], ARGV[10])
    if tonumber(ARGV[9]) > 0 then
        redis.call('ZADD', KEYS[5], ARGV[9], messageId)
    else
        redis.call('XADD', streamKey, '*', 'message_id', messageId)
    end
elseif mode == 'reject' then
    redis.call('SET', KEYS[7], ARGV[12], 'PX', ARGV[13])
    redis.call('ZADD', KEYS[8], ARGV[14], ARGV[11])
    redis.call('PEXPIRE', KEYS[8], ARGV[13])
end
redis.call('XACK', streamKey, groupName, transportId)
local deleted = redis.call('XDEL', streamKey, transportId)
if deleted > 0 then
    local refs = redis.call('GET', refKey)
    if not refs then
        redis.call('DEL', payloadKey)
    elseif redis.call('DECR', refKey) <= 0 then
        redis.call('DEL', payloadKey, refKey)
    end
end
if mode ~= 'retry' and mode ~= 'ack_for_retry' and KEYS[4] ~= ''
   and redis.call('GET', KEYS[4]) == messageId then
    redis.call('DEL', KEYS[4])
end
return 1
