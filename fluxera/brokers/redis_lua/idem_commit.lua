local rcall = redis.call

local idemKey = KEYS[1]

local owner = ARGV[1]
local fence = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
local resultRef = ARGV[5]
local resultDigest = ARGV[6]

local status = rcall("HGET", idemKey, "status")
if not status then
    return {"missing"}
end

if status ~= "running" then
    return {"not_running", status}
end

local currentOwner = rcall("HGET", idemKey, "owner") or ""
local currentFence = tonumber(rcall("HGET", idemKey, "fence") or "0")
if currentOwner ~= owner or currentFence ~= fence then
    return {"owner_mismatch", currentOwner, currentFence}
end

rcall("HSET", idemKey,
    "status", "completed",
    "owner", "",
    "completed_at_ms", nowMs,
    "result_ref", resultRef,
    "result_digest", resultDigest
)
rcall("PEXPIRE", idemKey, ttlMs)
return {"ok", nowMs}
