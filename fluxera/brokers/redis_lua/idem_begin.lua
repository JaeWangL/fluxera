local rcall = redis.call

local idemKey = KEYS[1]

local owner = ARGV[1]
local messageId = ARGV[2]
local attempt = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])
local leaseMs = tonumber(ARGV[5])

local existingStatus = rcall("HGET", idemKey, "status")
if not existingStatus then
    local leaseDeadline = nowMs + leaseMs
    rcall("HSET", idemKey,
        "status", "running",
        "owner", owner,
        "fence", 1,
        "message_id", messageId,
        "attempt", attempt,
        "started_at_ms", nowMs,
        "heartbeat_at_ms", nowMs,
        "lease_deadline_ms", leaseDeadline
    )
    return {"acquired", 1, leaseDeadline}
end

if existingStatus == "completed" then
    return {
        "completed",
        tonumber(rcall("HGET", idemKey, "fence") or "0"),
        tonumber(rcall("HGET", idemKey, "completed_at_ms") or "0"),
        rcall("HGET", idemKey, "result_ref") or "",
        rcall("HGET", idemKey, "result_digest") or "",
    }
end

if existingStatus ~= "running" then
    return {"error", "invalid_status"}
end

local existingOwner = rcall("HGET", idemKey, "owner") or ""
local existingFence = tonumber(rcall("HGET", idemKey, "fence") or "0")
local leaseDeadline = tonumber(rcall("HGET", idemKey, "lease_deadline_ms") or "0")

if existingOwner == owner then
    leaseDeadline = nowMs + leaseMs
    rcall("HSET", idemKey,
        "message_id", messageId,
        "attempt", attempt,
        "heartbeat_at_ms", nowMs,
        "lease_deadline_ms", leaseDeadline
    )
    return {"acquired", existingFence, leaseDeadline}
end

if leaseDeadline > nowMs then
    return {"busy", existingFence, leaseDeadline, existingOwner}
end

local nextFence = existingFence + 1
leaseDeadline = nowMs + leaseMs
rcall("HSET", idemKey,
    "status", "running",
    "owner", owner,
    "fence", nextFence,
    "message_id", messageId,
    "attempt", attempt,
    "heartbeat_at_ms", nowMs,
    "lease_deadline_ms", leaseDeadline
)
return {"stolen", nextFence, leaseDeadline, existingOwner}
