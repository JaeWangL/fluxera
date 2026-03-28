# Fluxera Redis Lua Contract Draft

Status: Draft v0.1

Last Updated: 2026-03-28

## 1. Scope

This document defines the first draft of Redis Lua contracts for:

- enqueue-time deduplication
- delayed-message replacement
- idempotency acquisition
- lease heartbeat
- success commit
- retry release

The goal is atomicity and predictable semantics, not minimal script count.

## 2. Redis Data Model

Fluxera should use a message registry model for Redis-backed dedupe and idempotency.

### 2.1 Core Keys

- `namespace:stream:{queue}` -> stream of `message_id`
- `namespace:delayed:{queue}` -> zset of `message_id` scored by due timestamp
- `namespace:message:{message_id}` -> encoded message payload
- `namespace:dead:{queue}` -> dead-letter list

### 2.2 Dedupe Keys

- `namespace:dedupe:{queue}:{actor}:{dedupe_id}`

Value:

- the owning `message_id`

TTL:

- no TTL for `simple`
- explicit TTL for `throttle`
- explicit TTL for `debounce`

### 2.3 Idempotency Keys

- `namespace:idem:{actor}:{idempotency_key}`

Suggested storage type:

- Redis hash

Suggested fields:

- `status`
- `owner`
- `fence`
- `message_id`
- `attempt`
- `started_at_ms`
- `heartbeat_at_ms`
- `lease_deadline_ms`
- `completed_at_ms`
- `result_ref`
- `result_digest`

## 3. Script Design Rules

- every script is idempotent with respect to retries from the same caller
- return values are machine-readable tuples, not ad hoc strings
- ownership-sensitive scripts require both `owner` and `fence`
- scripts never delete a key unless the caller proves ownership or the stored lease is stale
- dedupe and idempotency scripts are transport-independent; broker ack remains outside these scripts

## 4. Contract: `enqueue_or_deduplicate.lua`

### Purpose

Atomically decide whether to:

- enqueue immediately
- schedule for delay
- suppress as duplicate
- replace an older delayed message in debounce mode

### Required Keys

1. `message_key`
2. `stream_key`
3. `delayed_key`
4. `dedupe_key`

### Required Args

1. `message_id`
2. `encoded_message`
3. `now_ms`
4. `deliver_at_ms`
5. `mode` (`none`, `simple`, `throttle`, `debounce`)
6. `ttl_ms`
7. `extend` (`0` or `1`)
8. `replace` (`0` or `1`)

### Return Contract

```text
["enqueued", message_id]
["scheduled", message_id]
["deduplicated", existing_message_id]
["replaced", new_message_id, old_message_id]
["error", reason]
```

### Semantics

#### `mode = none`

- write `message_key`
- if `deliver_at_ms <= now_ms`, append `message_id` to `stream`
- otherwise add `message_id` to `delayed`

#### `mode = simple`

- if `dedupe_key` exists, return `deduplicated`
- otherwise set `dedupe_key = message_id`
- write `message_key`
- enqueue or schedule the message

#### `mode = throttle`

- if `SET dedupe_key message_id NX PX ttl_ms` fails, return `deduplicated`
- otherwise write `message_key` and enqueue or schedule

#### `mode = debounce`

Rules:

- must be used with delayed delivery
- `replace` must be true
- if `dedupe_key` does not exist, set it to `message_id` with TTL and schedule new delayed message
- if `dedupe_key` points to an existing delayed owner, remove the old `message_id` from `delayed`, delete its `message_key`, store the new message, update the key, and return `replaced`
- if the dedupe key exists but the pointed message is not in `delayed`, treat it as stale and overwrite it with the new delayed owner
- if `extend` is true, refresh TTL on every replacement
- if `extend` is false, preserve remaining TTL

## 5. Contract: `remove_dedupe_key_if_owner.lua`

### Purpose

Remove a dedupe key only if the caller still owns it.

### Required Keys

1. `dedupe_key`

### Required Args

1. `message_id`

### Return Contract

```text
[1]  -- removed
[0]  -- not removed
```

### Semantics

- compare stored value to `message_id`
- delete only on exact match

This script is used on terminal success, terminal failure, or manual dedupe release.

## 6. Contract: `idem_begin.lua`

### Purpose

Acquire or inspect the idempotency state before execution starts.

### Required Keys

1. `idempotency_key`

### Required Args

1. `owner`
2. `message_id`
3. `attempt`
4. `now_ms`
5. `lease_ms`

### Return Contract

```text
["acquired", fence_token, lease_deadline_ms]
["stolen", fence_token, lease_deadline_ms, previous_owner]
["completed", fence_token, completed_at_ms, result_ref, result_digest]
["busy", fence_token, lease_deadline_ms, current_owner]
["error", reason]
```

### Semantics

#### Missing key

- initialize hash as `running`
- set `owner`
- set `fence = 1`
- set `message_id`, `attempt`, `started_at_ms`, `heartbeat_at_ms`, `lease_deadline_ms`
- return `acquired`

#### Existing `status = completed`

- return `completed` with stored result metadata

#### Existing `status = running` and same `owner`

- refresh heartbeat and lease
- return `acquired`

#### Existing `status = running` and different owner, lease still live

- return `busy`

#### Existing `status = running` and lease expired

- increment `fence`
- replace `owner`
- overwrite `message_id`, `attempt`, `heartbeat_at_ms`, `lease_deadline_ms`
- return `stolen`

## 7. Contract: `idem_heartbeat.lua`

### Purpose

Renew the logical execution lease while the handler is still running.

### Required Keys

1. `idempotency_key`

### Required Args

1. `owner`
2. `fence_token`
3. `now_ms`
4. `lease_ms`

### Return Contract

```text
["ok", lease_deadline_ms]
["missing"]
["owner_mismatch", current_owner, current_fence]
["not_running", status]
```

### Semantics

- only succeeds when `status = running` and both `owner` and `fence` match
- updates `heartbeat_at_ms` and `lease_deadline_ms`

## 8. Contract: `idem_commit.lua`

### Purpose

Transition a running idempotency record into `completed`.

### Required Keys

1. `idempotency_key`

### Required Args

1. `owner`
2. `fence_token`
3. `now_ms`
4. `ttl_ms`
5. `result_ref`
6. `result_digest`

### Return Contract

```text
["ok", completed_at_ms]
["missing"]
["owner_mismatch", current_owner, current_fence]
["not_running", status]
```

### Semantics

- requires matching `owner` and `fence`
- set `status = completed`
- clear `owner`
- keep `fence`
- set `completed_at_ms`, `result_ref`, `result_digest`
- apply `PEXPIRE ttl_ms`

### Important Crash Property

If the worker crashes after `idem_commit` but before broker ack:

- the message may be redelivered
- `idem_begin` will return `completed`
- the runtime should short-circuit execution and only ack

This is the main reason commit must happen before broker ack.

## 9. Contract: `idem_release.lua`

### Purpose

Release a running idempotency record after a retryable failure when the runtime decides the next attempt may start immediately.

### Required Keys

1. `idempotency_key`

### Required Args

1. `owner`
2. `fence_token`

### Return Contract

```text
["released"]
["missing"]
["owner_mismatch", current_owner, current_fence]
["not_running", status]
```

### Semantics

- delete the key only when `status = running` and ownership matches
- do not release a completed record

### Warning

This script is not a magical safety switch. If the handler already performed an external side effect before failure, releasing the key may allow a duplicate effect on retry. This is only safe when the external sink is also idempotent.

## 10. Optional Later Contract: `idem_fail_terminal.lua`

This is intentionally deferred.

Possible future use:

- preserve a durable failed state for explicit operator inspection
- suppress hot duplicate retries for unrecoverable business conflicts

It should not be part of v1 because it complicates retry semantics and user expectations.

## 11. Runtime Sequence Diagrams

### 11.1 Normal Success

1. worker receives delivery
2. worker runs `idem_begin`
3. result = `acquired`
4. handler runs and heartbeats broker lease plus idempotency lease
5. worker runs `idem_commit`
6. worker acks broker delivery
7. worker runs `remove_dedupe_key_if_owner` when needed

### 11.2 Crash After Commit Before Ack

1. handler finishes
2. worker runs `idem_commit`
3. worker crashes before ack
4. broker redelivers message
5. next worker runs `idem_begin`
6. result = `completed`
7. next worker skips handler and only acks

### 11.3 Crash Before Commit

1. handler performs side effect
2. worker crashes before `idem_commit`
3. broker redelivers
4. next worker sees stale or missing running record and acquires or steals
5. handler may run again

This case is why external sink cooperation is still required for effectively-once effects.

## 12. Error Codes

Lua scripts should keep return values structured and reserve actual Redis script errors for malformed input only.

Suggested malformed-input reasons:

- `invalid_mode`
- `debounce_requires_delay`
- `replace_requires_debounce`
- `missing_ttl`
- `bad_fence_token`

## 13. Implementation Notes

- script registration should be lazy and versioned by filename hash
- Python wrappers should convert tuple responses into typed dataclasses
- every script wrapper should be benchmarked because these paths sit directly on enqueue or execution admission
- message registry cleanup must happen on terminal ack, dead-letter, or replaced-delay removal

## 14. Final Position

Fluxera should implement dedupe and idempotency as first-class Redis contracts, not as ad hoc runtime conditionals scattered around the broker and worker. That keeps the semantics explicit, testable, and benchmarkable.
