# Fluxera Deduplication and Idempotency Design

Status: Draft v0.1

Last Updated: 2026-03-28

## 1. Purpose

Fluxera already provides at-least-once delivery and ack-late processing. This document defines two optional policy layers that sit above those transport guarantees:

- `DedupPolicy`: prevent duplicate admission of logically equivalent messages
- `IdempotencyStore`: prevent duplicate side effects when the same logical work is redelivered or retried

These layers are related, but they solve different problems.

## 2. Terms

- `message_id`: the logical Fluxera message identifier. Retries of the same logical message keep the same `message_id`.
- `delivery_id`: the transport-specific delivery handle. One logical message may have many deliveries over time.
- `dedupe_id`: the key used to suppress or replace new enqueue attempts.
- `idempotency_key`: the key used to coordinate in-flight or completed logical work.
- `owner`: the runtime-owned execution identifier for one active attempt. In Redis this should be globally unique per execution.
- `fence_token`: a monotonic token incremented whenever ownership is newly acquired or stolen from a stale owner.
- `effectively-once`: the practical outcome where duplicate deliveries do not produce duplicate effects because the runtime and the downstream side effect both honor the same key or fence token.

## 3. Guarantee Matrix

Fluxera should document guarantees in three layers:

- Transport: at-least-once delivery
- Admission: optional exactly-once admission for duplicate enqueue attempts
- Effects: optional effectively-once side effects when an idempotency policy is configured

Expected combinations:

| Policy | New duplicate send | Redelivery after crash | Side-effect safety |
| --- | --- | --- | --- |
| none | accepted | rerun | none |
| dedupe only | suppressed or replaced | rerun | none |
| idempotency only | accepted | may short-circuit or steal | good when sink cooperates |
| dedupe + idempotency | suppressed or replaced | may short-circuit or steal | best practical model |

The important rule is simple: dedupe does not replace idempotency, and idempotency does not change the broker to exactly-once delivery.

## 4. Deduplication Policy

### 4.1 Scope

Deduplication keys should be scoped by default to:

- namespace
- queue name
- actor name
- user-provided dedupe id

This avoids accidental collisions between unrelated actors that share a queue.

### 4.2 API Shape

The runtime should support both actor defaults and per-message overrides.

Suggested message options:

```python
await actor.send_with_options(
    ...,
    job_id="billing:invoice:123",
    deduplication={
        "id": "billing:invoice:123",
        "mode": "simple",
        "ttl_ms": None,
        "extend": False,
        "replace": False,
    },
    idempotency={
        "key": "billing:invoice:123",
        "result_ttl_ms": 86_400_000,
        "busy_strategy": "requeue",
    },
)
```

Suggested rules:

- `job_id` is a convenience alias for `deduplication.mode="simple"` with a stable key
- `job_id` does not automatically imply idempotency
- `deduplication.id` and `job_id` are mutually exclusive unless one is explicitly declared to alias the other
- `idempotency.key` is optional and must be user-controlled or actor-configured

### 4.3 Modes

#### `simple`

Semantics:

- first enqueue wins
- later enqueue attempts with the same key are rejected while the owning logical message remains unfinished
- key is removed when the owning logical message reaches a terminal state

Use cases:

- avoid launching the same import twice
- prevent duplicate billing or report generation requests

#### `throttle`

Semantics:

- first enqueue within a TTL wins
- later attempts during the TTL are rejected
- completion state does not matter once the TTL exists

Use cases:

- protect expensive fanout jobs from bursty callers
- collapse repeated webhook-triggered refresh requests

#### `debounce`

Semantics:

- only valid for delayed messages
- while the key is active, a new enqueue attempt replaces the currently delayed message payload
- TTL may optionally extend on every replacement

Use cases:

- keep only the latest search-index refresh request
- keep only the latest cache rebuild request

### 4.4 Deduplication State Lifecycle

The runtime should treat the dedupe record as a pointer to the current logical owner:

- key value = `message_id`
- optional TTL depends on the mode
- terminal finalization removes the key only if the current owner still matches the key

This last condition matters because a debounce replacement may have changed the owner since the original message was first enqueued.

### 4.5 Deduplication Events and Metrics

Fluxera should emit:

- `message_deduplicated`
- `message_replaced`
- `dedupe_key_removed`

Recommended metrics:

- deduplicated messages total
- replaced messages total
- dedupe key cardinality
- dedupe decision latency

## 5. Idempotency Store

### 5.1 Goals

The `IdempotencyStore` exists to answer these questions before a handler runs:

- is this key already completed
- is this key currently running on another worker
- is the previous owner stale and safe to steal
- if we do run, what fence token proves current ownership

### 5.2 Scope

Idempotency keys should also be scoped by:

- namespace
- actor name
- user-provided idempotency key

Queue name may also be included, but actor name should be sufficient for v1 if actor names are globally unique within an app.

### 5.3 Stored State

Suggested v1 fields:

- `status`: `running` or `completed`
- `owner`
- `fence_token`
- `message_id`
- `attempt`
- `started_at_ms`
- `heartbeat_at_ms`
- `lease_deadline_ms`
- `completed_at_ms`
- `result_ref`
- `result_digest`

Optional later fields:

- `last_error_type`
- `last_error_at_ms`
- `last_worker_id`

### 5.4 Public Interface

Suggested async interface:

```python
class IdempotencyStore(Protocol):
    async def begin(self, *, key: str, owner: str, message_id: str, attempt: int, lease_ms: int) -> BeginResult: ...
    async def heartbeat(self, *, key: str, owner: str, fence_token: int, lease_ms: int) -> HeartbeatResult: ...
    async def commit(self, *, key: str, owner: str, fence_token: int, result_ref: str | None, result_digest: str | None, ttl_ms: int) -> CommitResult: ...
    async def release(self, *, key: str, owner: str, fence_token: int) -> ReleaseResult: ...
```

Expected `begin` outcomes:

- `acquired`
- `completed`
- `busy`
- `stolen`

### 5.5 Runtime Semantics

#### Before executing the actor

1. Worker resolves `idempotency_key`
2. Worker calls `begin`
3. Runtime branches on result:

- `acquired`: run the handler
- `stolen`: run the handler with the new fence token
- `completed`: do not rerun the handler; ack the delivery and emit `short_circuited_completed`
- `busy`: do not run the handler; apply `busy_strategy`

#### `busy_strategy`

Recommended initial strategies:

- `requeue`: default; ack current delivery only after a delayed retry or equivalent broker-safe requeue is scheduled
- `drop`: use only when a separate dedupe policy already guarantees user intent

`wait_for_completion` should not be part of v1 because it complicates runtime shutdown and local admission accounting.

#### While executing the actor

- the worker heartbeats the idempotency lease alongside the broker lease
- the handler can read the current fence token from context
- downstream integrations can use the idempotency key and fence token for external writes

#### On success

Success sequence must be:

1. commit idempotency state to `completed`
2. write any optional result backend entry if needed
3. ack the broker delivery
4. remove the dedupe key if the policy requires terminal release

This order intentionally prefers duplicate ack over duplicate effect:

- if the worker crashes after `commit` but before `ack`, a future redelivery will see `completed` and skip handler execution

#### On retryable failure

The runtime may call `release` and schedule a retry, but this only remains safe when the downstream side effect is itself idempotent.

This is the unavoidable boundary:

- if the handler already touched an external system and then crashed before `commit`, the runtime alone cannot prove whether the effect happened
- effectively-once behavior requires the external sink to enforce the same idempotency key or fence token

#### On terminal failure

For v1, terminal failure should remove the `running` state rather than store a durable failed state. This keeps semantics simple and allows a future explicit retry with the same key.

A later `failed_terminal` state may be added if operators want duplicate suppression after unrecoverable failure.

### 5.6 Fence Tokens

Every successful `begin` acquisition must return a monotonic `fence_token`.

Reasons:

- a stale owner may still be alive for a short time after losing its lease
- an external sink can reject writes with an older fence token
- this is the only robust way to protect against split-brain execution after lease expiry

Examples:

- SQL row update with `WHERE fence_token < :new_token`
- object storage manifest update guarded by monotonic version
- external API call using `Idempotency-Key` plus a monotonic attempt marker when supported

## 6. Implications for the Redis Broker

### 6.1 Current Prototype Limitation

The current prototype stores delayed payloads directly as sorted-set members. That layout is convenient for a minimal broker, but it is a poor fit for debounce replacement and idempotency-aware short-circuit paths.

For the next Redis contract, Fluxera should move to a message registry model:

- `stream` stores `message_id`
- `delayed` stores `message_id`
- `message` key stores the encoded payload

This enables:

- replacing a delayed message by id
- reusing the same logical message record across retries
- cleaner dedupe ownership checks
- lighter stream entries

### 6.2 Suggested Redis Keys

- `namespace:stream:{queue}` -> stream of `message_id`
- `namespace:delayed:{queue}` -> zset of `message_id` by due timestamp
- `namespace:message:{message_id}` -> encoded Fluxera message
- `namespace:dead:{queue}` -> dead-letter list of `message_id` or encoded payload
- `namespace:dedupe:{queue}:{actor}:{dedupe_id}` -> dedupe owner pointer
- `namespace:idem:{actor}:{idempotency_key}` -> idempotency hash

## 7. TaskRecord Extensions

`TaskRecord` should grow enough fields to expose the new semantics directly:

- `delivery_id`
- `dedupe_id`
- `idempotency_key`
- `idempotency_owner`
- `fence_token`
- `idempotency_status`
- `lease_deadline`
- `idempotency_deadline`
- `result_ref`
- `result_digest`
- `short_circuited`

This keeps runtime state inspectable and observable without ad hoc transport lookups.

## 8. Recommended v1 Boundaries

Fluxera v1 should include:

- `simple`, `throttle`, and `debounce` dedupe modes
- Redis-backed `IdempotencyStore`
- fence tokens
- result short-circuit on `completed`
- `requeue` and `drop` busy strategies

Fluxera v1 should defer:

- waiting on another owner's completion
- distributed result fan-in
- durable failed-terminal idempotency states
- cross-broker idempotency backends beyond Redis

## 9. Final Position

The right goal for Fluxera is not to market exactly-once delivery. It is to provide:

- precise enqueue-time deduplication
- explicit effect-level idempotency
- enough runtime state to make duplicate handling observable and debuggable

That is more honest than claiming broker-level exactly-once, and more useful than leaving idempotency as an application footnote.
