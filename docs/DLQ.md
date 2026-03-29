# Fluxera Dead Letter and Retry Design

Status: Draft v0.1

Last Updated: 2026-03-30

## 1. Purpose

Fluxera already aims to provide:

- ack-late processing
- at-least-once transport delivery
- explicit retry policies

That is necessary, but not sufficient for production operations.

A production-ready failure model must also guarantee:

- no silent loss when a message reaches a terminal failure path
- a durable record of why a message failed
- a clear distinction between `redelivery`, `retry`, `dead-letter`, and `operator replay`
- an admin surface that lets operators inspect and resolve failures safely

This document defines that model.

## 2. Design Rules

### 2.1 At-least-once comes before DLQ

The transport should keep retrying delivery until one of these is true:

- the actor succeeds and the message is acked
- the message is explicitly requeued
- the message reaches a terminal failure state and is recorded in the DLQ

Dead-lettering is a terminal decision above the transport layer. It is not a substitute for redelivery.

### 2.2 No silent drop

If Fluxera cannot safely execute or reconstruct a message, it must still produce an inspectable terminal record.

This applies especially to:

- retry exhaustion
- timeout exhaustion
- explicit `reject(requeue=False)`
- decode failure
- message registry payload loss
- invalid callback configuration

### 2.3 DLQ records must not depend on live message registry TTL

The current live payload registry exists to power normal delivery.

The DLQ must store an independent snapshot of the message and failure metadata so operators can inspect failures even after the live registry has expired.

### 2.4 Retry metadata and DLQ metadata are related, but different

Retry state answers:

- how many attempts have happened
- whether another attempt should be scheduled
- when the next attempt should happen

DLQ state answers:

- why no more automatic attempts will be made
- what exactly failed
- what operators can do next

### 2.5 Failure hooks must not become a new source of message loss

Observability and callback hooks are important, but they must be downstream of the core state transition.

The required order is:

1. persist retry or DLQ state
2. perform the broker ack or reject action
3. emit logs, metrics, and callbacks on a best-effort basis

Hook failures must never roll back a terminal DLQ write.

## 3. Terms

- `message_id`: the logical Fluxera message identifier
- `delivery_id`: the transport-specific delivery identifier
- `attempt`: the zero-based attempt number for the current execution
- `retry`: scheduling a new future attempt for the same logical message
- `redelivery`: the broker delivering the same logical message again after a lease loss, crash, or reclaim
- `dead_letter_id`: the durable identifier for one terminal dead-letter record
- `failure_kind`: the normalized terminal failure reason category
- `resolution_state`: the operator-facing state of a dead-letter record

## 4. Failure Taxonomy

Fluxera should normalize terminal outcomes into explicit categories.

| Failure kind | Meaning | Retry eligible | Dead-lettered |
| --- | --- | --- | --- |
| `exception` | actor raised an exception | yes, by policy | yes, after exhaustion or explicit abort |
| `timeout` | actor exceeded `timeout_ms` | yes, by policy | yes, after exhaustion |
| `cancel_requeue` | worker shutdown or external cancel with requeue policy | no | no |
| `cancel_reject` | cancellation with reject policy | no | yes |
| `integrity_missing_payload` | transport entry exists but live payload is gone | no | yes |
| `integrity_decode_error` | payload exists but cannot be decoded | no | yes |
| `operator_reject` | operator explicitly rejects replay or marks terminal | no | yes |
| `invalid_configuration` | actor retry/callback config is invalid at execution time | no | yes |

Important distinction:

- `cancel_requeue` is not a DLQ case
- `redelivery after crash` is not a DLQ case
- `retry exhaustion` is a DLQ case

## 5. Execution State Machine

The runtime should treat message execution as this state machine:

1. `received`
2. `running`
3. terminal branch:
   - `succeeded` -> ack
   - `retry_scheduled` -> enqueue retry, ack current delivery
   - `requeued` -> reject with requeue
   - `dead_lettered` -> persist `DeadLetterRecord`, remove current delivery from active transport

The critical point is the last branch:

- a message must not be considered dead-lettered until the dead-letter record itself is durable

## 6. DeadLetterRecord Schema

`DeadLetterRecord` should be the canonical terminal failure object for Fluxera.

Suggested shape:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ResolutionState = Literal["active", "requeued", "purged"]
FailureKind = Literal[
    "exception",
    "timeout",
    "cancel_reject",
    "integrity_missing_payload",
    "integrity_decode_error",
    "operator_reject",
    "invalid_configuration",
]


@dataclass(slots=True)
class DeadLetterRecord:
    dead_letter_id: str
    namespace: str
    queue_name: str
    actor_name: str
    message_id: str
    delivery_id: str | None
    resolution_state: ResolutionState
    failure_kind: FailureKind
    message_snapshot: dict[str, Any] | None
    payload_available: bool
    attempt: int
    max_retries: int | None
    timeout_ms: int | None
    execution_mode: str
    exception_type: str | None
    exception_message: str | None
    traceback_text: str | None
    cancel_reason: str | None
    retry_delay_ms: int | None
    dead_lettered_at_ms: int
    message_timestamp_ms: int | None
    received_at_ms: int | None
    started_at_ms: int | None
    failed_at_ms: int | None
    worker_id: str | None
    worker_revision: str | None
    consumer_name: str | None
    deduplication_id: str | None
    idempotency_key: str | None
    origin_dead_letter_id: str | None
    resolution_note: str | None
    resolved_at_ms: int | None
    retention_deadline_ms: int | None
```

## 7. Schema Semantics

### 7.1 Identity fields

- `dead_letter_id`
  - unique durable id for the terminal record
  - generated when the runtime decides to dead-letter
- `namespace`
  - broker namespace where the failure occurred
- `queue_name`
  - queue from which the message was processed
- `actor_name`
  - actor selected for execution
- `message_id`
  - original logical message id
- `delivery_id`
  - last transport delivery id, if available

### 7.2 Resolution fields

- `resolution_state`
  - `active`: still waiting for operator action
  - `requeued`: replayed or restored by an operator
  - `purged`: explicitly removed by an operator or retention job
- `origin_dead_letter_id`
  - set when an operator requeues an already dead-lettered record and the replay later dead-letters again
- `resolution_note`
  - optional human or automation note
- `resolved_at_ms`
  - timestamp of the last resolution transition

### 7.3 Failure details

- `failure_kind`
  - normalized category from the taxonomy above
- `exception_type`
  - Python exception class name or remote exception name
- `exception_message`
  - short human-readable message
- `traceback_text`
  - serialized traceback or remote traceback
- `cancel_reason`
  - for cancel-based terminal failures only
- `retry_delay_ms`
  - last computed retry delay before exhaustion, if any

### 7.4 Message snapshot

- `message_snapshot`
  - a self-contained serialized snapshot of the original message
  - must include:
    - `queue_name`
    - `actor_name`
    - `args`
    - `kwargs`
    - `options`
    - `message_id`
    - `message_timestamp`
- `payload_available`
  - `True` if the snapshot is complete
  - `False` when Fluxera had to synthesize a record without the original payload

This field is the key operational difference from the current implementation.

The DLQ must not depend on re-reading the normal message registry later.

### 7.5 Retry and execution context

- `attempt`
  - attempt number of the terminal execution
- `max_retries`
  - retry budget that applied at the point of failure
- `timeout_ms`
  - effective timeout for the failed attempt
- `execution_mode`
  - `async`, `thread`, or `process`

### 7.6 Timing fields

- `message_timestamp_ms`
  - original enqueue timestamp
- `received_at_ms`
  - when this attempt entered the runtime
- `started_at_ms`
  - when execution started
- `failed_at_ms`
  - when the failure was detected
- `dead_lettered_at_ms`
  - when the terminal record was written
- `retention_deadline_ms`
  - when the record becomes eligible for automatic purge

### 7.7 Worker context

- `worker_id`
  - worker runtime id
- `worker_revision`
  - rollout revision serving at the time of failure
- `consumer_name`
  - transport consumer identifier, when available

### 7.8 Coordination keys

- `deduplication_id`
  - optional dedupe id or job id that applied to the message
- `idempotency_key`
  - optional idempotency key that applied to the message

These fields matter because operator replay may need to know whether replaying the message will be deduplicated or short-circuited.

## 8. Redis Storage Architecture

Fluxera should store DLQ state independently from the live registry.

Recommended v1 key layout:

- `namespace:dlq:{queue}` -> sorted set of `dead_letter_id` scored by `dead_lettered_at_ms`
- `namespace:dlq:record:{dead_letter_id}` -> JSON-encoded `DeadLetterRecord`
- `namespace:dlq:message:{message_id}` -> optional set or sorted set of related dead-letter ids
- `namespace:dlq:stats:{queue}` -> optional counters and latest timestamps

Recommended retention default:

- `dead_letter_ttl_ms = 30 days`

Rationale:

- longer than live message registry retention
- long enough for operator response and incident review
- separate from retry backoff windows

### 8.1 Write path

When a terminal DLQ event happens:

1. build `DeadLetterRecord`
2. write `namespace:dlq:record:{dead_letter_id}`
3. add `dead_letter_id` to `namespace:dlq:{queue}` with score `dead_lettered_at_ms`
4. optionally index by message id
5. only then ack/xdel the active transport delivery

### 8.2 Read path

Admin reads should not need the normal message registry at all.

All list/get operations should read from DLQ record storage directly.

### 8.3 Purge path

Retention cleanup should:

1. remove expired ids from `namespace:dlq:{queue}`
2. delete their `namespace:dlq:record:{dead_letter_id}` entries
3. remove secondary indexes
4. increment purge metrics

## 9. Retry Policy Design

Retry needs to be more expressive than the current minimal model.

Recommended actor and message options:

- `max_retries`
- `min_backoff`
- `max_backoff`
- `retry_on_timeout`
- `retry_for`
- `retry_when`
- `jitter`
- `on_retry_scheduled`
- `on_retry_exhausted`
- `on_dead_lettered`
- `on_failure`

### 9.1 Default semantics

Conservative runtime default:

- `max_retries = 0`

Recommended production policy when retries are enabled:

- `min_backoff = 15_000`
- `max_backoff = 7 days`
- `jitter = "full"`

`Fluxera` should not silently retry with zero delay unless that is explicitly requested.

### 9.2 Backoff algorithm

Recommended policy:

- exponential backoff
- full jitter by default
- capped by `max_backoff`

This avoids retry stampedes better than deterministic doubling.

### 9.3 Predicate-based retry

`retry_when` should be supported as:

```python
def retry_when(attempt: int, exception: BaseException, record: TaskRecord) -> bool:
    ...
```

Rules:

- when `retry_when` is set, it takes precedence over simple `max_retries`
- timeout failures still pass through the predicate
- invalid predicates should cause a terminal `invalid_configuration` DLQ record

### 9.4 Exhaustion semantics

When the runtime decides that no more retries will be attempted:

1. it must emit a final retry exhaustion event
2. it must persist the dead-letter record
3. it may optionally enqueue an exhaustion callback actor

## 10. Callback and Hook Architecture

Fluxera should support two separate layers.

### 10.1 Runtime hooks

These are in-process hooks for logging, metrics, tracing, and error reporting.

Suggested protocol:

```python
class FailureLifecycleHooks(Protocol):
    async def on_retry_scheduled(self, record: TaskRecord, *, delay_ms: int) -> None: ...
    async def on_retry_exhausted(self, record: TaskRecord, *, dead_letter_id: str) -> None: ...
    async def on_dead_lettered(self, record: DeadLetterRecord) -> None: ...
    async def on_dead_letter_requeued(self, record: DeadLetterRecord) -> None: ...
```

Rules:

- hook failures are logged and swallowed
- hooks run after the durable state transition
- hooks must not mutate the record

### 10.2 Callback actors

These are queue-based follow-up actions for business workflows.

Suggested actor options:

- `on_retry_exhausted`
- `on_dead_lettered`
- `on_failure`
- `on_success`

Callback payloads must be JSON-serializable summaries, not raw exception objects.

Suggested dead-letter callback payload:

```json
{
  "dead_letter_id": "dlq_01H...",
  "namespace": "my-app",
  "queue_name": "default",
  "actor_name": "generate_report",
  "message_id": "msg_123",
  "failure_kind": "timeout",
  "exception_type": "TimeoutError",
  "exception_message": "",
  "attempt": 3,
  "max_retries": 3,
  "worker_revision": "20260330103000"
}
```

## 11. Python Admin Surface

Fluxera should expose an admin surface that is easy to call from applications and tooling.

Suggested types:

```python
@dataclass(slots=True)
class DeadLetterFilter:
    queue_name: str | None = None
    actor_name: str | None = None
    failure_kind: str | None = None
    resolution_state: str = "active"
    limit: int = 100
    cursor: str | None = None


@dataclass(slots=True)
class DeadLetterPage:
    records: list[DeadLetterRecord]
    next_cursor: str | None
```

Suggested functions:

- `list_dead_letters(...) -> DeadLetterPage`
- `get_dead_letter(dead_letter_id) -> DeadLetterRecord | None`
- `count_dead_letters(...) -> int`
- `requeue_dead_letter(dead_letter_id, *, as_new_message=True) -> Message`
- `purge_dead_letter(dead_letter_id) -> bool`
- `annotate_dead_letter(dead_letter_id, note: str) -> DeadLetterRecord`

### 11.1 Replay semantics

Default operator replay should use:

- a new `message_id`
- original payload snapshot
- reset `attempt=0`
- new options:
  - `replayed_from_dead_letter_id`
  - `origin_message_id`

Reason:

- safer around dedupe and idempotency
- preserves auditability
- keeps the original failed message immutable

Exact replay with preserved `message_id` may exist as an explicit override, but it should not be the default.

## 12. CLI Surface

The CLI should mirror the Python admin functions.

Suggested commands:

```bash
fluxera dlq list \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --queue default \
  --format json

fluxera dlq get \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --dead-letter-id dlq_01H...

fluxera dlq requeue \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --dead-letter-id dlq_01H...

fluxera dlq purge \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --dead-letter-id dlq_01H...

fluxera dlq stats \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --queue default
```

Required filtering and display fields:

- queue
- actor
- failure kind
- exception type
- dead-lettered time
- attempt and max retries
- worker revision
- resolution state

## 13. Logging and Metrics

Every retry and dead-letter transition should emit structured logs.

Minimum retry log fields:

- `event = retry_scheduled`
- `message_id`
- `actor_name`
- `queue_name`
- `attempt`
- `max_retries`
- `delay_ms`
- `exception_type`
- `worker_id`
- `worker_revision`

Minimum dead-letter log fields:

- `event = dead_lettered`
- `dead_letter_id`
- `message_id`
- `actor_name`
- `queue_name`
- `failure_kind`
- `exception_type`
- `attempt`
- `max_retries`
- `worker_id`
- `worker_revision`

Recommended metrics:

- `fluxera_retries_total`
- `fluxera_retry_exhausted_total`
- `fluxera_dead_letters_total`
- `fluxera_dead_letter_requeues_total`
- `fluxera_dead_letter_purges_total`
- `fluxera_dead_letter_active`
- `fluxera_dead_letter_oldest_age_seconds`

## 14. Required Changes Relative to Current Fluxera

The current implementation is a useful base, but it does not yet satisfy this design.

Primary gaps:

- DLQ stores only `message_id`, not a structured dead-letter record
- failure cause is not durable
- retry backoff has no jitter
- there is no `retry_when`
- there are no retry or DLQ callbacks
- there is no `dlq` CLI or Python admin page type
- payload-missing transport entries are currently dropped instead of dead-lettered

## 15. Implementation Order

Recommended order:

1. add `DeadLetterRecord` model and persistence path
2. route `integrity_missing_payload` and decode failures into DLQ
3. add Python admin surface
4. add `fluxera dlq ...` CLI
5. add retry jitter and `retry_when`
6. add runtime hooks and callback actors
7. add retention cleanup and DLQ stats

## 16. Non-Goals

This document does not try to provide:

- exactly-once delivery
- a visual web dashboard
- result backend semantics

Those may be layered on top later, but they are not required for a correct DLQ and retry architecture.
