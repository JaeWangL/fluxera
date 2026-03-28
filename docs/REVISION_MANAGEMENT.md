# Fluxera Revision Management

Status: Draft v0.1

Last Updated: 2026-03-29

## 1. Purpose

Fluxera keeps `namespace` as the broker identity boundary.

That is intentional and correct.

The problem revision management solves is different:

- old workers and new workers may temporarily share the same namespace and queue names
- in that overlap period, unstarted work should stop flowing to stale workers
- already-fetched but not-yet-executed work should be handed off to the serving revision automatically

This document defines the control plane for that behavior.

## 2. Terms

### `worker_revision`

The immutable revision identifier carried by one worker process.

Examples:

- git SHA
- container image digest
- deployment revision
- timestamp-based release id such as `YYYYMMddHHmmss`

This value must be injected by deployment metadata or environment variables.

Recommended environment variable:

```bash
FLUXERA_WORKER_REVISION=20260329153000
```

`worker_revision` is not generated automatically by Fluxera in production rollouts because all workers from the same deployment must agree on exactly the same revision string.

### `serving_revision`

The revision that is currently allowed to accept new work for a queue.

This is a broker-side control value, not a property embedded in each message.

### Worker states

- `accepting`: the worker's `worker_revision` matches the queue's `serving_revision`
- `draining`: the worker revision does not match and the worker must not start new work for that queue

## 3. Why This Exists

Without revision control, old and new workers are indistinguishable as long as they share:

- the same namespace
- the same queue names
- the same actor names

In that model, whichever worker happens to fetch a message first may execute it, even if a newer deployment is already healthy.

Revision management changes only that routing behavior. It does not change:

- `namespace`
- at-least-once transport semantics
- ack-late processing

## 4. Design Rules

### 4.1 Namespace is not a rollout mechanism

`namespace` remains a logical app or environment boundary.

It must not be rotated just to force new code to receive work first.

### 4.2 Revisions are queue-scoped control state

The current implementation stores `serving_revision` per queue.

That gives enough control to drain one queue at a time without requiring namespace-wide cutovers.

### 4.3 Old workers stop before execution, not just before fetch

A stale worker must not:

- fetch new work for a draining queue
- start execution of a message that was fetched before the promotion but not yet started

This second rule is essential.

It is the difference between "old workers slow down eventually" and "unstarted work actually moves to the new revision."

## 5. Redis Key Layout

The RedisBroker revision control plane uses these keys:

- `namespace:serving_revision:{queue}` -> current serving revision for a queue
- `namespace:worker:{worker_id}` -> worker heartbeat hash
- `namespace:workers:{queue}` -> zset of worker ids by last-seen timestamp

Current worker hash fields:

- `worker_revision`
- `last_seen_ms`
- `hostname`
- `pid`
- `queues`
- `accepting_queues`

## 6. Worker Lifecycle

### 6.1 Startup

On startup the worker:

1. resolves `worker_revision`
2. discovers managed queues
3. calls `ensure_serving_revision(queue, worker_revision)`
4. computes queue state:
   - equal -> `accepting`
   - different -> `draining`
5. registers its heartbeat and queue states

Bootstrap rule:

- if `serving_revision` is not set yet, the first worker claims it with its own `worker_revision`

### 6.2 Steady state

Each worker polls the broker periodically.

For every managed queue it refreshes:

- current `serving_revision`
- local queue state
- worker heartbeat

### 6.3 When a worker is `accepting`

The worker may:

- fetch deliveries
- enqueue them into local ready queues
- execute them normally

### 6.4 When a worker becomes `draining`

The worker must:

- stop fetching new deliveries for that queue
- requeue any delivery that was fetched but not yet started
- allow already-running work to finish under current policy

Current implementation behavior:

- ingress stops receiving from draining queues
- scheduler re-checks queue state before admitting execution
- if the queue is now draining, the delivery is requeued instead of executed

## 7. Rollout Flow

Normal rollout:

1. old workers are running with `worker_revision=rev-old`
2. queue serving revision is `rev-old`
3. new workers start with `worker_revision=rev-new`
4. new workers come up in `draining` state because `serving_revision` is still `rev-old`
5. once the new deployment is healthy, the control plane promotes:

```text
serving_revision(default) = rev-new
```

6. new workers transition to `accepting`
7. old workers transition to `draining`
8. old workers stop fetching new work and requeue unstarted backlog
9. new workers receive newly routed work and any handed-off backlog

Rollback works the same way in reverse by promoting the older revision again.

## 8. Current Public Surface

### Worker

`fluxera.Worker(...)` now accepts:

- `worker_revision`
- `worker_id`
- `revision_poll_interval`

If `worker_revision` is omitted, Fluxera reads:

```bash
FLUXERA_WORKER_REVISION
```

and falls back to `"local"` only when nothing is provided.

The `"local"` fallback exists for development convenience, not as a production rollout strategy.

### RedisBroker

The Redis broker currently exposes:

- `ensure_serving_revision(queue_name, worker_revision)`
- `get_serving_revision(queue_name)`
- `promote_serving_revision(queue_name, revision, expected_revision=None)`
- `register_worker_revision(worker_id, worker_revision, queue_states)`
- `unregister_worker_revision(worker_id, queue_names)`

### CLI

Fluxera also exposes revision admin commands:

```bash
fluxera revision get \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --queue default

fluxera revision promote \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace my-app \
  --queue default \
  --revision 20260329153000 \
  --expected-revision 20260329140000
```

For automation, prefer:

```bash
fluxera revision promote ... --format json
```

## 9. Example

```python
import os

import fluxera


broker = fluxera.RedisBroker("redis://127.0.0.1:6379/15", namespace="my-app")

worker = fluxera.Worker(
    broker,
    worker_revision=os.environ["FLUXERA_WORKER_REVISION"],
    revision_poll_interval=0.5,
)
```

Promotion example:

```python
await broker.promote_serving_revision(
    "default",
    "20260329153000",
    expected_revision="20260329140000",
)
```

## 10. What This Solves

This design solves:

- stale workers continuing to receive new work after a healthy new deployment exists
- prefetched but unstarted local backlog staying on old workers

It does not attempt to solve:

- in-flight task migration after execution has already started
- schema-incompatible message payload migrations
- exactly-once execution

Those concerns need separate policies.

## 11. Future Work

- health quorum checks before allowing promotion
- actor-level `on_revision_change` policy for in-flight work
- richer worker introspection and rollout dashboards
