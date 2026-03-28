# Getting Started With Fluxera

## What Fluxera Is

Fluxera is an async-native task runtime for Python applications that need to keep a lot of I/O in flight without scaling concurrency through large worker-thread pools.

It keeps three goals in balance:

- async actors should run as real `asyncio` tasks
- sync actors should still be easy to use
- CPU-bound actors should have a separate process lane so they do not block I/O-heavy work

## Requirements

- Python `3.10+`
- Redis `7+` recommended for the current Redis Streams transport

## Install

```bash
pip install fluxera
```

If you are developing locally from the repository:

```bash
docker compose up -d
python3 -m unittest discover -s tests -v
```

The local Docker Compose file starts Redis on `127.0.0.1:6379`.

## Your First Async Actor

```python
import asyncio

import fluxera


broker = fluxera.RedisBroker(
    "redis://127.0.0.1:6379/15",
    namespace="getting-started",
)


@fluxera.actor(broker=broker, queue_name="default")
async def fetch_profile(user_id: str) -> None:
    await asyncio.sleep(0.1)
    print("fetched profile", user_id)


async def main() -> None:
    async with fluxera.Worker(broker, concurrency=128):
        await fetch_profile.send("user-123")
        await broker.join(fetch_profile.queue_name)


asyncio.run(main())
```

What happens here:

- `RedisBroker` stores messages in Redis Streams
- `Worker` admits messages into lane-specific execution pools
- async actors run on the shared process event loop as tasks
- the broker acks only after the actor finishes successfully

## Running Sync And CPU-Bound Actors

Fluxera does not force everything into a single execution model.

### Sync actors

Regular `def` actors default to the thread lane:

```python
import time

import fluxera


broker = fluxera.RedisBroker("redis://127.0.0.1:6379/15", namespace="sync-example")


@fluxera.actor(broker=broker, queue_name="default")
def render_thumbnail(asset_id: str) -> None:
    time.sleep(0.1)
    print("rendered", asset_id)
```

### CPU-bound actors

CPU-heavy work should use the process lane:

```python
import fluxera


broker = fluxera.RedisBroker("redis://127.0.0.1:6379/15", namespace="cpu-example")


def score_document(text: str) -> int:
    return sum(ord(ch) for ch in text)


score_document_actor = fluxera.actor(
    broker=broker,
    actor_name="score_document",
    queue_name="cpu",
    execution="process",
)(score_document)
```

A worker can run all three lanes at once:

```python
worker = fluxera.Worker(
    broker,
    concurrency=128,
    thread_concurrency=16,
    process_concurrency=4,
)
```

This lets long-running async I/O keep making progress while CPU work is isolated in subprocesses.

Fluxera uses `spawn` as the default process start method so the process lane is safe to use from multithreaded workers. If you need a different policy for a specific deployment, pass `process_start_method=...` to `Worker(...)` or set `FLUXERA_PROCESS_START_METHOD`.

## Serving Revision Admin

Fluxera ships with a small admin CLI for queue revision control:

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

Use `--format json` when the command is being called by deployment automation.

## Deduplication And Idempotency

Fluxera separates three ideas:

- transport delivery is at-least-once
- enqueue admission can be deduplicated
- side effects can be made effectively-once with idempotency

Example enqueue-side deduplication:

```python
await fetch_profile.send_with_options(
    args=("user-123",),
    job_id="profile:user-123",
)
```

See [DEDUP_IDEMPOTENCY.md](DEDUP_IDEMPOTENCY.md) for the full model.

## Redis Broker Notes

The current `RedisBroker` uses Redis Streams plus a message registry:

- stream entries store `message_id`
- delayed jobs store `message_id`
- payloads live under `namespace:message:{message_id}`
- Lua scripts handle deduplication and idempotency state transitions

This layout keeps transport entries small and makes debounce, dedupe, and idempotency contracts easier to evolve.

## Verifying The Install

From the repository root you can run:

```bash
python3 -m unittest discover -s tests -v
python3 benchmarks/redis_transport_compare.py --repeat 1 --long-io-secs 1.5
```

These cover:

- core actor and worker behavior
- Redis at-least-once delivery and stale reclaim
- dedupe and idempotency Redis Lua wrappers
- Redis transport comparison against Dramatiq

## Current Limits

`0.0.1` is an early alpha, so a few edges are still intentionally narrow:

- public APIs may change
- result backends are not implemented yet
- queue-specific garbage collection for the message registry is still simple
- process start-method policy should be hardened before broad production rollout

## Where To Go Next

- [System Design](SYSTEM_DESIGN.md)
- [Deduplication and Idempotency](DEDUP_IDEMPOTENCY.md)
- [Redis Lua Contract](REDIS_LUA_CONTRACT.md)
