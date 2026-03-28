# Fluxera

Fluxera is an async-native Python task runtime inspired by Dramatiq.

It is built for workloads where a worker should keep a lot of I/O in flight without buying concurrency through large worker-thread pools, while still handling synchronous and CPU-bound work through dedicated execution lanes.

## Why Fluxera

- `async def` actors run as real `asyncio` tasks on the worker event loop.
- `def` actors still work through a bounded thread lane.
- CPU-heavy actors can be isolated in a separate process lane.
- Redis Streams is supported as an at-least-once transport with lease renewal, stale reclaim, deduplication, and idempotency primitives.
- Rolling deploys can hand off unstarted backlog between old and new worker revisions without rotating namespaces.

## Status

`0.0.1` is the first public alpha.

The runtime, Redis transport v2, revision management, benchmark harnesses, and release packaging are in place, but APIs may still change as the project hardens.

## Install

```bash
pip install fluxera
```

For local Redis development:

```bash
docker compose up -d
```

## Quick Start

```python
import asyncio

import fluxera


broker = fluxera.RedisBroker(
    "redis://127.0.0.1:6379/15",
    namespace="hello-fluxera",
)


@fluxera.actor(broker=broker, queue_name="default")
async def fetch_user(user_id: str) -> None:
    await asyncio.sleep(0.1)
    print("fetched", user_id)


async def main() -> None:
    async with fluxera.Worker(
        broker,
        concurrency=128,
        thread_concurrency=16,
        process_concurrency=4,
    ):
        await fetch_user.send("user-123")
        await broker.join(fetch_user.queue_name)


asyncio.run(main())
```

## Execution Model

Fluxera has three execution lanes:

- `async`: default for `async def` actors
- `thread`: default for regular `def` actors
- `process`: opt-in for CPU-heavy actors

The process lane defaults to `spawn` for safe multithreaded startup. You can still override it through `Worker(process_start_method=...)` or `FLUXERA_PROCESS_START_METHOD` when needed.

Example CPU actor:

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

## Serving Revision Admin

Fluxera keeps `namespace` as the broker identity boundary and uses `worker_revision` and `serving_revision` for rollout control.

Read the current serving revision:

```bash
fluxera revision get \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace hello-fluxera \
  --queue default
```

Promote a new serving revision with a CAS guard:

```bash
fluxera revision promote \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace hello-fluxera \
  --queue default \
  --revision 20260329153000 \
  --expected-revision 20260329140000
```

Use `--format json` when the command is called by deployment automation.

## Delivery Semantics

- Transport delivery is at-least-once.
- Deduplication is an enqueue-time admission policy, not exactly-once execution.
- Effectively-once side effects require idempotency keys or application-level dedupe.
- Redis workers renew leases for long-running tasks and reclaim stale pending deliveries.

## Benchmark Snapshot

Latest local measurements were taken on `2026-03-29` on `macOS 26.3.1`, `Python 3.12.10`, `Apple M5 Pro (15 cores)`.

Benchmark label legend:

- `c=`: Fluxera worker concurrency setting used by the benchmark runner
- `t=`: Dramatiq `worker_threads`

Headline results against the current local Dramatiq checkout:

| Scenario | Fluxera | Dramatiq | Takeaway |
| --- | --- | --- | --- |
| production-shaped async fanout | `0.258s` | `0.385s (t=8)` / `0.319s (t=32)` | Fluxera is faster with `2` threads instead of `12` or `36` |
| single-worker CPU-bound | `1.270s` | `3.893s (t=8)` / `3.704s (t=32)` | process lane still gives Fluxera a large single-worker win |
| mixed long I/O + short work | `short_drain=0.040s` | `6.038s (t=8)` / `0.098s (t=32)` | long I/O does not starve short work |
| Redis mixed long/short | `wall=1.527s`, `short_drain=0.081s` | `3.176s`, `1.673s (t=8)` / `1.713s`, `0.089s (t=32)` | transport advantage remains on real Redis |

See [BENCHMARK.md](docs/BENCHMARK.md) for the full methodology and numbers.

One nuance matters: with the safer default `spawn` process policy, cluster-scale CPU throughput is no longer universally faster than Dramatiq. Fluxera's strongest advantage is still async-heavy and mixed I/O workloads.

## Verification

The current release candidate was checked with:

- `python3 -m unittest discover -s tests -v`
- `python3 benchmarks/production_compare.py --profile smoke`
- `python3 benchmarks/redis_transport_compare.py --repeat 3 --long-io-secs 1.5`
- `/tmp/fluxera-release-venv/bin/python -m build --sdist --wheel`
- `/tmp/fluxera-release-venv/bin/python -m twine check dist/*`

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Benchmark Results](docs/BENCHMARK.md)
- [Revision Management](docs/REVISION_MANAGEMENT.md)
- [System Design](docs/SYSTEM_DESIGN.md)
- [Deduplication and Idempotency](docs/DEDUP_IDEMPOTENCY.md)
- [Redis Lua Contract](docs/REDIS_LUA_CONTRACT.md)

## Current Limits

- public APIs may still change during the alpha period
- result backends are not implemented yet
- message registry garbage collection is still intentionally simple
