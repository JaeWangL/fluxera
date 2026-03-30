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

`0.1.1` is the current public alpha.

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

## Worker CLI

Fluxera can now start workers directly from the CLI, similar to the way
projects used `dramatiq ...` before.

If your project has:

- a setup module that creates and registers a broker
- a worker registry that lists actor modules

you can run it like this:

```bash
fluxera worker \
  your_project.fluxera_setup \
  --module-registry your_project.worker_registry:WORKER_MODULES \
  --broker your_project.fluxera_setup:broker \
  --uvloop \
  --concurrency 64 \
  --thread-concurrency 8
```

For smoke tests and one-shot local runs, add `--exit-when-idle`.

## Producer APIs

Fluxera separates **message production** from **message execution**.

- `actor.send(...)` and `actor.send_sync(...)` produce messages
- worker execution lanes consume and execute those messages later

This is why `send_sync()` is not the same thing as the worker thread lane.

For `RedisBroker`, `send_sync()` now uses a real synchronous Redis producer
path. It is safe for normal blocking contexts such as schedulers, CLI tools,
and plain threads, but it still intentionally rejects calls made from inside an
already-running event loop.

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

Fluxera does not auto-promote `serving_revision` at worker startup by default.
That is intentional: startup only proves a worker booted, not that the new
revision should already receive queue traffic. Simple deployments may still
choose to auto-promote in their entrypoint or release automation.

## Delivery Semantics

- Transport delivery is at-least-once.
- Deduplication is an enqueue-time admission policy, not exactly-once execution.
- Effectively-once side effects require idempotency keys or application-level dedupe.
- Redis workers renew leases for long-running tasks and reclaim stale pending deliveries.

## Retry And Callbacks

Fluxera now supports Dramatiq-compatible retry controls plus async-native
callbacks.

Retry example:

```python
import fluxera


broker = fluxera.RedisBroker("redis://127.0.0.1:6379/15", namespace="retry-example")


def retry_when(attempt: int, exc: BaseException, record: fluxera.TaskRecord) -> bool:
    del record
    return attempt < 3 and not isinstance(exc, ValueError)


@fluxera.actor(
    broker=broker,
    queue_name="default",
    max_retries=5,
    min_backoff=15_000,
    max_backoff=300_000,
    jitter="full",
    retry_when=retry_when,
    throws=(ValueError,),
)
async def fetch_report(report_id: str) -> None:
    raise RuntimeError(f"temporary upstream failure for {report_id}")
```

Callback example:

```python
import fluxera


broker = fluxera.RedisBroker("redis://127.0.0.1:6379/15", namespace="callback-example")


def on_success(context: fluxera.OutcomeContext) -> None:
    print("finished", context.actor_name, context.message.message_id, context.result)


async def on_failure(context: fluxera.OutcomeContext) -> None:
    print("failed", context.failure_kind, context.exception_type)


@fluxera.actor(broker=broker, queue_name="callbacks")
async def notify_exhausted(payload: dict[str, object]) -> None:
    print("retry exhausted", payload["dead_letter_id"])


@fluxera.actor(
    broker=broker,
    queue_name="default",
    on_success=on_success,
    on_failure=on_failure,
    on_retry_exhausted="notify_exhausted",
)
async def generate_summary() -> dict[str, str]:
    return {"status": "ok"}
```

Rules of thumb:

- `throws` skips retries and goes straight to terminal handling
- `retry_when` overrides the simpler retry count rule
- `on_success` and `on_failure` can be sync or async callables
- `on_retry_exhausted` and `on_dead_lettered` can be callables or actor names
- actor callbacks receive JSON-safe payloads, not raw exception objects

## Distributed Concurrency Limits

Fluxera now ships a Redis-backed `ConcurrentRateLimiter` for application-level
distributed mutexes and small concurrency caps.

```python
import redis

import fluxera


client = redis.Redis.from_url("redis://127.0.0.1:6379/15")
limiter = fluxera.ConcurrentRateLimiter(client, "report:123", limit=1)

with limiter.acquire(raise_on_failure=False) as acquired:
    if not acquired:
        return
    print("exclusive section")
```

Use `aacquire()` when the limiter is created from an async Redis client or a
`fluxera.RedisBroker`.

The default limiter TTL is now aligned with the previous production wrappers:
`2 hours`, or `WORKER_CONCURRENCY_LOCK_TTL_MS` when that environment variable
is set.

The limiter can also be used from the CLI.

Probe a key without holding it:

```bash
fluxera rate-limit probe \
  --redis-url redis://127.0.0.1:6379/15 \
  --key report:123 \
  --format json
```

Run a command under a distributed mutex:

```bash
fluxera rate-limit run \
  --redis-url redis://127.0.0.1:6379/15 \
  --key report:123 \
  -- python3 scripts/generate_report.py
```

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
| Redis mixed long/short | `wall=1.542s`, `short_drain=0.089s` | `3.125s`, `1.660s (t=8)` / `1.681s`, `0.094s (t=32)` | transport advantage remains on real Redis |

See [BENCHMARK.md](docs/BENCHMARK.md) for the full methodology and numbers.

One nuance matters: with the safer default `spawn` process policy, cluster-scale CPU throughput is no longer universally faster than Dramatiq. Fluxera's strongest advantage is still async-heavy and mixed I/O workloads.

## Verification

The current release candidate was checked with:

- `python3 -m unittest discover -s tests -v`
- `python3 benchmarks/production_compare.py --profile smoke`
- `python3 benchmarks/redis_transport_compare.py --repeat 5 --long-io-secs 1.5`
- `/tmp/fluxera-release-venv/bin/python -m build --sdist --wheel`
- `/tmp/fluxera-release-venv/bin/python -m twine check dist/*`

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [FAQ](docs/FAQ.md)
- [Benchmark Results](docs/BENCHMARK.md)
- [Dead Letter and Retry](docs/DLQ.md)
- [Revision Management](docs/REVISION_MANAGEMENT.md)
- [System Design](docs/SYSTEM_DESIGN.md)
- [Deduplication and Idempotency](docs/DEDUP_IDEMPOTENCY.md)
- [Redis Lua Contract](docs/REDIS_LUA_CONTRACT.md)

## Current Limits

- public APIs may still change during the alpha period
- result backends are not implemented yet
- message registry garbage collection is still intentionally simple
