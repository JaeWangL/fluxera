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

`0.3.1` is the current alpha version.

The runtime, Redis transport v2, revision management, benchmark harnesses, and release packaging are in place, but APIs may still change as the project hardens.

## Install

```bash
pip install fluxera
```

For the RabbitMQ broker, install the optional extra:

```bash
pip install "fluxera[rabbitmq]"
```

For local development, `docker compose up -d` starts both Redis and RabbitMQ
(with the management plugin used by `join()` and queue diagnostics).

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

## Broker Feature Matrix

The worker runtime (execution lanes, retries, timeouts, callbacks, rate-limit
deferral) behaves the same on every broker. Transport-level features differ —
some need a shared KV store and are therefore Redis-only (see
[RabbitMQ Feature Parity Research](docs/RABBITMQ_PARITY.md) for why):

| Feature | StubBroker | RedisBroker | RabbitMQBroker |
| --- | :---: | :---: | :---: |
| At-least-once delivery, ack/reject | ✅ (in-memory) | ✅ | ✅ |
| Delayed messages (`delay=`) | ✅ | ✅ | ✅ (per-message TTL, FIFO order) |
| Dead letter records + requeue/purge admin | ✅ | ✅ | ✅ |
| Crash recovery / redelivery | — (process-local) | ✅ lease reclaim after `lease_seconds` | ✅ immediate on connection drop |
| Long-running tasks | ✅ | ✅ automatic lease renewal | ✅ set `consumer_timeout_seconds` above worst-case duration |
| `send_sync` producers | ✅ (fallback) | ✅ native sync client | ✅ dedicated loop thread |
| **Deduplication** (`deduplication`/`job_id`) | ❌ options ignored | ✅ simple / throttle / debounce+replace | ❌ raises `ValueError` |
| **Idempotency store** (begin/commit, fence tokens) | ❌ | ✅ | ❌ |
| **Serving-revision control plane** (blue/green handoff) | ✅ (in-process only) | ✅ | ❌ always accepting — mixed-generation consumption during rollouts |
| Worker presence registry | ✅ (in-process only) | ✅ | ❌ empty |
| Queue runtime diagnostics | partial | ✅ | ✅ (needs management API) |
| Distributed rate limits (`ConcurrentRateLimiter`) | ❌ | ✅ | ❌ |
| Admin dashboard / `monitor`·`dlq`·`revision` CLI | ❌ | ✅ | ❌ (CLI/admin construct `RedisBroker` from `--redis-url`) |
| `fluxera worker --broker module:attr` CLI | ✅ | ✅ | ✅ |

The bolded rows are the ones that fundamentally require a shared KV store:
RabbitMQ (with or without plugins) cannot express CAS, fence tokens, or
replace-pending-payload semantics. If you need those on a RabbitMQ transport,
pair it with Redis for the control plane, or use `RedisBroker` outright.

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
  --thread-concurrency 8 \
  --worker-presence-interval 5
```

For smoke tests and one-shot local runs, add `--exit-when-idle`.

## Worker Intake Pressure

Fluxera keeps queue-level consumer tasks for clear rollout, restart, and reclaim
boundaries, but it bounds the Redis pressure they can create while idle:

- `Worker(max_concurrent_consumer_receives=16)` limits concurrent Redis receive calls
- `Worker(consumer_idle_backoff_max=1.0)` backs off empty queues before polling again
- `Worker(worker_presence_interval=5.0)` keeps fast revision reads while publishing presence less often
- `RedisBroker(promote_due_interval_seconds=0.25)` avoids promoting delayed jobs on every idle poll
- `RedisBroker(stale_claim_interval_seconds=...)` defaults to a lease-aware interval for pending reclaim
- `RedisBroker` batches concurrent lease extensions with `XCLAIM JUSTID`
- `RedisBroker.join()` backs off unchanged queue-state polling from `0.1s` to `0.5s`

Serving revisions are read for all managed queues with one `MGET` per poll.
Presence is still published immediately when a queue changes between `accepting`
and `draining`, and its interval is capped at one third of the broker presence
TTL.

You can tune the worker defaults without code changes:

```bash
FLUXERA_MAX_CONCURRENT_CONSUMER_RECEIVES=16
FLUXERA_CONSUMER_IDLE_BACKOFF_MAX_SECONDS=1.0
FLUXERA_CONSUMER_IDLE_BACKOFF_MULTIPLIER=2.0
FLUXERA_WORKER_PRESENCE_INTERVAL_SECONDS=5.0
```

To reproduce idle pressure from many queues against a local Redis:

```bash
python3 benchmarks/redis_idle_consumer_pressure.py \
  --redis-url redis://127.0.0.1:6379/15 \
  --queues 58
```

## Producer APIs

Fluxera separates **message production** from **message execution**.

- `actor.send(...)` and `actor.send_sync(...)` produce messages
- worker execution lanes consume and execute those messages later

This is why `send_sync()` is not the same thing as the worker thread lane.

For `RedisBroker`, `send_sync()` now uses a real synchronous Redis producer
path. It is safe for normal blocking contexts such as schedulers, CLI tools,
and plain threads, but it still intentionally rejects calls made from inside an
already-running event loop.

## Scheduler Integration (APScheduler)

Fluxera can be used with APScheduler in the same style as Dramatiq.

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger


def enqueue_check_stale_documents() -> None:
    check_stale_documents.send_sync()


scheduler = BlockingScheduler(timezone="Asia/Seoul")
scheduler.add_job(
    enqueue_check_stale_documents,
    IntervalTrigger(minutes=30),
    id="check_stale_documents",
    replace_existing=True,
    max_instances=1,
    coalesce=True,
)
scheduler.start()
```

Guarantee boundary:

- APScheduler is responsible for timer firing behavior (`coalesce`, `max_instances`, misfire handling).
- Fluxera is responsible for durable enqueue and worker execution semantics after the message is enqueued.
- For Redis transport, Fluxera runs ack-late with lease renewal and stale reclaim (at-least-once).
- Exactly-once side effects still require idempotent handler logic.

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

## Runtime Monitoring And Admin Dashboard

Fluxera includes runtime monitoring commands and a lightweight `/admin` dashboard.

Get a JSON snapshot of worker and queue state:

```bash
fluxera monitor snapshot \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace hello-fluxera \
  --format json
```

Run a local admin dashboard:

```bash
fluxera monitor serve \
  --redis-url redis://127.0.0.1:6379/15 \
  --namespace hello-fluxera \
  --host 0.0.0.0 \
  --port 8090 \
  --snapshot-cache-seconds 25
```

Dashboard endpoints:

- `/admin`: auto-refreshing queue/worker dashboard
- `/admin/snapshot`: full runtime JSON payload backed by a recent-success cache
- `/healthz`: lightweight Redis readiness ping, not a full queue/worker snapshot

Long-lived admin servers reuse a small dedicated Redis readiness pool instead
of opening a connection for every probe. Runtime snapshots are cached by queue
filter; set the cache duration to `0` only when every request must force a fresh
Redis snapshot. The ASGI admin also applies a bounded refresh timeout.

Use your application's own liveness endpoint for "server up/down" monitoring.
Treat `/admin/snapshot` failures as Fluxera/Redis degradation signals instead
of process liveness failures.

The runtime snapshot includes:

- online/stale workers, revision, queue acceptance state
- queue backlog (`stream_ready`, `delayed`) and pending deliveries
- `pending_stale` count (idle deliveries likely stuck)
- `waiting_not_running` count for requests received but not yet executing

If you already run a root ASGI server (FastAPI/Starlette), mount Fluxera admin
into the existing router instead of using a separate port:

```python
from fastapi import FastAPI

import fluxera


app = FastAPI()
fluxera.mount_admin_asgi(
    app,
    mount_path="/admin/fluxera",
    redis_url="redis://127.0.0.1:6379/15",
    namespace="hello-fluxera",
)
```

Mounted endpoints become:

- `/admin/fluxera/`
- `/admin/fluxera/snapshot`
- `/admin/fluxera/healthz`

## Delivery Semantics

- Transport delivery is at-least-once.
- Deduplication is an enqueue-time admission policy, not exactly-once execution.
- Effectively-once side effects require idempotency keys or application-level dedupe.
- Redis workers renew leases for long-running tasks and reclaim stale pending deliveries.
- Redis 0.3.1 fences each acquisition by its PEL consumer and delivery generation.
  ACK, reject/DLQ, retry/defer enqueue, dedupe release, and lease renewal check
  that pair in the same Lua operation as the mutation. A stale attempt cannot
  finalize a newer receiver's work, even if ownership later returns to the same
  consumer. Normal renewal preserves the generation using `XCLAIM JUSTID`.
- A terminal Redis ACK removes the stream entry and releases its payload reference atomically.
- A confirmed ownership loss is recorded as `ownership_lost`; it does not invoke
  a stale attempt's completion/failure/retry callbacks. Already-running actor
  code, threads, and external requests can still finish; their business effects
  require application idempotency. Broker fencing is not exactly-once execution.
- RabbitMQ relies on native unacked redelivery: a dropped consumer connection requeues
  in-flight messages immediately, so there is no lease/heartbeat mechanism.

### Upgrading Redis consumers to 0.3.1

Message payloads and actor call signatures are unchanged. The Redis transport
requires Redis 6.2 or later; tests cover 6.2.24 and the production-compatible
7.4.3. Reclamation captures the PEL delivery counter atomically with `XCLAIM`;
renewal never changes that counter. Do not reset delivery counters with manual
`XCLAIM RETRYCOUNT` or delete/recreate consumer groups while deliveries are active.

An old 0.3.0 worker can still ACK or renew without checking ownership. Mixed
versions therefore do **not** provide the new fencing guarantee. Stop old workers
from acquiring new work, keep them alive until their executions and owned pending
deliveries have drained, then retire them before enabling the new release's
handoff policy. If that cannot be proven, use a bounded receive pause and reconcile
pending jobs; do not force-delete pending entries. Never infer drain from an
expired presence heartbeat alone. A rollback to 0.3.0 needs the same drain and
reintroduces its unfenced behavior; prefer rolling back application code while
retaining the patched transport. No global lease or new business schema is added.

`Broker.retry_delivery(consumer, delivery, message, delay=...)` is the runtime's
attempt-aware retry boundary. Redis overrides it atomically; the default retains
the previous publish-then-ACK behavior for RabbitMQ, Stub, and custom transports.
Calling `send_for_retry()` directly remains a producer operation and must not be
used by custom Redis consumer code as a substitute for fenced attempt settlement.

## Redis Registry And Cleanup

Redis queue discovery uses `namespace:registry:queues`. Enqueue, serving-revision,
and worker-presence writes all refresh the registry, so deleting the set is
self-healing while current writers are active.

`RedisBroker` defaults to `runtime_queue_discovery_mode="auto"`. It scans and
backfills legacy queue keys during a `300s` migration grace period, then reads
the registry directly. For a mixed-version rollout that can exceed that window,
keep operational discovery on `runtime_queue_discovery_mode="dual"` until all
old writers are gone. `reconcile_runtime_queue_registry(remove_stale=True)` can
remove entries only after an atomic recheck confirms that no runtime key exists.

Payloads use `namespace:message:{message_id}` plus a
`namespace:message_ref:{message_id}` reference counter. Success, terminal
failure, and integrity dead-letter paths release references atomically; retry
and requeue preserve the next attempt. Payloads orphaned by administrative
flushes or an interrupted producer still rely on `message_ttl_seconds`.

## RabbitMQ Broker

`RabbitMQBroker` is a drop-in transport for the same actor/worker runtime:

```python
import fluxera

broker = fluxera.RabbitMQBroker(
    "amqp://guest:guest@127.0.0.1:5672/",
    namespace="hello-fluxera",
)
```

Transport mapping:

- each Fluxera queue `q` is a durable AMQP queue `{namespace}:queue:{q}`
- delayed sends go through `{namespace}:delayed:{q}` using per-message TTL plus a
  dead-letter route back to the main queue (delays resolve in FIFO order, the same
  trade-off as Dramatiq delay queues)
- dead-letter records are stored as messages on `{namespace}:dlq:{q}` and support
  the same `get_dead_letter_records` / `requeue_dead_letter` / `purge_dead_letter` admin APIs
- `join()` and queue diagnostics combine AMQP queue counts, local in-flight counts,
  and the management HTTP API (derived from the AMQP URL, override with `management_url=`)

Differences from `RedisBroker` (see the [Broker Feature Matrix](#broker-feature-matrix)
for the full per-broker support table; the first two below need a shared KV store):

- message deduplication is not supported: sends carrying `deduplication`/`job_id`
  options raise `ValueError`
- the serving-revision control plane is not supported: every worker is always
  accepting. During a rollout, old and new workers consume the same queue
  concurrently: messages for a newly added actor that reach an old worker
  dead-letter as `invalid_configuration` (recoverable via `requeue_dead_letter`
  after the rollout), and messages whose handler changed may be processed
  silently by the old logic. See "Deploying Workers" below for the adopted
  deployment model
- there are no leases; recovery relies on RabbitMQ redelivering unacked messages
  when a consumer connection drops (`Delivery.redelivered` is preserved, so
  `on_worker_lost`/`redelivery_policy` behave the same)

### Deploying Workers (RabbitMQ): Stop-Then-Start

Because RabbitMQ has no serving-revision gating, the adopted deployment model
is **stop-then-start** — never run two worker generations against the same
queue at once:

1. Stop all old workers first. `Worker.stop(timeout=...)` (SIGTERM for the
   CLI worker) lets in-flight tasks finish for up to the stop timeout; anything
   still running is then cancelled and requeued, and unacked deliveries requeue
   the moment the connection closes — nothing is lost. **Set the stop timeout
   to at least your worst-case task duration** (e.g. one-hour tasks →
   `--stop-timeout 4200`, env `FLUXERA_STOP_TIMEOUT_SECONDS`), otherwise
   long-running work is cancelled and re-run from scratch on the new version.
2. Deploy and start the new workers. Producers can keep publishing during the
   gap — the backlog accumulates in RabbitMQ (disk-backed, so a deep backlog
   is safe) and drains once the new generation starts.

With `RedisBroker` this window does not exist: revision promotion drains old
workers while new ones take over, which is also gentler on very long tasks.

`RabbitMQBroker` works against [LavinMQ](https://lavinmq.com) unchanged — point
it at a LavinMQ URL (and pass `management_url=` if the HTTP API is not on the
derived host:15672). Verified empirically (LavinMQ 2.x, 2026-07): the full
integration test suite, the resilience scenarios (broker restart, hung broker,
`x-consumer-timeout` enforcement), and the transport benchmarks all pass.
Notable differences from RabbitMQ:

- consumer timeout is **disabled by default** (RabbitMQ defaults to 30
  minutes), so long-running tasks are safe out of the box; `x-consumer-timeout`
  is still honored when set, with tighter (~60s) enforcement granularity
- management API queue counts are computed at request time (no multi-second
  stats lag), which makes `join()` and diagnostics snappier
- quorum queues are silently downgraded to classic queues; clustering uses
  etcd-backed replication instead (confirms wait for fsync + in-sync followers
  since LavinMQ 2.9)
- message deduplication is built in (same argument conventions as the RabbitMQ
  community plugin): TTL-window (throttle-style) dedup works, but there is no
  ack-time key release, so Fluxera's Redis dedup modes still do not map onto it

### Long-Running Tasks (RabbitMQ)

If actors can run for a long time (for example sequentially calling several APIs
that each take minutes, or tasks approaching an hour), two settings matter:

- **`consumer_timeout_seconds` — the critical one.** RabbitMQ's *default*
  consumer timeout is 30 minutes: a delivery held unacked longer than that gets
  its channel closed (`PRECONDITION_FAILED`) and is redelivered while the
  original attempt is still running — duplicate execution at best. Set it well
  above your worst-case task duration:

  ```python
  broker = fluxera.RabbitMQBroker(
      url,
      # 최장 태스크(예: 1시간)의 2배 이상으로 설정
      consumer_timeout_seconds=2 * 3600,
  )
  ```

  This maps to the per-queue `x-consumer-timeout` argument. Note that queue
  arguments are fixed at declaration time — changing this value against
  already-declared queues fails until the queues are re-created.
- **Do not block the event loop.** AMQP heartbeats are serviced by the asyncio
  loop; async actors that await I/O keep the connection alive indefinitely
  (verified by tests spanning many heartbeat windows). CPU-heavy work must run
  on the thread or process lane — a coroutine that hogs the loop for longer
  than roughly three heartbeat intervals (default 60s each) gets the connection
  closed and the message redelivered mid-run.

`RedisBroker` needs no extra configuration for long tasks: workers renew the
delivery lease automatically for as long as the actor runs (verified by tests
spanning dozens of lease windows with a competing worker).

### Enqueue Throughput (RabbitMQ)

`send()` returns after the broker confirms the publish (fsync durability for
persistent messages). Confirms batch automatically when sends are concurrent —
overlapping `asyncio.gather(*(actor.send(...) ...))` calls share one channel
and wait for their confirms together, which is several times faster than a
sequential send loop (see the `enqueue-concurrent` benchmark scenario).

If enqueue throughput matters more than delivery guarantees on broker crash,
`RabbitMQBroker(publisher_confirms=False)` publishes fire-and-forget
(measured ~8x faster than sequential confirmed sends locally); unconfirmed
publishes can be lost if the broker crashes at the wrong moment, so keep the
default for at-least-once producers.

At-least-once still means at-least-once on both transports: a worker crash, a
broker restart, or a hung broker resolves to redelivery, so long-running actors
should be idempotent or use `on_worker_lost`/`redelivery_policy="fail"` to
detect recovered deliveries.

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
- `on_worker_lost` runs when a stale in-flight delivery is recovered (redelivered)
- `redelivery_policy="fail"` rejects recovered deliveries immediately instead of re-running actor code
- `Worker(on_worker_lost=..., default_redelivery_policy=...)` sets global defaults across actors
- actor callbacks receive JSON-safe payloads, not raw exception objects

Worker-global defaults example:

```python
import fluxera


async def mark_failed(context: fluxera.OutcomeContext) -> None:
    print("worker_lost", context.actor_name, context.message.message_id)


worker = fluxera.Worker(
    broker,
    concurrency=128,
    on_worker_lost=mark_failed,
    default_redelivery_policy="fail",
)
```

## Current Worker State

Fluxera can expose the currently executing invocation from inside actor code:

```python
import fluxera


@fluxera.actor
async def generate_report(history_id: str) -> None:
    state = fluxera.get_current_worker_state()
    if state is None:
        return

    if state.is_first_attempt:
        print("first attempt", state.message_id)
    else:
        print("retry", state.attempt, state.message_id)
```

For full field definitions and lane caveats, see
[docs/CURRENT_WORKER_STATE.md](docs/CURRENT_WORKER_STATE.md).

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
- `python3 benchmarks/broker_transport_compare.py --repeat 3`
- `python3 scripts/verify_rabbitmq_resilience.py` — RabbitMQ at-least-once
  verification that manipulates the local Docker broker: redelivery after a
  broker restart mid-task, after a hung (paused) broker detected via
  heartbeats, and after server-side `x-consumer-timeout` enforcement
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
- [RabbitMQ Feature Parity Research](docs/RABBITMQ_PARITY.md)
- [Redis Lua Contract](docs/REDIS_LUA_CONTRACT.md)

## Current Limits

- public APIs may still change during the alpha period
- result backends are not implemented yet
- abnormal or administratively removed message payloads still rely on registry TTL cleanup
