# Fluxera System Design

Status: Draft v0.1

Last Updated: 2026-03-28

## 1. Overview

Fluxera is a new async-native message processing system inspired by Dramatiq's API discipline and operational pragmatism, but redesigned around modern Python asynchronous execution.

Revision-aware rolling updates are documented separately in [REVISION_MANAGEMENT.md](REVISION_MANAGEMENT.md).

The main design goal is simple:

- Dramatiq-style ergonomics
- async-first execution
- I/O-friendly concurrency
- explicit backpressure
- task-based cancellation
- predictable broker semantics

Fluxera is not "Dramatiq with `asyncio` support". It is a new runtime in which the event loop is the primary execution engine, and threads are secondary tools used only for sync offload, broker adaptation, or isolation.

## 2. Why Fluxera Exists

Dramatiq's execution model is fundamentally thread-centric:

- broker consumers pull messages into an in-memory queue
- worker threads own messages until completion
- async actors are adapted into sync calls
- the worker thread waits for async completion before it can process the next message

That model is simple and effective for CPU-heavy or blocking synchronous work, but it has structural limitations for I/O-heavy workloads:

- concurrency is bounded by worker thread count, not by the event loop's ability to multiplex I/O
- async actors still consume one worker slot for their full lifetime
- a single bad coroutine can stall a shared loop and indirectly waste multiple waiting worker threads
- timeouts and shutdowns are modeled as thread interruption rather than task cancellation
- broker intake and local execution capacity are only loosely coupled

Fluxera replaces these assumptions with task ownership, structured concurrency, and lease-aware backpressure.

## 3. Design Principles

### 3.1 Preserve Dramatiq's Good Parts

We want to preserve the following qualities from Dramatiq:

- small, understandable public API
- explicit actor objects
- middleware as first-class extension points
- operationally conservative defaults
- ack-late delivery semantics
- multi-process deployment as the scaling primitive for CPU

### 3.2 Reject Thread Ownership

In Fluxera, a message is not owned by a worker thread. It is owned by a runtime task record that tracks:

- broker delivery state
- execution state
- timeout and cancellation state
- retry metadata
- result metadata
- metrics and tracing context

Threads are not the scheduling primitive.

### 3.3 One Good Event Loop Is Better Than Many Idle Threads

The default execution engine is one event loop per worker process running many actor tasks concurrently. This is the normal and preferred mode for I/O-bound workloads.

Multiple loops are a specialized isolation feature, not the default scaling strategy.

### 3.4 Scale I/O with Tasks, CPU with Processes

Fluxera scales different work in different ways:

- async I/O: many tasks on one loop
- synchronous blocking code: bounded thread pool
- CPU-bound code: restartable process lane with bounded worker slots

This separation is a core architectural rule.

### 3.5 Backpressure Must Be Explicit

Broker intake must not significantly exceed local capacity. The runtime should only admit as much work as it can execute, renew, and shut down safely.

### 3.6 Cancellation Should Use Native Async Semantics

Timeouts and shutdown should use task cancellation, not asynchronous thread exception injection. Actor cleanup must use normal `try/finally` and `CancelledError`-aware logic.

## 4. Non-Goals

Fluxera v1 does not aim to:

- provide transparent compatibility with every Dramatiq middleware
- auto-convert blocking sync code into efficient async code
- guarantee exactly-once delivery
- hide differences between brokers completely
- solve CPU-bound parallelism inside one event loop

Compatibility is a migration concern, not the core design constraint.

## 5. High-Level Runtime Model

Each Fluxera worker process runs a supervisor-owned runtime built around one primary event loop.

Core services inside a worker process:

1. Ingress Service
2. Admission Controller
3. Scheduler
4. Executor
5. Lease Manager
6. Retry and Delay Manager
7. Result Writer
8. Metrics and Tracing Exporter
9. Shutdown Coordinator

These services run inside one structured task tree.

```text
Supervisor Process
  -> Worker Process
       -> asyncio.Runner(loop_factory=...)
            -> Runtime TaskGroup
                 -> ingress tasks per queue/binding
                 -> scheduler task
                 -> lease renewal task
                 -> retry/delay task
                 -> metrics task
                 -> health endpoint task
                 -> actor execution tasks
```

## 6. Process Model

### 6.1 Master and Worker Processes

Fluxera keeps Dramatiq's process-oriented operational model:

- one parent CLI process supervises worker processes
- worker processes are independent failure domains
- process count is the main scaling control for CPU and memory isolation

This preserves operational familiarity and avoids trying to solve all resource isolation inside one Python process.

### 6.2 Event Loop per Worker Process

Each worker process runs one primary event loop by default.

Rationale:

- one loop can efficiently multiplex thousands of well-behaved async sockets
- cross-loop coordination is expensive and awkward
- context propagation, task cancellation, and metrics are simpler on one loop
- most I/O scalability problems come from task admission and blocking code, not from needing multiple loops

### 6.3 Optional Loop Shards

Fluxera may later support loop shards for isolation, not baseline throughput:

- dedicated loop for specific queues
- dedicated loop for specific actors
- quarantine loop for suspicious blocking integrations

This is an advanced feature that should be opt-in and policy-driven. It is not required for v1.

## 7. Actor Execution Model

### 7.1 Actor Types

Fluxera supports three execution modes:

- `async`: native coroutine actor, executed directly on the event loop
- `thread`: synchronous actor executed in a bounded thread pool
- `process`: CPU-bound actor executed in a bounded process lane with dedicated worker slots

Example intent:

```python
@fluxera.actor
async def fetch(url):
    ...

@fluxera.actor(execution="thread")
def legacy_sdk_call(user_id):
    ...

@fluxera.actor(execution="process")
def render_video(job_id):
    ...
```

Default execution mode:

- `async def` -> `async`
- `def` -> `thread`

This keeps ergonomics simple while preserving correct expectations.

### 7.2 No Sync Bridge for Async Actors

Unlike Dramatiq, async actors are not wrapped into sync callables. They remain first-class coroutine functions and are scheduled as tasks.

That means:

- no worker slot is held while awaiting I/O
- task cancellation is native
- local concurrency is not tied to thread count

### 7.3 Structured Execution

Actor executions live inside a runtime task group. If the runtime shuts down:

- intake stops first
- no new tasks are admitted
- in-flight tasks receive cancellation or grace treatment according to policy
- leases continue to renew only while tasks remain eligible to finish

This avoids orphan execution state.

## 8. Message Lifecycle

Fluxera models message processing as a state machine.

```text
queued
  -> delivered
  -> admitted
  -> running
  -> succeeded | failed | cancelled | timed_out
  -> retry_scheduled | dead_lettered | acked
```

Each in-flight message is represented by a `TaskRecord`.

Suggested fields:

- `message`
- `delivery`
- `received_at`
- `admitted_at`
- `started_at`
- `attempt`
- `queue_name`
- `actor_name`
- `execution_mode`
- `timeout_deadline`
- `lease_deadline`
- `result_future`
- `state`
- `cancel_reason`
- `trace_context`

This object is the unit of runtime truth.

## 9. Broker Abstraction

Fluxera's broker abstraction must be async from the bottom up.

### 9.1 Core Broker Contract

```python
class AsyncBroker:
    async def send(self, message, *, delay=None) -> MessageHandle: ...
    async def open_consumer(self, binding) -> AsyncConsumer: ...
    async def close(self) -> None: ...
```

```python
class AsyncConsumer:
    async def receive(self, *, limit: int, timeout: float | None) -> list[Delivery]: ...
    async def ack(self, delivery: Delivery) -> None: ...
    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None: ...
    async def extend_lease(self, delivery: Delivery, *, seconds: float) -> None: ...
    async def close(self) -> None: ...
```

### 9.2 Delivery Semantics

The broker must expose a `Delivery` object that carries:

- decoded message payload
- transport metadata
- broker-native identifier
- redelivery flag
- lease or visibility metadata when available

The runtime only deals in deliveries, never raw broker frames.

### 9.3 Lease-Aware Design

Fluxera normalizes around ack-late and lease-aware delivery:

- if the broker has native visibility timeout, Fluxera renews it
- if the broker uses open-channel unacked delivery, Fluxera keeps the channel alive and treats that as the lease
- if the broker has no lease extension primitive, the runtime must document the limitation explicitly

This contract is necessary for safe long-running async work.

### 9.4 Planned Broker Implementations

Initial target set:

- Redis Streams broker
- RabbitMQ broker

Initial recommendation:

- build Redis Streams first as the reference async broker
- build RabbitMQ second using an async client

Redis Streams is a strong fit for lease-aware delivery and consumer-group recovery. RabbitMQ remains important for Dramatiq users and mature deployments.

## 10. Admission and Backpressure

Backpressure is a first-class runtime concern.

### 10.1 Capacity Sources

Fluxera tracks capacity along several axes:

- global in-flight limit
- per-queue concurrency limit
- per-actor concurrency limit
- per-execution-lane concurrency limit
- thread pool saturation
- process pool saturation
- broker-specific prefetch budget

### 10.2 Admission Rules

Messages are not executed immediately after delivery. They first pass through admission control.

Admission checks:

- global permits available
- queue permits available
- actor permits available
- execution-mode-specific capacity available
- runtime health within policy thresholds

If admission fails:

- intake slows or pauses
- local pending queue is bounded
- broker prefetch is reduced or not replenished

In the current prototype, admission is lane-based:

- async work enters an async-ready queue
- thread work enters a thread-ready queue
- process work enters a process-ready queue

Each lane has its own permits and bounded backlog. This prevents long-running CPU work from consuming async admission capacity and lets I/O-heavy actors continue to make progress while process slots are busy.

### 10.3 Small Prefetch by Default

Dramatiq uses prefetch partly to hide broker I/O behind worker threads. Fluxera does not need large speculative intake for that reason.

Fluxera defaults to:

- prefetch near available capacity
- small local pending buffers
- dynamic refill based on permit release

This lowers memory use and improves fairness across workers.

### 10.4 Fair Scheduling

The scheduler should support weighted fair queueing across bindings so that:

- noisy queues do not starve latency-sensitive queues
- per-queue admission is enforceable
- retry floods do not monopolize execution

This can be implemented with a small weighted ready-queue layer above raw deliveries.

## 11. Event Loop Strategy

### 11.1 Default Loop

Fluxera uses stdlib `asyncio` as the default runtime dependency.

Reasons:

- no hard dependency on alternate runtimes
- modern Python already provides `TaskGroup`, `Runner`, and better cancellation primitives
- lower conceptual and packaging complexity

### 11.2 Optional uvloop

Fluxera should support `uvloop` as an optional performance enhancement on supported platforms.

Policy:

- never require `uvloop`
- detect it when installed through an extra, for example `fluxera[uvloop]`
- use `asyncio.Runner(loop_factory=uvloop.new_event_loop)` in worker processes when enabled

`uvloop` is a loop optimization, not an architectural dependency. It can improve throughput and latency for network-heavy workloads, but it does not replace correct admission control or non-blocking actor code.

### 11.3 Loop Health Monitoring

The runtime must continuously measure:

- loop lag
- scheduler latency
- ready-queue depth
- in-flight task count
- task cancellation latency
- thread/process offload queue depth

Loop lag is the main signal for event-loop distress.

### 11.4 Isolation Policy

Fluxera should not split work across loops reactively by default. A busy but healthy loop is normal.

Loop sharding may be triggered only by explicit policy, such as:

- actor tagged `isolation_group="billing"`
- queue tagged `latency_sensitive=True`
- operator-configured shard binding

Automatic loop spawning is dangerous without strong heuristics and should not ship in v1.

## 12. Middleware Model

Fluxera keeps middleware, but makes it async-capable.

### 12.1 Hook Philosophy

Hooks should be:

- few in number
- lifecycle-oriented
- transport-agnostic when possible
- allowed to be sync or async

### 12.2 Proposed Hook Set

- `before_send`
- `after_send`
- `before_admit`
- `after_admit`
- `before_execute`
- `after_execute`
- `before_ack`
- `after_ack`
- `before_reject`
- `after_reject`
- `before_retry`
- `after_retry`
- `worker_startup`
- `worker_shutdown`

### 12.3 Task Context

Current message access should use `contextvars`, not thread-local storage.

Suggested context values:

- current message
- current task record
- actor name
- attempt number
- trace span

This fits async-native execution naturally.

## 13. Timeouts and Cancellation

### 13.1 Task-Native Time Limits

Per-message timeouts should use native async timeouts:

- `asyncio.timeout()` in the core runtime
- cancellation propagated into the actor task

This provides predictable semantics for well-behaved async code.

### 13.2 Shutdown Behavior

Shutdown should proceed in phases:

1. stop new intake
2. stop refilling broker credit
3. wait for fast completion during grace period
4. cancel remaining tasks according to policy
5. ack, retry, or release deliveries according to final state

### 13.3 Cancellation Contract

Actor authors are expected to write cancellation-safe code:

- `try/finally` for cleanup
- do not swallow `CancelledError` unless they re-raise after cleanup
- use idempotency when side effects may be retried

This must be documented as a first-class operational rule.

### 13.4 Process Lane Cancellation

Process actors need stronger semantics than thread or task cancellation alone.

The current runtime model is:

- each process lane owns a bounded set of long-lived worker slots
- each slot runs one task at a time
- if a process task times out, is cancelled, or its worker exits unexpectedly, that slot is terminated
- the runtime replaces the terminated slot before admitting more process work

This is stricter than a vanilla `ProcessPoolExecutor` model because timeout and shutdown should release real CPU capacity rather than leave runaway child work behind.

## 14. Retries, Delays, and Dead Letters

### 14.1 Retry Policy

Retries are modeled as message policy, not thread behavior.

Suggested options:

- max retries
- backoff strategy
- retryable exception filter
- jitter
- dead-letter routing

### 14.2 Safe Retry Scheduling

The runtime must not ack a failed delivery until the retry message has been durably scheduled, unless the broker provides a single atomic requeue primitive.

This is a core safety rule.

### 14.3 Delayed Messages

Delay support should be broker-native when possible.

Fallback strategy:

- maintain a scheduler stream or delayed set in the broker
- a small runtime scheduler service promotes due messages back to their execution queues

Delayed execution must not rely on large in-memory delay queues inside workers.

## 15. Results

Results remain optional.

Design rules:

- no implicit result storage
- async backends
- result writes happen after successful completion and before ack finalization when policy requires
- failed results store structured exception metadata

The result pipeline should not block the event loop unnecessarily. If a result backend is slow, it must have bounded concurrency and explicit metrics.

## 16. Observability

Fluxera should ship with first-class observability for async runtime health.

### 16.1 Required Metrics

- messages received total
- messages admitted total
- messages completed total
- messages failed total
- messages retried total
- messages dead-lettered total
- in-flight task count
- scheduler ready depth
- broker pending depth when available
- event loop lag histogram
- actor duration histogram
- ack latency histogram
- lease extension count
- cancellation count
- thread pool queue depth
- process pool queue depth

For the current prototype, process-lane health should also include:

- live process slot count
- slot restart count
- process cancellation latency

### 16.2 Tracing

Tracing must propagate:

- message id
- parent trace context
- queue
- actor
- attempt

Every message execution should become a span with clearly separated phases:

- receive
- admit
- execute
- result write
- ack

## 17. API Shape

Fluxera should feel familiar to Dramatiq users.

Example:

```python
import fluxera

broker = fluxera.RedisStreamsBroker(url="redis://127.0.0.1:6379/0")
fluxera.set_broker(broker)


@fluxera.actor(
    queue_name="default",
    max_retries=8,
    concurrency=200,
    timeout=30_000,
)
async def fetch_url(url: str) -> str:
    ...


async def main():
    await fetch_url.send("https://example.com")
```

Key differences from Dramatiq:

- `send()` is async in the core API
- async actors are native
- sync actors declare or infer offload mode
- concurrency options describe task admission, not thread count

For convenience, a synchronous enqueue helper may exist for non-async callers, but it should be an adapter, not the core API.

## 18. Proposed Package Layout

```text
fluxera/
  fluxera/
    __init__.py
    actor.py
    app.py
    broker.py
    message.py
    errors.py
    runtime/
      __init__.py
      worker.py
      supervisor.py
      scheduler.py
      admission.py
      execution.py
      leases.py
      shutdown.py
      context.py
      health.py
    brokers/
      __init__.py
      redis_streams.py
      rabbitmq.py
    middleware/
      __init__.py
      base.py
      retries.py
      results.py
      metrics.py
      tracing.py
      current_message.py
    results/
      __init__.py
      base.py
      redis.py
```

This mirrors Dramatiq's preference for small top-level modules while giving the runtime a dedicated home.

## 19. Delivery Guarantees

Fluxera should provide:

- at-least-once delivery
- ack-late processing
- bounded local prefetch
- graceful lease renewal for long-running tasks
- exactly-once admission when a deduplication policy is configured and the broker accepts the message atomically
- effectively-once effects only when an idempotency policy is configured and the downstream side effect cooperates with the same key or fence token

Fluxera should not claim:

- exactly-once execution
- exactly-once side effects from broker mechanics alone
- transparent safety for non-idempotent handlers
- safe cancellation of arbitrary blocking code

These guarantees must stay explicit.

Related design notes:

- [Deduplication and Idempotency Design](./DEDUP_IDEMPOTENCY.md)
- [Redis Lua Contract Draft](./REDIS_LUA_CONTRACT.md)

## 20. Operational Defaults

Recommended initial defaults:

- processes: CPU count or operator-specified
- primary event loops per process: 1
- global async concurrency per process: 1_000
- local pending buffer: 2x available permits, capped
- thread pool size: small and bounded
- process lane size: CPU count capped by configured async concurrency, started lazily when process actors run
- graceful shutdown timeout: 30 seconds
- lease heartbeat interval: 25-33 percent of lease window
- `uvloop`: disabled unless installed and enabled

These defaults should be revisited with benchmarks, but they reflect Fluxera's async-first stance.

## 21. Migration Strategy from Dramatiq

Fluxera is not drop-in compatible, but should be migration-friendly.

Migration aids:

- actor decorator with familiar options
- middleware concepts preserved
- Redis and RabbitMQ support retained
- queue naming and message metadata kept simple
- sync actor support available through explicit offload

Breaking changes to embrace:

- async enqueue as the primary API
- task-based runtime hooks
- no thread interruption model
- runtime capacity options instead of worker-thread-centric tuning

## 22. Risks

### 22.1 Async-Native Does Not Fix Blocking Libraries

If users write:

- `async def` actors that call `requests`
- `async def` actors that call `time.sleep`
- CPU-heavy code on the main loop

the loop will still stall.

Fluxera must provide:

- documentation
- metrics
- isolation options
- sync offload modes

It cannot make bad async code good.

### 22.2 Broker Differences Matter

RabbitMQ and Redis Streams do not expose identical delivery models. The abstraction must normalize behavior where reasonable and document transport-specific limits where not.

### 22.3 Middleware Compatibility Is Limited

A Dramatiq middleware that assumes worker-thread identity or thread-local state is not directly portable.

### 22.4 Too Much Magic Would Recreate Old Problems

Automatic loop spawning, transparent sync wrapping, or overly aggressive speculative prefetch would make the runtime harder to reason about. Fluxera should prefer explicitness.

## 23. Open Questions

These questions do not block the design but should be resolved early:

1. Should Redis Streams be the required first broker, or should RabbitMQ and Redis launch together?
2. Should `send()` be async-only, or should the top-level actor expose both `send()` and `send_sync()`?
3. Should loop sharding exist in v1 behind an internal API, or be deferred entirely?
4. Should the core runtime stay pure `asyncio`, or adopt AnyIO internally for stronger cross-backend abstractions?
5. How much broker-specific policy should be configurable per queue versus per actor?
6. Resolved: Fluxera defaults the process lane to `spawn` and only uses another start method when explicitly configured. This trades some startup cost for predictable multithreaded safety across platforms.

## 24. Recommended v1 Scope

Fluxera v1 should aim for the smallest system that proves the async-native architecture:

1. Core actor API
2. Async runtime with one event loop per process
3. Structured task execution with native cancellation
4. Global, queue, and actor concurrency controls
5. Execution-lane admission for async, thread, and process work
6. Restartable process slots with timeout and cancellation recycling
7. Redis Streams broker as the reference implementation
8. Retry, delay, and dead-letter support
9. Current-message middleware via `contextvars`
10. Prometheus-style metrics for loop lag and execution health
11. Optional `uvloop` support
12. Deduplication policies and a Redis-backed idempotency store for effectively-once workflows

Everything else should be staged after the runtime contract is stable.

## 25. Implementation Roadmap

### Phase 0: Skeleton

- create package layout
- define core message, actor, and broker interfaces
- create worker process bootstrap with `asyncio.Runner`

### Phase 1: Runtime Core

- implement admission controller
- implement scheduler
- implement execution task records
- implement graceful shutdown
- implement context propagation

### Phase 2: Redis Streams Broker

- send
- receive
- ack
- reject
- lease extension
- delayed delivery support
- atomic enqueue-time deduplication
- idempotency state scripts and result short-circuit paths

Initial reference shape:

- stream key: `namespace:stream:{queue}` storing `message_id` entries
- delayed key: `namespace:delayed:{queue}` as a sorted set of `message_id` values keyed by delivery timestamp
- message key: `namespace:message:{message_id}` storing the encoded Fluxera message
- dead-letter key: `namespace:dead:{queue}`
- payload encoding: JSON by default for safe message primitives, with explicit opt-in required for pickle-style trusted payloads
- group model: one consumer group per queue namespace, many ephemeral consumers
- stale recovery: cursorized reclaim of expired pending entries to a live consumer, while suppressing duplicates for deliveries already active on that consumer
- long-running tasks: runtime heartbeat renews the lease through the consumer contract while the task remains in good standing

### Phase 3: Middleware and Policy

- retries
- current message
- metrics
- results

### Phase 4: Operational Tooling

- CLI
- health endpoints
- Prometheus exporter
- logging and tracing

### Phase 5: RabbitMQ Broker

- async transport integration
- broker-specific lease model
- shutdown and redelivery validation

## 26. Final Position

Fluxera should be built as a task-native, lease-aware, async-first runtime.

The crucial architecture choice is not "use more event loops". It is:

- keep one well-observed loop per worker process by default
- run async actors as native tasks
- keep intake tightly coupled to capacity
- use threads and processes only as bounded offload tools
- make cancellation and shutdown task-native

That is the cleanest path to a Dramatiq successor that is genuinely I/O-friendly rather than thread-centric with async adapters layered on top.

## Appendix A. Deduplication and Idempotency

Fluxera treats deduplication and idempotency as explicit policy layers above at-least-once delivery:

- deduplication is an enqueue admission policy
- idempotency is an execution and side-effect coordination policy
- neither changes the core broker claim that deliveries may be retried or redelivered

For the detailed design, see:

- [Deduplication and Idempotency Design](./DEDUP_IDEMPOTENCY.md)
- [Redis Lua Contract Draft](./REDIS_LUA_CONTRACT.md)
