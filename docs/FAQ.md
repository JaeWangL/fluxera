# Fluxera FAQ

## Why does `send_sync()` still check `asyncio.get_running_loop()`?

`send_sync()` is a **producer-side synchronous API**.

That means it is intended for code that is already running in a normal blocking
context such as:

- a scheduler callback
- a management command
- a CLI script
- a plain background thread

It is **not** intended to be called from inside an already-running async event
loop.

Fluxera still checks `asyncio.get_running_loop()` because calling `send_sync()`
from inside an async function would block the current event loop thread. That
would stall your web server or worker loop.

So the guard remains on purpose:

- if there is **no** running loop, `send_sync()` is allowed
- if there **is** a running loop, `send_sync()` raises immediately

Use `await actor.send(...)` from async code.

Use `actor.send_sync(...)` from sync code.

## If Fluxera is async-native, why does `send_sync()` not use the worker thread lane?

Because the **worker execution lanes** and the **producer API** solve different
problems.

There are two separate phases in Fluxera:

1. message production
2. message execution

### Message production

This is what `actor.send()` and `actor.send_sync()` do.

They only enqueue a message into the broker.

### Message execution

This is what the worker runtime does after the message is consumed.

That is where Fluxera uses its three execution lanes:

- `async` lane
- `thread` lane
- `process` lane

So the thread lane is for running a **consumed actor body** such as a regular
`def` function.

It is **not** for producing messages.

## What does `send_sync()` do today?

For `RedisBroker`, `send_sync()` now uses a **real synchronous Redis producer
path** built on `redis.Redis`.

It does not call `asyncio.run()` around an async Redis client anymore when the
broker provides a native sync path.

That matters for schedulers and other long-lived sync processes because it
avoids cross-event-loop reuse bugs such as:

- `RuntimeError: Event loop is closed`

## When should I use `send()` vs `send_sync()`?

Use `send()` when you are already in async code:

```python
await generate_report.send(report_id)
```

Use `send_sync()` when you are in sync code:

```python
generate_report.send_sync(report_id)
```

Typical `send_sync()` callers:

- APScheduler blocking jobs
- synchronous cron entrypoints
- click/typer management commands
- ordinary threads

Typical `send()` callers:

- FastAPI async endpoints
- async services
- async schedulers
- worker-side async orchestration

## Does async-native mean Fluxera only works for async actors?

No.

`Fluxera` is **async-native**, not **async-only**.

Its execution model is:

- async actors run directly as `asyncio.Task`s
- sync actors run in the thread lane
- CPU-heavy actors can run in the process lane

The point is that async execution is the default center of the runtime, while
sync and CPU work are still supported explicitly.

## Why was the old `send_sync()` implementation unsafe with Redis?

The old path used `asyncio.run(self.send(...))`.

When combined with:

- a global `RedisBroker`
- a long-lived `redis.asyncio` client
- repeated sync enqueue calls

that could create and tear down a fresh event loop for every call while
reusing async Redis internals across loop boundaries.

That is what could eventually surface as:

- `RuntimeError: Event loop is closed`

The current sync producer path fixes that by using a proper sync Redis client
for sync enqueue operations.

## How do I mark business records as failed when a worker dies mid-task?

Use the actor-level redelivery hooks:

- `on_worker_lost`: invoked when Fluxera recovers a stale pending delivery
- `redelivery_policy="fail"`: optional fail-fast mode that dead-letters the recovered delivery immediately

Example:

```python
@fluxera.actor(
    on_worker_lost=mark_history_failed,
    redelivery_policy="fail",
)
async def generate_problem(history_id: str) -> None:
    ...
```

If you want automatic re-run behavior, keep the default
`redelivery_policy="continue"` and use `on_worker_lost` only for observability
or compensating state updates.
