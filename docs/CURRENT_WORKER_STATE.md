# Current Worker State

Fluxera provides a runtime helper to inspect the currently executing actor invocation.

## API

```python
import fluxera


state = fluxera.get_current_worker_state()
```

Return type:

- `None`: called outside of actor execution
- `fluxera.CurrentWorkerState`: called during an actor invocation

## What You Can Read

`CurrentWorkerState` contains:

- `worker_id`
- `worker_revision`
- `actor_name`
- `queue_name`
- `message_id`
- `execution_mode` (`async`, `thread`, `process`)
- `attempt`
- `max_retries`
- `redelivered`
- `timeout_ms`
- `is_first_attempt` (derived)
- `is_retry` (derived)

## First Attempt vs Retry

Use these fields:

- first attempt: `state.is_first_attempt` (`attempt == 0`)
- retry execution: `state.is_retry` (`attempt > 0`)

Example:

```python
import fluxera


@fluxera.actor
async def generate_report(report_id: str) -> None:
    state = fluxera.get_current_worker_state()
    if state is None:
        return

    if state.is_first_attempt:
        print("first attempt", state.message_id)
    else:
        print("retry attempt", state.attempt, state.message_id)
```

## Redelivery (Worker-Lost Recovery) vs Retry

- Retry is controlled by retry policy and increments `attempt`.
- Redelivery happens when an in-flight delivery is recovered by the broker and sets `redelivered=True`.

So a recovered message can be:

- `attempt == 0` and `redelivered == True` (no retry yet, recovered delivery)
- `attempt > 0` and `redelivered == False/True` (retry path, optionally also recovered)

## Lane Notes

- `async` lane: state is available inside actor code.
- `thread` lane: state is propagated to worker thread execution and available inside actor code.
- `process` lane: actor code runs in a different process; `get_current_worker_state()` is currently not propagated there and may return `None`.

