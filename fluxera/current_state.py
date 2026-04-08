from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class CurrentWorkerState:
    """Execution context for the currently running actor invocation."""

    worker_id: str
    worker_revision: str
    actor_name: str
    queue_name: str
    message_id: str
    execution_mode: str
    attempt: int
    max_retries: int
    redelivered: bool
    timeout_ms: Optional[int] = None

    @property
    def is_retry(self) -> bool:
        return self.attempt > 0

    @property
    def is_first_attempt(self) -> bool:
        return self.attempt == 0


_CURRENT_WORKER_STATE: contextvars.ContextVar[Optional[CurrentWorkerState]] = contextvars.ContextVar(
    "fluxera_current_worker_state",
    default=None,
)


def get_current_worker_state() -> Optional[CurrentWorkerState]:
    """Return the current worker execution state for the active actor call."""

    return _CURRENT_WORKER_STATE.get()


def _set_current_worker_state(state: CurrentWorkerState) -> contextvars.Token[Optional[CurrentWorkerState]]:
    return _CURRENT_WORKER_STATE.set(state)


def _reset_current_worker_state(token: contextvars.Token[Optional[CurrentWorkerState]]) -> None:
    _CURRENT_WORKER_STATE.reset(token)

