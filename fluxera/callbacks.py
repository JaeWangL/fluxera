from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from .dead_letters import DeadLetterRecord
from .encoder import encode_message_snapshot
from .message import Message


OutcomeEvent = Literal["success", "failure", "retry_scheduled", "retry_exhausted", "redelivered", "worker_lost"]


@dataclass(slots=True)
class OutcomeContext:
    event: OutcomeEvent
    message: Message
    queue_name: str
    actor_name: str
    attempt: int
    max_retries: int
    execution_mode: str
    redelivered: bool = False
    worker_id: Optional[str] = None
    worker_revision: Optional[str] = None
    result: Any = None
    failure_kind: Optional[str] = None
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    traceback_text: Optional[str] = None
    retry_delay_ms: Optional[int] = None
    dead_letter_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event": self.event,
            "queue_name": self.queue_name,
            "actor_name": self.actor_name,
            "message_id": self.message.message_id,
            "message_timestamp_ms": self.message.message_timestamp,
            "attempt": self.attempt,
            "max_retries": self.max_retries,
            "execution_mode": self.execution_mode,
            "redelivered": self.redelivered,
            "worker_id": self.worker_id,
            "worker_revision": self.worker_revision,
            "failure_kind": self.failure_kind,
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "traceback_text": self.traceback_text,
            "retry_delay_ms": self.retry_delay_ms,
            "dead_letter_id": self.dead_letter_id,
            "message": encode_message_snapshot(self.message),
            "result_available": self.result is not None,
        }
        if self.result is not None:
            payload["result_repr"] = repr(self.result)
        return payload


@dataclass(slots=True)
class DeadLetterContext:
    record: DeadLetterRecord

    def to_dict(self) -> dict[str, Any]:
        return self.record.to_dict()


__all__ = [
    "DeadLetterContext",
    "OutcomeContext",
    "OutcomeEvent",
]
