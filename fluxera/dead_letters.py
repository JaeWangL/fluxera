from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional
from uuid import uuid4

from .encoder import decode_message_snapshot, encode_message_snapshot
from .message import Message, current_millis


ResolutionState = Literal["active", "requeued", "purged"]
FailureKind = Literal[
    "exception",
    "timeout",
    "worker_lost",
    "cancel_reject",
    "integrity_missing_payload",
    "integrity_decode_error",
    "operator_reject",
    "invalid_configuration",
]


def _default_dead_letter_id() -> str:
    return f"dlq-{uuid4().hex}"


@dataclass(slots=True)
class DeadLetterRecord:
    dead_letter_id: str = field(default_factory=_default_dead_letter_id)
    namespace: str = "fluxera"
    queue_name: str = ""
    actor_name: str = ""
    message_id: str = ""
    delivery_id: Optional[str] = None
    resolution_state: ResolutionState = "active"
    failure_kind: FailureKind = "exception"
    message_snapshot: Optional[dict[str, Any]] = None
    payload_available: bool = True
    attempt: int = 0
    max_retries: Optional[int] = None
    timeout_ms: Optional[int] = None
    execution_mode: str = "async"
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    traceback_text: Optional[str] = None
    cancel_reason: Optional[str] = None
    retry_delay_ms: Optional[int] = None
    dead_lettered_at_ms: int = field(default_factory=current_millis)
    message_timestamp_ms: Optional[int] = None
    received_at_ms: Optional[int] = None
    started_at_ms: Optional[int] = None
    failed_at_ms: Optional[int] = None
    worker_id: Optional[str] = None
    worker_revision: Optional[str] = None
    consumer_name: Optional[str] = None
    deduplication_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    origin_dead_letter_id: Optional[str] = None
    resolution_note: Optional[str] = None
    resolved_at_ms: Optional[int] = None
    retention_deadline_ms: Optional[int] = None

    @classmethod
    def from_message(
        cls,
        message: Message,
        *,
        namespace: str,
        queue_name: Optional[str] = None,
        actor_name: Optional[str] = None,
        delivery_id: Optional[str] = None,
        failure_kind: FailureKind,
        payload_available: bool = True,
        **overrides: Any,
    ) -> "DeadLetterRecord":
        snapshot = encode_message_snapshot(message) if payload_available else None
        return cls(
            namespace=namespace,
            queue_name=queue_name or message.queue_name,
            actor_name=actor_name or message.actor_name,
            message_id=message.message_id,
            delivery_id=delivery_id,
            failure_kind=failure_kind,
            message_snapshot=snapshot,
            payload_available=payload_available,
            message_timestamp_ms=message.message_timestamp,
            **overrides,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DeadLetterRecord":
        return cls(**data)

    def to_message(self) -> Optional[Message]:
        if self.message_snapshot is None:
            return None
        return decode_message_snapshot(self.message_snapshot)


def coerce_dead_letter_record(
    raw_record: Any,
    *,
    namespace: str,
    queue_name: str,
    actor_name: str,
    message: Optional[Message],
    delivery_id: Optional[str],
    consumer_name: Optional[str],
    failure_kind: FailureKind,
    execution_mode: str = "async",
    dead_lettered_at_ms: Optional[int] = None,
    retention_deadline_ms: Optional[int] = None,
) -> DeadLetterRecord:
    dead_lettered_at_ms = current_millis() if dead_lettered_at_ms is None else int(dead_lettered_at_ms)

    if isinstance(raw_record, DeadLetterRecord):
        data = raw_record.to_dict()
    elif isinstance(raw_record, dict):
        data = dict(raw_record)
    elif message is not None:
        data = DeadLetterRecord.from_message(
            message,
            namespace=namespace,
            queue_name=queue_name,
            actor_name=actor_name,
            delivery_id=delivery_id,
            failure_kind=failure_kind,
            execution_mode=execution_mode,
            consumer_name=consumer_name,
            dead_lettered_at_ms=dead_lettered_at_ms,
        ).to_dict()
    else:
        data = DeadLetterRecord(
            namespace=namespace,
            queue_name=queue_name,
            actor_name=actor_name,
            message_id="",
            delivery_id=delivery_id,
            failure_kind=failure_kind,
            message_snapshot=None,
            payload_available=False,
            execution_mode=execution_mode,
            consumer_name=consumer_name,
            dead_lettered_at_ms=dead_lettered_at_ms,
        ).to_dict()

    data.setdefault("namespace", namespace)
    data.setdefault("queue_name", queue_name)
    data.setdefault("actor_name", actor_name)
    data.setdefault("delivery_id", delivery_id)
    data.setdefault("consumer_name", consumer_name)
    data.setdefault("failure_kind", failure_kind)
    data.setdefault("execution_mode", execution_mode)
    data.setdefault("dead_lettered_at_ms", dead_lettered_at_ms)
    if retention_deadline_ms is not None and data.get("retention_deadline_ms") is None:
        data["retention_deadline_ms"] = int(retention_deadline_ms)
    return DeadLetterRecord.from_dict(data)


__all__ = [
    "coerce_dead_letter_record",
    "DeadLetterRecord",
    "FailureKind",
    "ResolutionState",
]
