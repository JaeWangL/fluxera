from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from .dead_letters import DeadLetterRecord
from .errors import ActorNotFound
from .message import Message

if TYPE_CHECKING:
    from .actor import Actor


global_broker: Optional["Broker"] = None


def get_broker() -> "Broker":
    if global_broker is None:
        raise RuntimeError("Global broker not set. Call fluxera.set_broker(...) before declaring actors.")
    return global_broker


def set_broker(broker: "Broker") -> None:
    global global_broker
    global_broker = broker


@dataclass(slots=True)
class Delivery:
    """A broker delivery object wrapping a message and transport metadata."""

    message: Message
    redelivered: bool = False
    transport_id: Optional[str] = None
    lease_deadline: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.message, name)


class Consumer(ABC):
    """Async message consumer interface."""

    @abstractmethod
    async def receive(self, *, limit: int, timeout: Optional[float]) -> list[Delivery]:
        raise NotImplementedError

    @abstractmethod
    async def ack(self, delivery: Delivery) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None:
        raise NotImplementedError

    async def extend_lease(self, delivery: Delivery, *, seconds: float) -> None:
        """Extend a delivery lease when the broker supports it."""

    async def close(self) -> None:
        """Close any transport resources held by the consumer."""


class Broker(ABC):
    """Base class for Fluxera brokers."""

    def __init__(self) -> None:
        self.actors: dict[str, "Actor"] = {}
        self.queues: set[str] = set()

    def declare_actor(self, actor: "Actor") -> None:
        self.declare_queue(actor.queue_name)
        self.actors[actor.actor_name] = actor

    def declare_queue(self, queue_name: str) -> None:
        if queue_name in self.queues:
            return

        self._declare_queue(queue_name)
        self.queues.add(queue_name)

    def get_actor(self, actor_name: str) -> "Actor":
        try:
            return self.actors[actor_name]
        except KeyError:
            raise ActorNotFound(actor_name) from None

    def get_declared_queues(self) -> set[str]:
        return self.queues.copy()

    @abstractmethod
    def _declare_queue(self, queue_name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(self, message: Message, *, delay: Optional[float] = None) -> Message:
        raise NotImplementedError

    @abstractmethod
    async def open_consumer(self, queue_name: str, *, prefetch: int = 1) -> Consumer:
        raise NotImplementedError

    async def flush(self, queue_name: str) -> None:
        raise NotImplementedError

    async def join(self, queue_name: str) -> None:
        raise NotImplementedError

    async def get_dead_letters(self, queue_name: str) -> list[Message]:
        del queue_name
        return []

    async def get_dead_letter_records(self, queue_name: str) -> list[DeadLetterRecord]:
        del queue_name
        return []

    async def get_dead_letter_record(self, queue_name: str, dead_letter_id: str) -> Optional[DeadLetterRecord]:
        del queue_name, dead_letter_id
        return None

    async def requeue_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        del queue_name, dead_letter_id, note
        return None

    async def purge_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        del queue_name, dead_letter_id, note
        return None

    async def ensure_serving_revision(self, queue_name: str, worker_revision: str) -> str:
        del queue_name
        return worker_revision

    async def get_serving_revision(self, queue_name: str) -> Optional[str]:
        del queue_name
        return None

    async def promote_serving_revision(
        self,
        queue_name: str,
        revision: str,
        *,
        expected_revision: Optional[str] = None,
    ) -> bool:
        del queue_name, revision, expected_revision
        return False

    async def register_worker_revision(
        self,
        *,
        worker_id: str,
        worker_revision: str,
        queue_states: dict[str, str],
        runtime_state: Optional[dict[str, Any]] = None,
    ) -> None:
        del worker_id, worker_revision, queue_states, runtime_state

    async def unregister_worker_revision(self, *, worker_id: str, queue_names: set[str]) -> None:
        del worker_id, queue_names

    async def close(self) -> None:
        """Close this broker and release external resources."""
