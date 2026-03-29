from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from ..broker import Broker, Consumer, Delivery
from ..dead_letters import DeadLetterRecord, coerce_dead_letter_record
from ..errors import QueueNotFound
from ..message import Message, current_millis


@dataclass(slots=True)
class _StubEntry:
    message: Message
    redelivered: bool = False


class StubBroker(Broker):
    """An in-memory broker suitable for tests and local benchmarks."""

    def __init__(self) -> None:
        super().__init__()
        self.queue_objects: dict[str, asyncio.Queue[_StubEntry]] = {}
        self.dead_letters_by_queue: defaultdict[str, list[DeadLetterRecord]] = defaultdict(list)
        self.delayed_tasks_by_queue: defaultdict[str, set[asyncio.Task[None]]] = defaultdict(set)
        self.serving_revisions: dict[str, str] = {}
        self.worker_revisions: dict[str, str] = {}
        self.worker_queue_states: dict[str, dict[str, str]] = {}

    @property
    def dead_letters(self) -> list[DeadLetterRecord]:
        return [record for records in self.dead_letters_by_queue.values() for record in records]

    def _declare_queue(self, queue_name: str) -> None:
        self.queue_objects[queue_name] = asyncio.Queue()

    async def send(self, message: Message, *, delay: Optional[float] = None) -> Message:
        self.declare_queue(message.queue_name)
        queue = self.queue_objects[message.queue_name]

        if delay is None:
            await queue.put(_StubEntry(message))
            return message

        async def put_later() -> None:
            await asyncio.sleep(delay)
            await queue.put(_StubEntry(message))

        task = asyncio.create_task(put_later(), name=f"fluxera-stub-delay:{message.queue_name}:{message.message_id}")
        delayed_tasks = self.delayed_tasks_by_queue[message.queue_name]
        delayed_tasks.add(task)
        task.add_done_callback(delayed_tasks.discard)
        return message

    async def open_consumer(self, queue_name: str, *, prefetch: int = 1) -> Consumer:
        self.declare_queue(queue_name)
        return _StubConsumer(self, queue_name)

    async def ensure_serving_revision(self, queue_name: str, worker_revision: str) -> str:
        self.declare_queue(queue_name)
        return self.serving_revisions.setdefault(queue_name, worker_revision)

    async def get_serving_revision(self, queue_name: str) -> Optional[str]:
        return self.serving_revisions.get(queue_name)

    async def promote_serving_revision(
        self,
        queue_name: str,
        revision: str,
        *,
        expected_revision: Optional[str] = None,
    ) -> bool:
        current_revision = self.serving_revisions.get(queue_name)
        if expected_revision is not None and current_revision != expected_revision:
            return False

        self.serving_revisions[queue_name] = revision
        return True

    async def register_worker_revision(
        self,
        *,
        worker_id: str,
        worker_revision: str,
        queue_states: dict[str, str],
    ) -> None:
        self.worker_revisions[worker_id] = worker_revision
        self.worker_queue_states[worker_id] = queue_states.copy()

    async def unregister_worker_revision(self, *, worker_id: str, queue_names: set[str]) -> None:
        del queue_names
        self.worker_revisions.pop(worker_id, None)
        self.worker_queue_states.pop(worker_id, None)

    async def flush(self, queue_name: str) -> None:
        try:
            queue = self.queue_objects[queue_name]
        except KeyError:
            raise QueueNotFound(queue_name) from None

        for task in list(self.delayed_tasks_by_queue[queue_name]):
            task.cancel()

        if self.delayed_tasks_by_queue[queue_name]:
            await asyncio.gather(*self.delayed_tasks_by_queue[queue_name], return_exceptions=True)

        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                queue.task_done()

        self.dead_letters_by_queue[queue_name].clear()
        self.serving_revisions.pop(queue_name, None)

    async def join(self, queue_name: str) -> None:
        try:
            queue = self.queue_objects[queue_name]
        except KeyError:
            raise QueueNotFound(queue_name) from None

        while True:
            delayed_tasks = list(self.delayed_tasks_by_queue[queue_name])
            if delayed_tasks:
                await asyncio.gather(*delayed_tasks, return_exceptions=True)

            await queue.join()

            pending_delayed = [task for task in self.delayed_tasks_by_queue[queue_name] if not task.done()]
            if not pending_delayed and queue.empty():
                return

    async def get_dead_letter_records(self, queue_name: str) -> list[DeadLetterRecord]:
        return [
            record
            for record in self.dead_letters_by_queue[queue_name]
            if record.resolution_state == "active"
        ]

    async def get_dead_letter_record(self, queue_name: str, dead_letter_id: str) -> Optional[DeadLetterRecord]:
        for record in self.dead_letters_by_queue[queue_name]:
            if record.dead_letter_id == dead_letter_id:
                return record
        return None

    async def get_dead_letters(self, queue_name: str) -> list[Message]:
        messages: list[Message] = []
        for record in await self.get_dead_letter_records(queue_name):
            message = record.to_message()
            if message is not None:
                messages.append(message)
        return messages

    async def requeue_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        record = await self.get_dead_letter_record(queue_name, dead_letter_id)
        if record is None or record.resolution_state != "active":
            return record

        message = record.to_message()
        if message is None:
            raise ValueError("Dead letter record does not contain a message snapshot and cannot be requeued.")

        await self.send(message)
        record.resolution_state = "requeued"
        record.resolved_at_ms = current_millis()
        record.resolution_note = note
        return record

    async def purge_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        record = await self.get_dead_letter_record(queue_name, dead_letter_id)
        if record is None or record.resolution_state != "active":
            return record

        record.resolution_state = "purged"
        record.resolved_at_ms = current_millis()
        record.resolution_note = note
        return record


class _StubConsumer(Consumer):
    def __init__(self, broker: StubBroker, queue_name: str) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.queue = broker.queue_objects[queue_name]

    async def receive(self, *, limit: int, timeout: Optional[float]) -> list[Delivery]:
        entries: list[_StubEntry] = []

        try:
            if timeout is None:
                entries.append(await self.queue.get())
            else:
                entries.append(await asyncio.wait_for(self.queue.get(), timeout))
        except asyncio.TimeoutError:
            return []

        while len(entries) < limit:
            try:
                entries.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return [
            Delivery(
                message=entry.message,
                redelivered=entry.redelivered,
                transport_id=entry.message.message_id,
                metadata={"queue_name": self.queue_name},
            )
            for entry in entries
        ]

    async def ack(self, delivery: Delivery) -> None:
        self.queue.task_done()

    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None:
        if requeue:
            await self.queue.put(_StubEntry(delivery.message, redelivered=True))
        else:
            record = coerce_dead_letter_record(
                delivery.metadata.get("dead_letter_record"),
                namespace="stub",
                queue_name=self.queue_name,
                actor_name=delivery.actor_name,
                message=delivery.message,
                delivery_id=delivery.transport_id,
                consumer_name=None,
                failure_kind="operator_reject",
                execution_mode=delivery.metadata.get("execution_mode", "async"),
            )
            self.broker.dead_letters_by_queue[self.queue_name].append(record)

        self.queue.task_done()
