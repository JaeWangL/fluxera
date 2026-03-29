from __future__ import annotations

import asyncio
import os
import time
from typing import Optional
from uuid import uuid4

import orjson
import redis.asyncio as redis
from redis.exceptions import ResponseError, WatchError

from ..broker import Broker, Consumer, Delivery
from ..dead_letters import DeadLetterRecord, coerce_dead_letter_record
from ..encoder import JSONMessageEncoder, MessageEncoder
from ..errors import QueueNotFound
from ..message import Message
from .redis_scripts import RedisLuaScripts


def _normalize_error_message(exc: ResponseError) -> str:
    if not exc.args:
        return ""
    return str(exc.args[0])


class RedisBroker(Broker):
    """Redis Streams broker v2 with a message registry and Lua wrappers."""

    def __init__(
        self,
        url: str,
        *,
        namespace: str = "fluxera",
        group_name: str = "workers",
        lease_seconds: float = 30.0,
        worker_presence_ttl_seconds: float = 30.0,
        promote_batch_size: int = 128,
        pending_scan_size: int = 256,
        consumer_name_prefix: str = "fluxera",
        message_ttl_seconds: float = 86_400.0,
        dead_letter_ttl_seconds: float = 604_800.0,
        encoder: Optional[MessageEncoder] = None,
    ) -> None:
        super().__init__()
        self.url = url
        self.namespace = namespace.strip(":") or "fluxera"
        self.group_name = group_name
        self.lease_seconds = float(lease_seconds)
        self.worker_presence_ttl_seconds = max(float(worker_presence_ttl_seconds), 1.0)
        self.promote_batch_size = max(int(promote_batch_size), 1)
        self.pending_scan_size = max(int(pending_scan_size), 1)
        self.consumer_name_prefix = consumer_name_prefix
        self.message_ttl_seconds = max(float(message_ttl_seconds), self.lease_seconds, 1.0)
        self.dead_letter_ttl_seconds = max(float(dead_letter_ttl_seconds), 1.0)
        self.encoder = encoder or JSONMessageEncoder()
        self.client = redis.from_url(url, decode_responses=False)
        self.scripts = RedisLuaScripts(self.client)

    @property
    def message_ttl_ms(self) -> int:
        return max(int(self.message_ttl_seconds * 1000), 1)

    @property
    def dead_letter_ttl_ms(self) -> int:
        return max(int(self.dead_letter_ttl_seconds * 1000), 1)

    def _declare_queue(self, queue_name: str) -> None:
        del queue_name

    def _stream_key(self, queue_name: str) -> str:
        return f"{self.namespace}:stream:{queue_name}"

    def _serving_revision_key(self, queue_name: str) -> str:
        return f"{self.namespace}:serving_revision:{queue_name}"

    def _worker_key(self, worker_id: str) -> str:
        return f"{self.namespace}:worker:{worker_id}"

    def _workers_key(self, queue_name: str) -> str:
        return f"{self.namespace}:workers:{queue_name}"

    def _delayed_key(self, queue_name: str) -> str:
        return f"{self.namespace}:delayed:{queue_name}"

    def _dead_letter_key(self, queue_name: str) -> str:
        return f"{self.namespace}:dlq:{queue_name}"

    def _dead_letter_record_key(self, dead_letter_id: str) -> str:
        return f"{self.namespace}:dlq:record:{dead_letter_id}"

    def _message_key_prefix(self) -> str:
        return f"{self.namespace}:message:"

    def _message_key(self, message_id: str) -> str:
        return f"{self._message_key_prefix()}{message_id}"

    def _dedupe_key(self, queue_name: str, actor_name: str, dedupe_id: str) -> str:
        return f"{self.namespace}:dedupe:{queue_name}:{actor_name}:{dedupe_id}"

    def _idempotency_key(self, actor_name: str, idempotency_key: str) -> str:
        return f"{self.namespace}:idem:{actor_name}:{idempotency_key}"

    def _consumer_name(self, queue_name: str) -> str:
        del queue_name
        return f"{self.consumer_name_prefix}-{uuid4().hex}"

    @property
    def worker_presence_ttl_ms(self) -> int:
        return max(int(self.worker_presence_ttl_seconds * 1000), 1)

    def _encode_message(self, message: Message) -> bytes:
        return self.encoder.dumps(message)

    def _decode_message(self, payload: bytes) -> Message:
        return self.encoder.loads(payload)

    def _encode_dead_letter_record(self, record: DeadLetterRecord) -> bytes:
        return orjson.dumps(record.to_dict())

    def _decode_dead_letter_record(self, payload: bytes) -> DeadLetterRecord:
        return DeadLetterRecord.from_dict(orjson.loads(payload))

    def _dead_letter_record_ttl_ms(self, record: DeadLetterRecord, *, now_ms: Optional[int] = None) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        if record.retention_deadline_ms is None:
            record.retention_deadline_ms = now_ms + self.dead_letter_ttl_ms
            return self.dead_letter_ttl_ms

        return max(int(record.retention_deadline_ms) - now_ms, 1)

    def _queue_dead_letter_record_commands(self, pipe, record: DeadLetterRecord, *, now_ms: Optional[int] = None) -> None:
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        ttl_ms = self._dead_letter_record_ttl_ms(record, now_ms=now_ms)
        pipe.set(self._dead_letter_record_key(record.dead_letter_id), self._encode_dead_letter_record(record), px=ttl_ms)
        pipe.zadd(self._dead_letter_key(record.queue_name), {record.dead_letter_id: int(record.dead_lettered_at_ms)})
        pipe.pexpire(self._dead_letter_key(record.queue_name), ttl_ms)

    def _update_dead_letter_record_commands(self, pipe, record: DeadLetterRecord, *, now_ms: Optional[int] = None) -> None:
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        ttl_ms = self._dead_letter_record_ttl_ms(record, now_ms=now_ms)
        pipe.set(self._dead_letter_record_key(record.dead_letter_id), self._encode_dead_letter_record(record), px=ttl_ms)
        pipe.zrem(self._dead_letter_key(record.queue_name), record.dead_letter_id)
        pipe.pexpire(self._dead_letter_key(record.queue_name), ttl_ms)

    def _normalize_deduplication(self, message: Message) -> tuple[str, str, int, bool, bool]:
        options = message.options
        raw = options.get("deduplication")
        if raw is None:
            job_id = options.get("job_id")
            if job_id is None:
                return "none", "", 0, False, False
            raw = {"id": str(job_id), "mode": "simple"}

        if not isinstance(raw, dict):
            return "none", "", 0, False, False

        dedupe_id = raw.get("id")
        if dedupe_id in {None, ""}:
            return "none", "", 0, False, False

        mode = raw.get("mode")
        replace = bool(raw.get("replace", False))
        extend = bool(raw.get("extend", False))
        raw_ttl = raw.get("ttl_ms")
        ttl_ms = 0 if raw_ttl in {None, False} else max(int(raw_ttl), 0)

        if mode is None:
            if replace:
                mode = "debounce"
            elif ttl_ms > 0:
                mode = "throttle"
            else:
                mode = "simple"

        return str(mode), str(dedupe_id), ttl_ms, extend, replace

    async def release_deduplication_for_message(self, message: Message) -> bool:
        mode, dedupe_id, _ttl_ms, _extend, _replace = self._normalize_deduplication(message)
        if mode not in {"simple", "debounce"} or not dedupe_id:
            return False

        return await self.scripts.remove_dedupe_key_if_owner(
            dedupe_key=self._dedupe_key(message.queue_name, message.actor_name, dedupe_id),
            message_id=message.message_id,
        )

    async def idempotency_begin(
        self,
        *,
        actor_name: str,
        idempotency_key: str,
        owner: str,
        message_id: str,
        attempt: int,
        lease_ms: int,
    ):
        return await self.scripts.idem_begin(
            idempotency_key=self._idempotency_key(actor_name, idempotency_key),
            owner=owner,
            message_id=message_id,
            attempt=attempt,
            now_ms=int(time.time() * 1000),
            lease_ms=lease_ms,
        )

    async def idempotency_heartbeat(
        self,
        *,
        actor_name: str,
        idempotency_key: str,
        owner: str,
        fence_token: int,
        lease_ms: int,
    ):
        return await self.scripts.idem_heartbeat(
            idempotency_key=self._idempotency_key(actor_name, idempotency_key),
            owner=owner,
            fence_token=fence_token,
            now_ms=int(time.time() * 1000),
            lease_ms=lease_ms,
        )

    async def idempotency_commit(
        self,
        *,
        actor_name: str,
        idempotency_key: str,
        owner: str,
        fence_token: int,
        result_ttl_ms: int,
        result_ref: Optional[str] = None,
        result_digest: Optional[str] = None,
    ):
        return await self.scripts.idem_commit(
            idempotency_key=self._idempotency_key(actor_name, idempotency_key),
            owner=owner,
            fence_token=fence_token,
            now_ms=int(time.time() * 1000),
            ttl_ms=max(int(result_ttl_ms), 1),
            result_ref=result_ref,
            result_digest=result_digest,
        )

    async def idempotency_release(
        self,
        *,
        actor_name: str,
        idempotency_key: str,
        owner: str,
        fence_token: int,
    ):
        return await self.scripts.idem_release(
            idempotency_key=self._idempotency_key(actor_name, idempotency_key),
            owner=owner,
            fence_token=fence_token,
        )

    async def _ensure_group(self, queue_name: str) -> None:
        stream_key = self._stream_key(queue_name)
        try:
            await self.client.xgroup_create(stream_key, self.group_name, id="0-0", mkstream=True)
        except ResponseError as exc:
            message = _normalize_error_message(exc)
            if "BUSYGROUP" not in message:
                raise

    async def ensure_serving_revision(self, queue_name: str, worker_revision: str) -> str:
        key = self._serving_revision_key(queue_name)
        await self.client.setnx(key, worker_revision)
        current = await self.client.get(key)
        if current is None:
            await self.client.set(key, worker_revision)
            return worker_revision
        return _decode_message_id(current)

    async def get_serving_revision(self, queue_name: str) -> Optional[str]:
        current = await self.client.get(self._serving_revision_key(queue_name))
        if current is None:
            return None
        return _decode_message_id(current)

    async def promote_serving_revision(
        self,
        queue_name: str,
        revision: str,
        *,
        expected_revision: Optional[str] = None,
    ) -> bool:
        key = self._serving_revision_key(queue_name)
        if expected_revision is None:
            await self.client.set(key, revision)
            return True

        while True:
            pipe = self.client.pipeline()
            try:
                await pipe.watch(key)
                current = await pipe.get(key)
                current_revision = None if current is None else _decode_message_id(current)
                if current_revision != expected_revision:
                    await pipe.reset()
                    return False
                pipe.multi()
                pipe.set(key, revision)
                await pipe.execute()
                return True
            except WatchError:
                continue
            finally:
                await pipe.reset()

    async def register_worker_revision(
        self,
        *,
        worker_id: str,
        worker_revision: str,
        queue_states: dict[str, str],
    ) -> None:
        now_ms = int(time.time() * 1000)
        worker_key = self._worker_key(worker_id)
        accepting = ",".join(sorted(queue_name for queue_name, state in queue_states.items() if state == "accepting"))
        all_queues = ",".join(sorted(queue_states))
        mapping = {
            "worker_revision": worker_revision,
            "last_seen_ms": str(now_ms),
            "hostname": os.uname().nodename if hasattr(os, "uname") else "",
            "pid": str(os.getpid()),
            "queues": all_queues,
            "accepting_queues": accepting,
        }
        pipe = self.client.pipeline()
        pipe.hset(worker_key, mapping=mapping)
        pipe.pexpire(worker_key, self.worker_presence_ttl_ms)
        stale_before = now_ms - self.worker_presence_ttl_ms
        for queue_name in queue_states:
            workers_key = self._workers_key(queue_name)
            pipe.zadd(workers_key, {worker_id: now_ms})
            pipe.zremrangebyscore(workers_key, 0, stale_before)
        await pipe.execute()

    async def unregister_worker_revision(self, *, worker_id: str, queue_names: set[str]) -> None:
        pipe = self.client.pipeline()
        pipe.delete(self._worker_key(worker_id))
        for queue_name in queue_names:
            pipe.zrem(self._workers_key(queue_name), worker_id)
        await pipe.execute()

    async def _promote_due(self, queue_name: str, *, limit: Optional[int] = None) -> int:
        return await self.scripts.promote_due(
            delayed_key=self._delayed_key(queue_name),
            stream_key=self._stream_key(queue_name),
            now_ms=int(time.time() * 1000),
            limit=limit or self.promote_batch_size,
        )

    async def send(self, message: Message, *, delay: Optional[float] = None) -> Message:
        self.declare_queue(message.queue_name)
        now_ms = int(time.time() * 1000)
        deliver_at_ms = 0
        if delay is not None and delay > 0:
            deliver_at_ms = now_ms + int(delay * 1000)

        mode, dedupe_id, ttl_ms, extend, replace = self._normalize_deduplication(message)
        if mode == "debounce" and deliver_at_ms <= now_ms:
            raise ValueError("Debounce deduplication requires delayed delivery.")

        if mode == "none":
            dedupe_key = f"{self.namespace}:dedupe:unused"
        else:
            dedupe_key = self._dedupe_key(message.queue_name, message.actor_name, dedupe_id)

        decision = await self.scripts.enqueue_or_deduplicate(
            message_key_prefix=self._message_key_prefix(),
            stream_key=self._stream_key(message.queue_name),
            delayed_key=self._delayed_key(message.queue_name),
            dedupe_key=dedupe_key,
            message_id=message.message_id,
            encoded_message=self._encode_message(message),
            now_ms=now_ms,
            deliver_at_ms=deliver_at_ms,
            mode=mode,
            ttl_ms=ttl_ms,
            extend=extend,
            replace=replace,
            message_ttl_ms=self.message_ttl_ms,
        )
        if decision.status == "error":
            raise ValueError(f"Redis deduplication rejected message {message.message_id}: {decision.message_id}")
        return message

    async def open_consumer(self, queue_name: str, *, prefetch: int = 1) -> Consumer:
        self.declare_queue(queue_name)
        await self._ensure_group(queue_name)
        return _RedisConsumer(
            self,
            queue_name,
            prefetch=prefetch,
            consumer_name=self._consumer_name(queue_name),
        )

    async def get_dead_letter_records(self, queue_name: str) -> list[DeadLetterRecord]:
        dead_letter_ids = await self.client.zrange(self._dead_letter_key(queue_name), 0, -1)
        if not dead_letter_ids:
            return []

        payloads = await self.client.mget(
            *[self._dead_letter_record_key(_decode_message_id(dead_letter_id)) for dead_letter_id in dead_letter_ids]
        )
        records: list[DeadLetterRecord] = []
        for payload in payloads:
            if payload is None:
                continue
            records.append(self._decode_dead_letter_record(payload))
        return records

    async def get_dead_letter_record(self, queue_name: str, dead_letter_id: str) -> Optional[DeadLetterRecord]:
        payload = await self.client.get(self._dead_letter_record_key(dead_letter_id))
        if payload is None:
            return None

        record = self._decode_dead_letter_record(payload)
        if record.queue_name != queue_name:
            return None
        return record

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
        now_ms = int(time.time() * 1000)
        record.resolution_state = "requeued"
        record.resolved_at_ms = now_ms
        record.resolution_note = note

        pipe = self.client.pipeline()
        self._update_dead_letter_record_commands(pipe, record, now_ms=now_ms)
        await pipe.execute()
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

        now_ms = int(time.time() * 1000)
        record.resolution_state = "purged"
        record.resolved_at_ms = now_ms
        record.resolution_note = note

        pipe = self.client.pipeline()
        self._update_dead_letter_record_commands(pipe, record, now_ms=now_ms)
        await pipe.execute()
        return record

    async def flush(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        dead_letter_ids = await self.client.zrange(self._dead_letter_key(queue_name), 0, -1)
        dead_letter_record_keys = [
            self._dead_letter_record_key(_decode_message_id(dead_letter_id))
            for dead_letter_id in dead_letter_ids
        ]
        await self.client.delete(
            self._stream_key(queue_name),
            self._delayed_key(queue_name),
            self._dead_letter_key(queue_name),
            *dead_letter_record_keys,
        )

    async def join(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        stream_key = self._stream_key(queue_name)
        delayed_key = self._delayed_key(queue_name)
        sleep_interval = min(max(self.lease_seconds * 0.1, 0.01), 0.1)

        while True:
            await self._promote_due(queue_name)
            delayed_count = await self.client.zcard(delayed_key)
            stream_exists = await self.client.exists(stream_key)

            if not stream_exists:
                if delayed_count == 0:
                    return
                await asyncio.sleep(sleep_interval)
                continue

            stream_length = await self.client.xlen(stream_key)
            try:
                pending = await self.client.xpending(stream_key, self.group_name)
            except ResponseError as exc:
                message = _normalize_error_message(exc)
                if "NOGROUP" in message:
                    pending_count = 0
                else:
                    raise
            else:
                pending_count = int(pending["pending"])

            if delayed_count == 0 and stream_length == 0 and pending_count == 0:
                return

            await asyncio.sleep(sleep_interval)

    async def close(self) -> None:
        await self.client.aclose()


def _decode_stream_id(stream_id) -> str:
    if isinstance(stream_id, bytes):
        return stream_id.decode("utf-8")
    return str(stream_id)


def _decode_message_id(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


class _RedisConsumer(Consumer):
    def __init__(
        self,
        broker: RedisBroker,
        queue_name: str,
        *,
        prefetch: int,
        consumer_name: str,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = max(int(prefetch), 1)
        self.consumer_name = consumer_name
        self.stream_key = broker._stream_key(queue_name)
        self.dead_letter_key = broker._dead_letter_key(queue_name)
        self.claim_cursor = "0-0"
        self.active_ids: set[str] = set()
        self._closed = False

    @property
    def lease_ms(self) -> int:
        return max(int(self.broker.lease_seconds * 1000), 1)

    def _message_id_from_fields(self, fields) -> str:
        if b"message_id" in fields:
            return _decode_message_id(fields[b"message_id"])
        return _decode_message_id(fields["message_id"])

    async def _build_deliveries(self, entries, *, redelivered: bool) -> list[Delivery]:
        if not entries:
            return []

        message_ids = [self._message_id_from_fields(fields) for _, fields in entries]
        payloads = await self.broker.client.mget(*[self.broker._message_key(message_id) for message_id in message_ids])

        deliveries: list[Delivery] = []
        dead_letter_records: list[DeadLetterRecord] = []
        for (entry_id, _fields), message_id, payload in zip(entries, message_ids, payloads):
            transport_id = _decode_stream_id(entry_id)
            if payload is None:
                now_ms = int(time.time() * 1000)
                dead_letter_records.append(
                    DeadLetterRecord(
                        namespace=self.broker.namespace,
                        queue_name=self.queue_name,
                        actor_name="<unknown>",
                        message_id=message_id,
                        delivery_id=transport_id,
                        failure_kind="integrity_missing_payload",
                        message_snapshot=None,
                        payload_available=False,
                        execution_mode="unknown",
                        exception_message=f"Missing payload for message_id={message_id}.",
                        dead_lettered_at_ms=now_ms,
                        worker_id=None,
                        worker_revision=None,
                        consumer_name=self.consumer_name,
                    )
                )
                continue

            try:
                message = self.broker._decode_message(payload)
            except BaseException as exc:
                now_ms = int(time.time() * 1000)
                dead_letter_records.append(
                    DeadLetterRecord(
                        namespace=self.broker.namespace,
                        queue_name=self.queue_name,
                        actor_name="<unknown>",
                        message_id=message_id,
                        delivery_id=transport_id,
                        failure_kind="integrity_decode_error",
                        message_snapshot=None,
                        payload_available=True,
                        execution_mode="unknown",
                        exception_type=type(exc).__name__,
                        exception_message=str(exc),
                        dead_lettered_at_ms=now_ms,
                        worker_id=None,
                        worker_revision=None,
                        consumer_name=self.consumer_name,
                    )
                )
                continue

            delivery = Delivery(
                message=message,
                redelivered=redelivered,
                transport_id=transport_id,
                lease_deadline=time.time() + self.broker.lease_seconds,
                metadata={
                    "queue_name": self.queue_name,
                    "consumer_name": self.consumer_name,
                    "lease_seconds": self.broker.lease_seconds,
                    "message_id": message_id,
                    "message_key": self.broker._message_key(message_id),
                },
            )
            deliveries.append(delivery)
            self.active_ids.add(transport_id)

        if dead_letter_records:
            now_ms = int(time.time() * 1000)
            pipe = self.broker.client.pipeline()
            for record in dead_letter_records:
                self.broker._queue_dead_letter_record_commands(pipe, record, now_ms=now_ms)
                transport_id = record.delivery_id
                if transport_id is None:
                    continue
                pipe.xack(self.stream_key, self.broker.group_name, transport_id)
                pipe.xdel(self.stream_key, transport_id)
            await pipe.execute()

        return deliveries

    async def _claim_stale(self, *, limit: int) -> list[Delivery]:
        try:
            cursor, entries, _ = await self.broker.client.xautoclaim(
                self.stream_key,
                self.broker.group_name,
                self.consumer_name,
                self.lease_ms,
                self.claim_cursor,
                count=max(limit, self.broker.pending_scan_size),
            )
        except ResponseError as exc:
            message = _normalize_error_message(exc)
            if "NOGROUP" in message or "no such key" in message:
                return []
            raise

        self.claim_cursor = _decode_stream_id(cursor)
        filtered_entries = []
        for entry_id, fields in entries:
            transport_id = _decode_stream_id(entry_id)
            if transport_id in self.active_ids:
                continue
            filtered_entries.append((entry_id, fields))
            if len(filtered_entries) >= limit:
                break

        return await self._build_deliveries(filtered_entries, redelivered=True)

    async def receive(self, *, limit: int, timeout: Optional[float]) -> list[Delivery]:
        await self.broker._ensure_group(self.queue_name)
        await self.broker._promote_due(self.queue_name, limit=max(limit, self.prefetch))

        deliveries = await self._claim_stale(limit=limit)
        if deliveries:
            return deliveries

        block_ms = None if timeout is None else max(int(timeout * 1000), 1)
        response = await self.broker.client.xreadgroup(
            self.broker.group_name,
            self.consumer_name,
            {self.stream_key: ">"},
            count=limit,
            block=block_ms,
        )
        if not response:
            return []

        _, entries = response[0]
        return await self._build_deliveries(entries, redelivered=False)

    async def ack(self, delivery: Delivery) -> None:
        if delivery.transport_id is None:
            await self.broker.release_deduplication_for_message(delivery.message)
            return

        pipe = self.broker.client.pipeline()
        pipe.xack(self.stream_key, self.broker.group_name, delivery.transport_id)
        pipe.xdel(self.stream_key, delivery.transport_id)
        await pipe.execute()
        self.active_ids.discard(delivery.transport_id)
        await self.broker.release_deduplication_for_message(delivery.message)

    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None:
        if requeue:
            await self.broker.send(delivery.message)

        if delivery.transport_id is None:
            if not requeue:
                await self.broker.release_deduplication_for_message(delivery.message)
            return

        pipe = self.broker.client.pipeline()
        pipe.xack(self.stream_key, self.broker.group_name, delivery.transport_id)
        pipe.xdel(self.stream_key, delivery.transport_id)

        if not requeue:
            record = coerce_dead_letter_record(
                delivery.metadata.get("dead_letter_record"),
                namespace=self.broker.namespace,
                queue_name=self.queue_name,
                actor_name=delivery.actor_name,
                message=delivery.message,
                delivery_id=delivery.transport_id,
                consumer_name=self.consumer_name,
                failure_kind="operator_reject",
                execution_mode=delivery.metadata.get("execution_mode", "async"),
                retention_deadline_ms=int(time.time() * 1000) + self.broker.dead_letter_ttl_ms,
            )
            self.broker._queue_dead_letter_record_commands(pipe, record)

        await pipe.execute()
        self.active_ids.discard(delivery.transport_id)
        if not requeue:
            await self.broker.release_deduplication_for_message(delivery.message)

    async def extend_lease(self, delivery: Delivery, *, seconds: float) -> None:
        if delivery.transport_id is None:
            return

        await self.broker.client.xclaim(
            self.stream_key,
            self.broker.group_name,
            self.consumer_name,
            0,
            [delivery.transport_id],
            idle=0,
        )
        delivery.lease_deadline = time.time() + seconds
        delivery.metadata["lease_seconds"] = seconds

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self.active_ids.clear()
        try:
            await self.broker.client.xgroup_delconsumer(
                self.stream_key,
                self.broker.group_name,
                self.consumer_name,
            )
        except ResponseError as exc:
            message = _normalize_error_message(exc)
            if "NOGROUP" not in message and "no such key" not in message:
                raise
