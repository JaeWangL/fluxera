from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Optional
from uuid import uuid4

import orjson
import redis as sync_redis
import redis.asyncio as redis
from redis.exceptions import ResponseError, WatchError

from ..broker import Broker, Consumer, Delivery
from ..dead_letters import DeadLetterRecord, coerce_dead_letter_record
from ..encoder import JSONMessageEncoder, MessageEncoder
from ..errors import QueueNotFound
from ..message import Message
from .redis_scripts import RedisLuaScripts

DEFAULT_REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = 2.0
DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS = 5.0
DEFAULT_REDIS_SOCKET_KEEPALIVE = True
DEFAULT_REDIS_HEALTH_CHECK_INTERVAL_SECONDS = 30
DEFAULT_REDIS_MAX_CONNECTIONS = 96
DEFAULT_REDIS_CONNECTION_POOL_TIMEOUT_SECONDS = 2.0
DEFAULT_REDIS_PROMOTE_DUE_INTERVAL_SECONDS = 0.25
DEFAULT_REDIS_STALE_CLAIM_INTERVAL_MAX_SECONDS = 5.0
DEFAULT_REDIS_STALE_CLAIM_INTERVAL_MIN_SECONDS = 0.1
DEFAULT_REDIS_JOIN_POLL_INTERVAL_SECONDS = 0.1
DEFAULT_REDIS_JOIN_POLL_INTERVAL_MAX_SECONDS = 0.5
DEFAULT_REDIS_JOIN_POLL_INTERVAL_MULTIPLIER = 2.0
DEFAULT_REDIS_LEASE_EXTENSION_BATCH_SIZE = 256
DEFAULT_REDIS_LEASE_EXTENSION_BATCH_WINDOW_SECONDS = 0.001
DEFAULT_REDIS_QUEUE_DISCOVERY_MODE = "auto"
DEFAULT_REDIS_QUEUE_REGISTRY_MIGRATION_GRACE_SECONDS = 300.0
DEFAULT_REDIS_RUNTIME_QUEUE_BATCH_SIZE = 100


def _normalize_error_message(exc: ResponseError) -> str:
    if not exc.args:
        return ""
    return str(exc.args[0])


def _is_busy_group_error(exc: ResponseError) -> bool:
    return "BUSYGROUP" in _normalize_error_message(exc).upper()


def _is_missing_group_error(exc: ResponseError) -> bool:
    message = _normalize_error_message(exc).upper()
    return "NOGROUP" in message or "NO SUCH KEY" in message


def _default_client_name(namespace: str) -> str:
    hostname = os.uname().nodename if hasattr(os, "uname") else "unknown-host"
    return f"fluxera:{namespace}:{hostname}:{os.getpid()}"


def _default_stale_claim_interval(lease_seconds: float) -> float:
    return min(
        max(float(lease_seconds) * 0.5, DEFAULT_REDIS_STALE_CLAIM_INTERVAL_MIN_SECONDS),
        DEFAULT_REDIS_STALE_CLAIM_INTERVAL_MAX_SECONDS,
    )


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
        socket_connect_timeout: Optional[float] = DEFAULT_REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
        socket_timeout: Optional[float] = DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_keepalive: bool = DEFAULT_REDIS_SOCKET_KEEPALIVE,
        health_check_interval: int = DEFAULT_REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
        max_connections: int = DEFAULT_REDIS_MAX_CONNECTIONS,
        connection_pool_timeout: float = DEFAULT_REDIS_CONNECTION_POOL_TIMEOUT_SECONDS,
        client_name: Optional[str] = None,
        promote_due_interval_seconds: float = DEFAULT_REDIS_PROMOTE_DUE_INTERVAL_SECONDS,
        stale_claim_interval_seconds: Optional[float] = None,
        join_poll_interval_seconds: float = DEFAULT_REDIS_JOIN_POLL_INTERVAL_SECONDS,
        join_poll_interval_max_seconds: float = DEFAULT_REDIS_JOIN_POLL_INTERVAL_MAX_SECONDS,
        join_poll_interval_multiplier: float = DEFAULT_REDIS_JOIN_POLL_INTERVAL_MULTIPLIER,
        lease_extension_batch_size: int = DEFAULT_REDIS_LEASE_EXTENSION_BATCH_SIZE,
        lease_extension_batch_window_seconds: float = DEFAULT_REDIS_LEASE_EXTENSION_BATCH_WINDOW_SECONDS,
        runtime_queue_discovery_mode: str = DEFAULT_REDIS_QUEUE_DISCOVERY_MODE,
        queue_registry_migration_grace_seconds: float = DEFAULT_REDIS_QUEUE_REGISTRY_MIGRATION_GRACE_SECONDS,
        runtime_queue_batch_size: int = DEFAULT_REDIS_RUNTIME_QUEUE_BATCH_SIZE,
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
        self.promote_due_interval_seconds = max(float(promote_due_interval_seconds), 0.0)
        self.stale_claim_interval_seconds = (
            _default_stale_claim_interval(self.lease_seconds)
            if stale_claim_interval_seconds is None
            else max(float(stale_claim_interval_seconds), 0.0)
        )
        self.join_poll_interval_seconds = max(float(join_poll_interval_seconds), 0.001)
        self.join_poll_interval_max_seconds = max(
            float(join_poll_interval_max_seconds),
            self.join_poll_interval_seconds,
        )
        self.join_poll_interval_multiplier = max(float(join_poll_interval_multiplier), 1.0)
        self.lease_extension_batch_size = max(int(lease_extension_batch_size), 1)
        self.lease_extension_batch_window_seconds = max(
            float(lease_extension_batch_window_seconds),
            0.0,
        )
        self.runtime_queue_discovery_mode = str(runtime_queue_discovery_mode).lower()
        if self.runtime_queue_discovery_mode not in {
            "auto",
            "dual",
            "registry",
            "scan",
        }:
            raise ValueError(
                "runtime_queue_discovery_mode must be one of "
                "'auto', 'dual', 'registry', or 'scan'."
            )
        self.queue_registry_migration_grace_seconds = max(
            float(queue_registry_migration_grace_seconds),
            0.0,
        )
        self.runtime_queue_batch_size = max(int(runtime_queue_batch_size), 1)
        self.encoder = encoder or JSONMessageEncoder()
        self.socket_connect_timeout = (
            None
            if socket_connect_timeout is None
            else max(float(socket_connect_timeout), 0.001)
        )
        self.socket_timeout = (
            None
            if socket_timeout is None
            else max(float(socket_timeout), 0.001)
        )
        self.socket_keepalive = bool(socket_keepalive)
        self.health_check_interval = max(int(health_check_interval), 0)
        self.max_connections = max(int(max_connections), 1)
        self.connection_pool_timeout = max(float(connection_pool_timeout), 0.0)
        self.client_name = client_name or _default_client_name(self.namespace)
        common_connection_kwargs = {
            "decode_responses": False,
            "socket_connect_timeout": self.socket_connect_timeout,
            "socket_timeout": self.socket_timeout,
            "socket_keepalive": self.socket_keepalive,
            "health_check_interval": self.health_check_interval,
        }
        async_pool = redis.BlockingConnectionPool.from_url(
            url,
            max_connections=self.max_connections,
            timeout=self.connection_pool_timeout,
            client_name=f"{self.client_name}:async",
            **common_connection_kwargs,
        )
        sync_pool = sync_redis.BlockingConnectionPool.from_url(
            url,
            max_connections=self.max_connections,
            timeout=self.connection_pool_timeout,
            client_name=f"{self.client_name}:sync",
            **common_connection_kwargs,
        )
        self.client = redis.Redis(connection_pool=async_pool)
        self.sync_client = sync_redis.Redis(connection_pool=sync_pool)
        self.scripts = RedisLuaScripts(self.client)
        self.sync_scripts = RedisLuaScripts(self.sync_client)
        self._ensured_groups: set[str] = set()
        self._registered_runtime_queues: set[str] = set()

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

    def _queue_registry_key(self) -> str:
        return f"{self.namespace}:registry:queues"

    def _queue_registry_migrated_at_key(self) -> str:
        return f"{self.namespace}:registry:queues:migrated_at_ms"

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

    def _message_ref_key_prefix(self) -> str:
        return f"{self.namespace}:message_ref:"

    def _message_ref_key(self, message_id: str) -> str:
        return f"{self._message_ref_key_prefix()}{message_id}"

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

    def _forget_group(self, queue_name: str) -> None:
        self._ensured_groups.discard(queue_name)

    async def _ensure_group(self, queue_name: str, *, force: bool = False) -> None:
        if not force and queue_name in self._ensured_groups:
            return

        await self._create_group(queue_name)
        await self._register_runtime_queue(queue_name, force=True)
        self._ensured_groups.add(queue_name)

    async def _register_runtime_queue(
        self,
        queue_name: str,
        *,
        force: bool = False,
    ) -> None:
        if not force and queue_name in self._registered_runtime_queues:
            return
        await self.client.sadd(self._queue_registry_key(), queue_name)
        self._registered_runtime_queues.add(queue_name)

    async def _create_group(self, queue_name: str) -> None:
        stream_key = self._stream_key(queue_name)
        try:
            await self.client.xgroup_create(stream_key, self.group_name, id="0-0", mkstream=True)
        except ResponseError as exc:
            if not _is_busy_group_error(exc):
                raise

    async def ensure_serving_revision(self, queue_name: str, worker_revision: str) -> str:
        key = self._serving_revision_key(queue_name)
        pipe = self.client.pipeline()
        pipe.sadd(self._queue_registry_key(), queue_name)
        pipe.setnx(key, worker_revision)
        pipe.get(key)
        _registered, _created, current = await pipe.execute()
        self._registered_runtime_queues.add(queue_name)
        if current is None:
            await self.client.set(key, worker_revision)
            return worker_revision
        return _decode_message_id(current)

    async def get_serving_revision(self, queue_name: str) -> Optional[str]:
        current = await self.client.get(self._serving_revision_key(queue_name))
        if current is None:
            return None
        return _decode_message_id(current)

    async def get_serving_revisions(
        self,
        queue_names: set[str],
    ) -> dict[str, Optional[str]]:
        queue_names_sorted = sorted(queue_names)
        if not queue_names_sorted:
            return {}

        values = await self.client.mget(
            *[
                self._serving_revision_key(queue_name)
                for queue_name in queue_names_sorted
            ]
        )
        return {
            queue_name: None if value is None else _decode_message_id(value)
            for queue_name, value in zip(queue_names_sorted, values)
        }

    async def promote_serving_revision(
        self,
        queue_name: str,
        revision: str,
        *,
        expected_revision: Optional[str] = None,
    ) -> bool:
        key = self._serving_revision_key(queue_name)
        if expected_revision is None:
            pipe = self.client.pipeline()
            pipe.sadd(self._queue_registry_key(), queue_name)
            pipe.set(key, revision)
            await pipe.execute()
            self._registered_runtime_queues.add(queue_name)
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
                pipe.sadd(self._queue_registry_key(), queue_name)
                pipe.set(key, revision)
                await pipe.execute()
                self._registered_runtime_queues.add(queue_name)
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
        runtime_state: Optional[dict[str, Any]] = None,
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
        if runtime_state:
            for key, value in runtime_state.items():
                if value is None:
                    continue
                mapping[str(key)] = str(value)
        pipe = self.client.pipeline()
        if queue_states:
            pipe.sadd(self._queue_registry_key(), *sorted(queue_states))
        pipe.hset(worker_key, mapping=mapping)
        pipe.pexpire(worker_key, self.worker_presence_ttl_ms)
        stale_before = now_ms - self.worker_presence_ttl_ms
        for queue_name in queue_states:
            workers_key = self._workers_key(queue_name)
            pipe.zadd(workers_key, {worker_id: now_ms})
            pipe.zremrangebyscore(workers_key, 0, stale_before)
        await pipe.execute()
        self._registered_runtime_queues.update(queue_states)

    async def unregister_worker_revision(self, *, worker_id: str, queue_names: set[str]) -> None:
        pipe = self.client.pipeline()
        pipe.delete(self._worker_key(worker_id))
        for queue_name in queue_names:
            pipe.zrem(self._workers_key(queue_name), worker_id)
        await pipe.execute()

    async def _scan_runtime_queues(self) -> set[str]:
        queue_names: set[str] = set()
        scans = (
            (f"{self.namespace}:serving_revision:*", f"{self.namespace}:serving_revision:"),
            (f"{self.namespace}:stream:*", f"{self.namespace}:stream:"),
            (f"{self.namespace}:delayed:*", f"{self.namespace}:delayed:"),
            (f"{self.namespace}:workers:*", f"{self.namespace}:workers:"),
            (f"{self.namespace}:dlq:*", f"{self.namespace}:dlq:"),
        )
        for pattern, prefix in scans:
            async for raw_key in self.client.scan_iter(match=pattern, count=128):
                key = _decode_message_id(raw_key)
                if key.startswith(prefix):
                    queue_name = key[len(prefix) :]
                    if prefix.endswith(":dlq:") and queue_name.startswith("record:"):
                        continue
                    if queue_name:
                        queue_names.add(queue_name)
        return queue_names

    async def _registry_runtime_queues(self) -> set[str]:
        return {
            _decode_message_id(queue_name)
            for queue_name in await self.client.smembers(self._queue_registry_key())
            if queue_name
        }

    async def _registry_discovery_state(
        self,
    ) -> tuple[set[str], Optional[int]]:
        pipe = self.client.pipeline(transaction=False)
        pipe.smembers(self._queue_registry_key())
        pipe.get(self._queue_registry_migrated_at_key())
        raw_queues, raw_migrated_at_ms = await pipe.execute()
        queue_names = {
            _decode_message_id(queue_name)
            for queue_name in raw_queues
            if queue_name
        }
        try:
            migrated_at_ms = (
                None
                if raw_migrated_at_ms is None
                else int(_decode_message_id(raw_migrated_at_ms))
            )
        except (TypeError, ValueError):
            migrated_at_ms = None
        return queue_names, migrated_at_ms

    async def _backfill_runtime_queue_registry(
        self,
        scanned_queues: set[str],
        registry_queues: set[str],
        *,
        mark_migrated: bool,
    ) -> None:
        backfilled = scanned_queues - registry_queues
        pipe = self.client.pipeline()
        if backfilled:
            pipe.sadd(self._queue_registry_key(), *sorted(backfilled))
        if mark_migrated:
            pipe.setnx(
                self._queue_registry_migrated_at_key(),
                int(time.time() * 1000),
            )
        if backfilled or mark_migrated:
            await pipe.execute()
            self._registered_runtime_queues.update(backfilled)

    async def list_runtime_queues(self) -> list[str]:
        mode = self.runtime_queue_discovery_mode
        if mode == "scan":
            return sorted(set(self.queues) | await self._scan_runtime_queues())

        try:
            registry_queues, migrated_at_ms = await self._registry_discovery_state()
        except Exception:
            return sorted(set(self.queues) | await self._scan_runtime_queues())

        now_ms = int(time.time() * 1000)
        grace_elapsed = (
            migrated_at_ms is not None
            and now_ms - migrated_at_ms
            >= int(self.queue_registry_migration_grace_seconds * 1000)
        )
        if mode == "registry" and registry_queues:
            return sorted(set(self.queues) | registry_queues)
        if mode == "auto" and registry_queues and grace_elapsed:
            return sorted(set(self.queues) | registry_queues)

        scanned_queues = await self._scan_runtime_queues()
        try:
            await self._backfill_runtime_queue_registry(
                scanned_queues,
                registry_queues,
                mark_migrated=mode in {"auto", "dual"},
            )
        except Exception:
            pass
        return sorted(set(self.queues) | scanned_queues | registry_queues)

    async def reconcile_runtime_queue_registry(
        self,
        *,
        remove_stale: bool = False,
    ) -> dict[str, list[str]]:
        scanned_queues = await self._scan_runtime_queues()
        registry_queues = await self._registry_runtime_queues()
        backfilled = sorted(scanned_queues - registry_queues)
        if backfilled:
            await self.client.sadd(self._queue_registry_key(), *backfilled)
            self._registered_runtime_queues.update(backfilled)

        removed: list[str] = []
        if remove_stale:
            stale_candidates = sorted(
                registry_queues - scanned_queues - set(self.queues)
            )
            for queue_name in stale_candidates:
                runtime_keys = [
                    self._serving_revision_key(queue_name),
                    self._stream_key(queue_name),
                    self._delayed_key(queue_name),
                    self._workers_key(queue_name),
                    self._dead_letter_key(queue_name),
                ]
                if await self.scripts.remove_stale_queue(
                    registry_key=self._queue_registry_key(),
                    queue_name=queue_name,
                    runtime_keys=runtime_keys,
                ):
                    removed.append(queue_name)
                    self._registered_runtime_queues.discard(queue_name)

        return {
            "backfilled": backfilled,
            "removed": removed,
            "scanned": sorted(scanned_queues),
            "registered": sorted(
                (registry_queues | set(backfilled)) - set(removed)
            ),
        }

    async def list_worker_runtime_rows(self, *, queue_names: Optional[set[str]] = None) -> list[dict[str, str]]:
        worker_ids: set[str] = set()
        if queue_names is not None:
            if not queue_names:
                return []
            queue_names_sorted = sorted(queue_names)
            for offset in range(0, len(queue_names_sorted), self.runtime_queue_batch_size):
                queue_chunk = queue_names_sorted[
                    offset : offset + self.runtime_queue_batch_size
                ]
                pipe = self.client.pipeline(transaction=False)
                for queue_name in queue_chunk:
                    pipe.zrange(self._workers_key(queue_name), 0, -1)
                memberships = await pipe.execute()
                for members in memberships:
                    for raw_member in members:
                        worker_ids.add(_decode_message_id(raw_member))
        else:
            prefix = f"{self.namespace}:worker:"
            async for raw_key in self.client.scan_iter(match=f"{prefix}*", count=128):
                key = _decode_message_id(raw_key)
                if key.startswith(prefix):
                    worker_ids.add(key[len(prefix) :])

        if not worker_ids:
            return []

        worker_ids_sorted = sorted(worker_id for worker_id in worker_ids if worker_id)
        rows: list[dict[str, str]] = []
        for offset in range(0, len(worker_ids_sorted), self.runtime_queue_batch_size):
            worker_id_chunk = worker_ids_sorted[
                offset : offset + self.runtime_queue_batch_size
            ]
            pipe = self.client.pipeline(transaction=False)
            for worker_id in worker_id_chunk:
                pipe.hgetall(self._worker_key(worker_id))
            payloads = await pipe.execute()

            for worker_id, payload in zip(worker_id_chunk, payloads):
                if not payload:
                    continue
                decoded = {
                    _decode_message_id(key): _decode_message_id(value)
                    for key, value in payload.items()
                }
                decoded["worker_id"] = worker_id
                rows.append(decoded)
        return rows

    async def get_queue_runtime_row(
        self,
        queue_name: str,
        *,
        pending_idle_threshold_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        stream_key = self._stream_key(queue_name)
        delayed_key = self._delayed_key(queue_name)
        workers_key = self._workers_key(queue_name)

        pipe = self.client.pipeline()
        pipe.exists(stream_key)
        pipe.zcard(delayed_key)
        pipe.zrange(workers_key, 0, -1)
        exists_stream, delayed_count, worker_members = await pipe.execute()

        stream_length = 0
        if int(exists_stream):
            stream_length = int(await self.client.xlen(stream_key))

        pending_count = 0
        try:
            pending = await self.client.xpending(stream_key, self.group_name)
        except ResponseError as exc:
            message = _normalize_error_message(exc)
            if "NOGROUP" not in message and "no such key" not in message:
                raise
        else:
            pending_count = int(pending["pending"])

        pending_stale_count = 0
        if pending_idle_threshold_ms is not None and pending_idle_threshold_ms > 0 and pending_count > 0:
            pending_stale_count = await self._count_pending_with_min_idle(
                stream_key,
                min_idle_ms=pending_idle_threshold_ms,
            )

        return {
            "queue_name": queue_name,
            "serving_revision": await self.get_serving_revision(queue_name),
            "stream_length": stream_length,
            "delayed_count": int(delayed_count),
            "pending_count": pending_count,
            "pending_stale_count": pending_stale_count,
            "worker_ids": [_decode_message_id(worker_id) for worker_id in worker_members],
        }

    async def get_queue_runtime_rows(
        self,
        queue_names: list[str],
        *,
        pending_idle_threshold_ms: Optional[int] = None,
        include_worker_ids: bool = True,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        batch_size = self.runtime_queue_batch_size
        for offset in range(0, len(queue_names), batch_size):
            queue_chunk = queue_names[offset : offset + batch_size]
            pipe = self.client.pipeline(transaction=False)
            for queue_name in queue_chunk:
                stream_key = self._stream_key(queue_name)
                pipe.xlen(stream_key)
                pipe.zcard(self._delayed_key(queue_name))
                pipe.get(self._serving_revision_key(queue_name))
                pipe.xpending(stream_key, self.group_name)
                if include_worker_ids:
                    pipe.zrange(self._workers_key(queue_name), 0, -1)

            responses = await pipe.execute(raise_on_error=False)
            response_offset = 0
            for queue_name in queue_chunk:
                stream_length = responses[response_offset]
                delayed_count = responses[response_offset + 1]
                serving_revision = responses[response_offset + 2]
                pending = responses[response_offset + 3]
                response_offset += 4
                worker_members = []
                if include_worker_ids:
                    worker_members = responses[response_offset]
                    response_offset += 1

                for response in (
                    stream_length,
                    delayed_count,
                    serving_revision,
                    worker_members,
                ):
                    if isinstance(response, BaseException):
                        raise response

                pending_count = 0
                if isinstance(pending, ResponseError):
                    message = _normalize_error_message(pending)
                    if "NOGROUP" not in message and "no such key" not in message:
                        raise pending
                elif isinstance(pending, BaseException):
                    raise pending
                else:
                    pending_count = int(pending["pending"])

                pending_stale_count = 0
                if (
                    pending_idle_threshold_ms is not None
                    and pending_idle_threshold_ms > 0
                    and pending_count > 0
                ):
                    pending_stale_count = await self._count_pending_with_min_idle(
                        self._stream_key(queue_name),
                        min_idle_ms=pending_idle_threshold_ms,
                    )

                rows.append(
                    {
                        "queue_name": queue_name,
                        "serving_revision": (
                            None
                            if serving_revision is None
                            else _decode_message_id(serving_revision)
                        ),
                        "stream_length": int(stream_length),
                        "delayed_count": int(delayed_count),
                        "pending_count": pending_count,
                        "pending_stale_count": pending_stale_count,
                        "worker_ids": [
                            _decode_message_id(worker_id)
                            for worker_id in worker_members
                        ],
                    }
                )
        return rows

    async def _count_pending_with_min_idle(self, stream_key: str, *, min_idle_ms: int) -> int:
        total = 0
        start = "-"
        previous_last_id = None
        while True:
            try:
                entries = await self.client.xpending_range(
                    stream_key,
                    self.group_name,
                    start,
                    "+",
                    self.pending_scan_size,
                    idle=min_idle_ms,
                )
            except ResponseError as exc:
                message = _normalize_error_message(exc)
                if "NOGROUP" in message or "no such key" in message:
                    return total
                raise
            if not entries:
                return total

            total += len(entries)
            last_id = _pending_entry_message_id(entries[-1])
            if not last_id or last_id == previous_last_id:
                return total
            previous_last_id = last_id

            if len(entries) < self.pending_scan_size:
                return total
            start = f"({last_id}"

    async def _promote_due(self, queue_name: str, *, limit: Optional[int] = None) -> int:
        return await self.scripts.promote_due(
            delayed_key=self._delayed_key(queue_name),
            stream_key=self._stream_key(queue_name),
            now_ms=int(time.time() * 1000),
            limit=limit or self.promote_batch_size,
        )

    async def send(self, message: Message, *, delay: Optional[float] = None) -> Message:
        return await self._send(
            message,
            delay=delay,
            bypass_deduplication=False,
        )

    async def send_for_retry(
        self,
        message: Message,
        *,
        delay: Optional[float] = None,
    ) -> Message:
        return await self._send(
            message,
            delay=delay,
            bypass_deduplication=True,
        )

    async def _send(
        self,
        message: Message,
        *,
        delay: Optional[float],
        bypass_deduplication: bool,
    ) -> Message:
        self.declare_queue(message.queue_name)
        now_ms = int(time.time() * 1000)
        deliver_at_ms = 0
        if delay is not None and delay > 0:
            deliver_at_ms = now_ms + int(delay * 1000)

        if bypass_deduplication:
            mode, dedupe_id, ttl_ms, extend, replace = (
                "none",
                "",
                0,
                False,
                False,
            )
        else:
            mode, dedupe_id, ttl_ms, extend, replace = self._normalize_deduplication(message)
        if mode == "debounce" and deliver_at_ms <= now_ms:
            raise ValueError("Debounce deduplication requires delayed delivery.")

        if mode == "none":
            dedupe_key = f"{self.namespace}:dedupe:unused"
        else:
            dedupe_key = self._dedupe_key(message.queue_name, message.actor_name, dedupe_id)

        decision = await self.scripts.enqueue_or_deduplicate(
            message_key_prefix=self._message_key_prefix(),
            message_ref_key_prefix=self._message_ref_key_prefix(),
            stream_key=self._stream_key(message.queue_name),
            delayed_key=self._delayed_key(message.queue_name),
            dedupe_key=dedupe_key,
            queue_registry_key=self._queue_registry_key(),
            queue_name=message.queue_name,
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
        self._registered_runtime_queues.add(message.queue_name)
        return message

    def send_sync(self, message: Message, *, delay: Optional[float] = None) -> Message:
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

        decision = self.sync_scripts.enqueue_or_deduplicate_sync(
            message_key_prefix=self._message_key_prefix(),
            message_ref_key_prefix=self._message_ref_key_prefix(),
            stream_key=self._stream_key(message.queue_name),
            delayed_key=self._delayed_key(message.queue_name),
            dedupe_key=dedupe_key,
            queue_registry_key=self._queue_registry_key(),
            queue_name=message.queue_name,
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
        self._registered_runtime_queues.add(message.queue_name)
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
        self._forget_group(queue_name)

    async def join(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        stream_key = self._stream_key(queue_name)
        delayed_key = self._delayed_key(queue_name)
        sleep_interval = self.join_poll_interval_seconds
        previous_state: Optional[tuple[int, int, int]] = None

        while True:
            await self._promote_due(queue_name)
            delayed_count = await self.client.zcard(delayed_key)
            stream_exists = await self.client.exists(stream_key)

            if not stream_exists:
                if delayed_count == 0:
                    return
                state = (int(delayed_count), 0, 0)
                if state == previous_state:
                    sleep_interval = min(
                        sleep_interval * self.join_poll_interval_multiplier,
                        self.join_poll_interval_max_seconds,
                    )
                else:
                    sleep_interval = self.join_poll_interval_seconds
                previous_state = state
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

            state = (int(delayed_count), stream_length, pending_count)
            if state == previous_state:
                sleep_interval = min(
                    sleep_interval * self.join_poll_interval_multiplier,
                    self.join_poll_interval_max_seconds,
                )
            else:
                sleep_interval = self.join_poll_interval_seconds
            previous_state = state
            await asyncio.sleep(sleep_interval)

    async def close(self) -> None:
        await self.client.aclose(close_connection_pool=True)
        self.sync_client.close()
        self.sync_client.connection_pool.disconnect()


def _decode_stream_id(stream_id) -> str:
    if isinstance(stream_id, bytes):
        return stream_id.decode("utf-8")
    return str(stream_id)


def _decode_message_id(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


def _pending_entry_message_id(entry: Any) -> str:
    if isinstance(entry, dict):
        if "message_id" in entry:
            return _decode_message_id(entry["message_id"])
        if b"message_id" in entry:
            return _decode_message_id(entry[b"message_id"])
    if isinstance(entry, (list, tuple)) and entry:
        return _decode_message_id(entry[0])
    return ""


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
        self._next_promote_due_at = 0.0
        self._next_stale_claim_at = 0.0
        self._pending_lease_extensions: dict[
            float,
            list[tuple[Delivery, asyncio.Future[None]]],
        ] = {}
        self._lease_extension_flush_task: Optional[asyncio.Task[None]] = None

    @property
    def lease_ms(self) -> int:
        return max(int(self.broker.lease_seconds * 1000), 1)

    def _message_id_from_fields(self, fields) -> str:
        if b"message_id" in fields:
            return _decode_message_id(fields[b"message_id"])
        return _decode_message_id(fields["message_id"])

    def _registry_message_id(self, delivery: Delivery) -> str:
        message_id = delivery.metadata.get("message_id")
        if message_id is None:
            return delivery.message_id
        return _decode_message_id(message_id)

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
                    "message_ref_key": self.broker._message_ref_key(message_id),
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
                await self.broker.scripts.acknowledge_delivery(
                    stream_key=self.stream_key,
                    payload_key=self.broker._message_key(record.message_id),
                    payload_ref_key=self.broker._message_ref_key(record.message_id),
                    group_name=self.broker.group_name,
                    transport_id=transport_id,
                    client=pipe,
                )
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
            if _is_missing_group_error(exc):
                self.broker._forget_group(self.queue_name)
                await self.broker._ensure_group(self.queue_name, force=True)
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

    async def _promote_due_if_due(self, *, limit: int) -> None:
        interval = self.broker.promote_due_interval_seconds
        now = time.monotonic()
        if interval > 0 and now < self._next_promote_due_at:
            return

        promoted = await self.broker._promote_due(self.queue_name, limit=max(limit, self.prefetch))
        self._next_promote_due_at = 0.0 if promoted > 0 else time.monotonic() + interval

    async def _claim_stale_if_due(self, *, limit: int) -> list[Delivery]:
        interval = self.broker.stale_claim_interval_seconds
        now = time.monotonic()
        if interval > 0 and now < self._next_stale_claim_at:
            return []

        deliveries = await self._claim_stale(limit=limit)
        self._next_stale_claim_at = 0.0 if deliveries else time.monotonic() + interval
        return deliveries

    async def receive(self, *, limit: int, timeout: Optional[float]) -> list[Delivery]:
        await self.broker._ensure_group(self.queue_name)
        await self._promote_due_if_due(limit=limit)

        deliveries = await self._claim_stale_if_due(limit=limit)
        if deliveries:
            return deliveries

        block_ms = None if timeout is None else max(int(timeout * 1000), 1)
        try:
            response = await self.broker.client.xreadgroup(
                self.broker.group_name,
                self.consumer_name,
                {self.stream_key: ">"},
                count=limit,
                block=block_ms,
            )
        except ResponseError as exc:
            if _is_missing_group_error(exc):
                self.broker._forget_group(self.queue_name)
                await self.broker._ensure_group(self.queue_name, force=True)
                return []
            raise
        if not response:
            return []

        _, entries = response[0]
        return await self._build_deliveries(entries, redelivered=False)

    async def ack(self, delivery: Delivery) -> None:
        await self._acknowledge_delivery(
            delivery,
            release_deduplication=True,
        )

    async def ack_for_retry(self, delivery: Delivery) -> None:
        await self._acknowledge_delivery(
            delivery,
            release_deduplication=False,
        )

    async def _acknowledge_delivery(
        self,
        delivery: Delivery,
        *,
        release_deduplication: bool,
    ) -> None:
        if delivery.transport_id is None:
            if release_deduplication:
                await self.broker.release_deduplication_for_message(delivery.message)
            return

        registry_message_id = self._registry_message_id(delivery)
        await self.broker.scripts.acknowledge_delivery(
            stream_key=self.stream_key,
            payload_key=self.broker._message_key(registry_message_id),
            payload_ref_key=self.broker._message_ref_key(registry_message_id),
            group_name=self.broker.group_name,
            transport_id=delivery.transport_id,
        )
        self.active_ids.discard(delivery.transport_id)
        if release_deduplication:
            await self.broker.release_deduplication_for_message(delivery.message)

    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None:
        if requeue:
            await self.broker.send_for_retry(delivery.message)
            await self._acknowledge_delivery(
                delivery,
                release_deduplication=False,
            )
            return

        if delivery.transport_id is None:
            await self.broker.release_deduplication_for_message(delivery.message)
            return

        pipe = self.broker.client.pipeline()
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
        registry_message_id = self._registry_message_id(delivery)
        await self.broker.scripts.acknowledge_delivery(
            stream_key=self.stream_key,
            payload_key=self.broker._message_key(registry_message_id),
            payload_ref_key=self.broker._message_ref_key(registry_message_id),
            group_name=self.broker.group_name,
            transport_id=delivery.transport_id,
            client=pipe,
        )

        await pipe.execute()
        self.active_ids.discard(delivery.transport_id)
        await self.broker.release_deduplication_for_message(delivery.message)

    async def extend_lease(self, delivery: Delivery, *, seconds: float) -> None:
        if delivery.transport_id is None:
            return

        loop = asyncio.get_running_loop()
        completed: asyncio.Future[None] = loop.create_future()
        self._pending_lease_extensions.setdefault(float(seconds), []).append(
            (delivery, completed)
        )
        if (
            self._lease_extension_flush_task is None
            or self._lease_extension_flush_task.done()
        ):
            self._lease_extension_flush_task = asyncio.create_task(
                self._flush_lease_extensions(),
                name=f"fluxera-lease-batch:{self.queue_name}:{self.consumer_name}",
            )
        await completed

    async def _flush_lease_extensions(self) -> None:
        try:
            await asyncio.sleep(self.broker.lease_extension_batch_window_seconds)
            while self._pending_lease_extensions:
                batches = self._pending_lease_extensions
                self._pending_lease_extensions = {}
                for seconds, requests in batches.items():
                    await self._extend_lease_batch(requests, seconds=seconds)
                await asyncio.sleep(0)
        finally:
            self._lease_extension_flush_task = None
            if self._pending_lease_extensions:
                self._lease_extension_flush_task = asyncio.create_task(
                    self._flush_lease_extensions(),
                    name=f"fluxera-lease-batch:{self.queue_name}:{self.consumer_name}",
                )

    async def _extend_lease_batch(
        self,
        requests: list[tuple[Delivery, asyncio.Future[None]]],
        *,
        seconds: float,
    ) -> None:
        batch_size = self.broker.lease_extension_batch_size
        for offset in range(0, len(requests), batch_size):
            chunk = requests[offset : offset + batch_size]
            transport_ids = [
                delivery.transport_id
                for delivery, _completed in chunk
                if delivery.transport_id is not None
            ]
            try:
                claimed = await self.broker.client.xclaim(
                    self.stream_key,
                    self.broker.group_name,
                    self.consumer_name,
                    0,
                    transport_ids,
                    idle=0,
                    justid=True,
                )
            except BaseException as exc:
                for _delivery, completed in chunk:
                    if not completed.done():
                        completed.set_exception(exc)
                if isinstance(exc, asyncio.CancelledError):
                    raise
                continue

            claimed_ids = {
                _decode_stream_id(transport_id)
                for transport_id in claimed
            }
            lease_deadline = time.time() + seconds
            for delivery, completed in chunk:
                transport_id = delivery.transport_id
                if transport_id in claimed_ids:
                    delivery.lease_deadline = lease_deadline
                    delivery.metadata["lease_seconds"] = seconds
                    if not completed.done():
                        completed.set_result(None)
                    continue

                if transport_id not in self.active_ids:
                    if not completed.done():
                        completed.set_result(None)
                    continue

                if not completed.done():
                    completed.set_exception(
                        RuntimeError(
                            f"Redis did not extend active delivery lease {transport_id!r}."
                        )
                    )

    async def close(self, *, forget: bool = False) -> None:
        if self._closed and not forget:
            return

        self._closed = True
        if self._lease_extension_flush_task is not None:
            await asyncio.gather(
                self._lease_extension_flush_task,
                return_exceptions=True,
            )
            self._lease_extension_flush_task = None
        if not forget:
            return

        if self.active_ids:
            return

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
