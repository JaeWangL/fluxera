from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional
from uuid import uuid4

import orjson

try:
    import aio_pika
    from aio_pika.exceptions import AMQPError, ChannelInvalidStateError, QueueEmpty
    from aiormq.exceptions import ChannelLockedResource
except ImportError as exc:  # pragma: no cover - extra 미설치 환경에서만 도달
    raise ImportError(
        "RabbitMQBroker requires the 'aio-pika' package. Install it with: pip install 'fluxera[rabbitmq]'"
    ) from exc

from ..broker import Broker, Consumer, Delivery
from ..dead_letters import DeadLetterRecord, coerce_dead_letter_record
from ..encoder import JSONMessageEncoder, MessageEncoder
from ..errors import QueueNotFound
from ..message import Message

DEFAULT_RABBITMQ_MANAGEMENT_PORT = 15672
DEFAULT_RABBITMQ_DEAD_LETTER_TTL_SECONDS = 604_800.0
DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_SECONDS = 0.1
DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_MAX_SECONDS = 0.5
DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_MULTIPLIER = 2.0
DEFAULT_RABBITMQ_JOIN_IDLE_GRACE_SECONDS = 1.0
DEFAULT_RABBITMQ_RECEIVE_BATCH_WINDOW_SECONDS = 0.01
DEFAULT_RABBITMQ_MANAGEMENT_TIMEOUT_SECONDS = 5.0
DEFAULT_RABBITMQ_CLOSE_TIMEOUT_SECONDS = 5.0
DEFAULT_RABBITMQ_DLQ_SCAN_LOCK_TIMEOUT_SECONDS = 10.0

logger = logging.getLogger(__name__)

_TRANSPORT_ERRORS = (AMQPError, ChannelInvalidStateError, ConnectionError, OSError)

_DEAD_LETTER_RECORD_FIELDS = {field.name for field in dataclasses.fields(DeadLetterRecord)}


def _default_client_name(namespace: str) -> str:
    hostname = os.uname().nodename if hasattr(os, "uname") else "unknown-host"
    return f"fluxera:{namespace}:{hostname}:{os.getpid()}"


def _derive_management_url(amqp_url: str) -> str:
    parsed = urllib.parse.urlsplit(amqp_url)
    scheme = "https" if parsed.scheme == "amqps" else "http"
    username = parsed.username or "guest"
    password = parsed.password or "guest"
    host = parsed.hostname or "127.0.0.1"
    return f"{scheme}://{username}:{password}@{host}:{DEFAULT_RABBITMQ_MANAGEMENT_PORT}/"


def _vhost_from_url(amqp_url: str) -> str:
    parsed = urllib.parse.urlsplit(amqp_url)
    path = parsed.path or "/"
    if path in {"", "/"}:
        return "/"
    return urllib.parse.unquote(path[1:])


class _ManagementAPI:
    """블로킹 RabbitMQ management API 클라이언트. asyncio.to_thread로 호출한다."""

    def __init__(self, management_url: str, vhost: str, *, timeout_seconds: float) -> None:
        parsed = urllib.parse.urlsplit(management_url)
        username = urllib.parse.unquote(parsed.username or "guest")
        password = urllib.parse.unquote(parsed.password or "guest")
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or DEFAULT_RABBITMQ_MANAGEMENT_PORT
        self.base_url = f"{parsed.scheme}://{host}:{port}"
        self.vhost = vhost
        self.timeout_seconds = timeout_seconds
        credentials = f"{username}:{password}".encode("utf-8")
        self._auth_header = "Basic " + base64.b64encode(credentials).decode("ascii")

    def _request(self, path: str) -> Any:
        request = urllib.request.Request(f"{self.base_url}{path}")
        request.add_header("Authorization", self._auth_header)
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            payload = response.read()
        if not payload:
            return None
        return json.loads(payload)

    def queue_stats(self, queue_name: str) -> Optional[dict[str, Any]]:
        vhost = urllib.parse.quote(self.vhost, safe="")
        encoded = urllib.parse.quote(queue_name, safe="")
        try:
            return self._request(f"/api/queues/{vhost}/{encoded}")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {}
            raise

    def list_queue_names(self) -> list[str]:
        vhost = urllib.parse.quote(self.vhost, safe="")
        queues = self._request(f"/api/queues/{vhost}?columns=name") or []
        return [queue["name"] for queue in queues if "name" in queue]


class _BrokerState:
    """단일 이벤트 루프에 바인딩된 커넥션/채널 묶음."""

    def __init__(self) -> None:
        self.publish_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.consume_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.publish_channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.lock = asyncio.Lock()
        self.closed = False

    async def close(self) -> None:
        # closed를 먼저 세워 진행 중인 send가 이 상태 위에 재연결하지 못하게 한다
        self.closed = True
        if self.publish_channel is not None and not self.publish_channel.is_closed:
            try:
                await asyncio.wait_for(
                    self.publish_channel.close(),
                    DEFAULT_RABBITMQ_CLOSE_TIMEOUT_SECONDS,
                )
            except (asyncio.TimeoutError, *_TRANSPORT_ERRORS):
                pass
        for connection in (self.publish_connection, self.consume_connection):
            if connection is not None and not connection.is_closed:
                try:
                    await asyncio.wait_for(
                        connection.close(),
                        DEFAULT_RABBITMQ_CLOSE_TIMEOUT_SECONDS,
                    )
                except (asyncio.TimeoutError, *_TRANSPORT_ERRORS):
                    pass
        self.publish_channel = None
        self.publish_connection = None
        self.consume_connection = None


class _LoopThreadRunner:
    """동기 send 파사드를 지탱하는 전용 이벤트 루프 스레드."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def _ensure_running(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None and self._thread is not None and self._thread.is_alive():
                return self._loop

            loop = asyncio.new_event_loop()

            def run_forever() -> None:
                asyncio.set_event_loop(loop)
                loop.run_forever()

            thread = threading.Thread(target=run_forever, name=self.name, daemon=True)
            thread.start()
            self._loop = loop
            self._thread = thread
            return loop

    def submit(self, coro):
        loop = self._ensure_running()
        return asyncio.run_coroutine_threadsafe(coro, loop)

    def run(self, coro, *, timeout: Optional[float] = None):
        return self.submit(coro).result(timeout)

    def stop(self, shutdown_coro=None) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
            self._loop = None
            self._thread = None

        if loop is None or thread is None or not thread.is_alive():
            if shutdown_coro is not None:
                shutdown_coro.close()
            return

        if shutdown_coro is not None:
            try:
                asyncio.run_coroutine_threadsafe(shutdown_coro, loop).result(10.0)
            except Exception:
                logger.warning("Failed to close RabbitMQ sync facade cleanly.", exc_info=True)

        # 잔여 태스크를 취소해 대기 중인 send_sync 호출자가 30초 타임아웃까지
        # 얼어붙지 않게 한 뒤 루프를 세운다
        async def drain_and_stop() -> None:
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            loop.stop()

        try:
            asyncio.run_coroutine_threadsafe(drain_and_stop(), loop)
        except RuntimeError:
            pass
        thread.join(5.0)
        if not thread.is_alive() and not loop.is_closed():
            loop.close()


class RabbitMQBroker(Broker):
    """aio-pika 기반 RabbitMQ 브로커.

    전송 계층 매핑:

    - Fluxera 큐 ``q``는 durable AMQP 큐 ``{namespace}:queue:{q}``에 대응한다
    - 지연 전송은 메시지별 TTL과 메인 큐로 되돌아오는 dead-letter 라우팅을 가진
      ``{namespace}:delayed:{q}``로 발행한다 (지연은 FIFO 순서로 처리되므로 긴 지연이
      짧은 지연보다 앞에 있으면 짧은 쪽이 뒤로 밀린다 — Dramatiq 지연 큐와 같은 트레이드오프)
    - dead-letter 레코드는 ``{namespace}:dlq:{q}`` 큐의 메시지로 저장한다

    RedisBroker 대비 미지원 기능(둘 다 공유 KV 저장소가 필요):
    메시지 중복 제거(실질적 dedupe를 요구하는 ``deduplication``/``job_id`` 옵션은
    ``ValueError``)와 serving-revision 컨트롤 플레인(기반 클래스 기본값 적용: 모든
    워커가 accepting). RabbitMQ는 컨슈머 커넥션이 끊기면 unacked 메시지를 재전달하므로
    리스/하트비트 메커니즘도 없다. ``Delivery.metadata``에 ``lease_seconds``가 실리지
    않는다.

    RedisBroker와 시맨틱이 다른 지점:

    - ``flush()``는 AMQP ``queue.purge``를 사용하므로 ack 대기 중(unacked)인 인플라이트
      메시지는 지우지 못한다. 해당 메시지는 소유 채널이 닫히거나 nack되면 다시 큐로
      돌아온다. Redis flush는 스트림 키 삭제로 PEL까지 제거한다.
    - dead-letter 조회는 basic_get 기반 스캔이다. 스캔 중에는 해당 레코드들이 다른
      클라이언트에게 보이지 않으므로, 네임스페이스별 배타 락 큐
      (``{namespace}:dlq-scan-lock:{q}``)로 브로커 인스턴스·프로세스 간 스캔을
      직렬화한다. 락과 무관한 외부 컨슈머가 DLQ 큐를 직접 읽는 것까지 막지는 못한다.
    - ``join()``의 크로스 프로세스 unacked 감지는 management API 통계에 의존한다.
      서버의 ``collect_statistics_interval``(기본 5초)이 ``join_idle_grace_seconds``
      (기본 1초)보다 길면 다른 프로세스의 인플라이트 메시지를 놓치고 조기 반환할 수
      있다. 같은 프로세스의 인플라이트는 로컬 카운터로 정확히 추적한다. 크로스
      프로세스 드레인이 필요하면 ``join_idle_grace_seconds``를 서버 통계 주기 이상으로
      올려라 (로컬 개발 compose는 통계 주기를 500ms로 설정한다).
    """

    def __init__(
        self,
        url: str,
        *,
        namespace: str = "fluxera",
        management_url: Optional[str] = None,
        dead_letter_ttl_seconds: float = DEFAULT_RABBITMQ_DEAD_LETTER_TTL_SECONDS,
        client_name: Optional[str] = None,
        consumer_name_prefix: str = "fluxera",
        consumer_timeout_seconds: Optional[float] = None,
        heartbeat_seconds: Optional[float] = None,
        reconnect_interval_seconds: float = 5.0,
        publisher_confirms: bool = True,
        join_poll_interval_seconds: float = DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_SECONDS,
        join_poll_interval_max_seconds: float = DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_MAX_SECONDS,
        join_poll_interval_multiplier: float = DEFAULT_RABBITMQ_JOIN_POLL_INTERVAL_MULTIPLIER,
        join_idle_grace_seconds: float = DEFAULT_RABBITMQ_JOIN_IDLE_GRACE_SECONDS,
        receive_batch_window_seconds: float = DEFAULT_RABBITMQ_RECEIVE_BATCH_WINDOW_SECONDS,
        management_timeout_seconds: float = DEFAULT_RABBITMQ_MANAGEMENT_TIMEOUT_SECONDS,
        dead_letter_scan_lock_timeout_seconds: float = DEFAULT_RABBITMQ_DLQ_SCAN_LOCK_TIMEOUT_SECONDS,
        encoder: Optional[MessageEncoder] = None,
    ) -> None:
        super().__init__()
        self.url = url
        self.namespace = namespace.strip(":") or "fluxera"
        self.dead_letter_ttl_seconds = max(float(dead_letter_ttl_seconds), 1.0)
        self.client_name = client_name or _default_client_name(self.namespace)
        self.consumer_name_prefix = consumer_name_prefix
        # RabbitMQ 기본 consumer timeout은 30분이다. 이보다 오래 걸리는 태스크
        # (예: 5분 타임아웃 API를 순차 6개 호출)는 ack 전에 서버가 채널을 끊고
        # 메시지를 재전달하므로, 최장 태스크 시간보다 넉넉히 크게 설정해야 한다.
        self.consumer_timeout_seconds = (
            None if consumer_timeout_seconds is None else max(float(consumer_timeout_seconds), 1.0)
        )
        self.heartbeat_seconds = (
            None if heartbeat_seconds is None else max(float(heartbeat_seconds), 1.0)
        )
        self.reconnect_interval_seconds = max(float(reconnect_interval_seconds), 0.1)
        # True(기본)면 send가 브로커 confirm(fsync 보장) 후 반환한다. 동시 send는
        # 한 채널에서 confirm 대기가 겹쳐 자동 배칭된다. False면 fire-and-forget:
        # 수 배 빠르지만 브로커 crash 시 미확정 발행이 유실될 수 있다.
        self.publisher_confirms = bool(publisher_confirms)
        self.join_poll_interval_seconds = max(float(join_poll_interval_seconds), 0.001)
        self.join_poll_interval_max_seconds = max(
            float(join_poll_interval_max_seconds),
            self.join_poll_interval_seconds,
        )
        self.join_poll_interval_multiplier = max(float(join_poll_interval_multiplier), 1.0)
        self.join_idle_grace_seconds = max(float(join_idle_grace_seconds), 0.0)
        self.receive_batch_window_seconds = max(float(receive_batch_window_seconds), 0.0)
        self.dead_letter_scan_lock_timeout_seconds = max(
            float(dead_letter_scan_lock_timeout_seconds),
            0.1,
        )
        self.encoder = encoder or JSONMessageEncoder()
        self.management = _ManagementAPI(
            management_url or _derive_management_url(url),
            _vhost_from_url(url),
            timeout_seconds=max(float(management_timeout_seconds), 0.1),
        )

        self._async_state: Optional[_BrokerState] = None
        self._async_state_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_state_lock = asyncio.Lock()
        self._sync_state: Optional[_BrokerState] = None
        self._sync_facade_lock = threading.Lock()
        self._sync_runner = _LoopThreadRunner(f"fluxera-rabbitmq-sync:{self.namespace}")
        self._declared_topologies: set[str] = set()
        self._consumers: set["_RabbitMQConsumer"] = set()
        self._dead_letter_lock = asyncio.Lock()
        self._management_unavailable_logged = False

    @property
    def dead_letter_ttl_ms(self) -> int:
        return max(int(self.dead_letter_ttl_seconds * 1000), 1)

    def _declare_queue(self, queue_name: str) -> None:
        del queue_name

    def _queue_name(self, queue_name: str) -> str:
        return f"{self.namespace}:queue:{queue_name}"

    def _delayed_queue_name(self, queue_name: str) -> str:
        return f"{self.namespace}:delayed:{queue_name}"

    def _dead_letter_queue_name(self, queue_name: str) -> str:
        return f"{self.namespace}:dlq:{queue_name}"

    def _consumer_name(self, queue_name: str) -> str:
        del queue_name
        return f"{self.consumer_name_prefix}-{uuid4().hex}"

    def _encode_message(self, message: Message) -> bytes:
        return self.encoder.dumps(message)

    def _decode_message(self, payload: bytes) -> Message:
        return self.encoder.loads(payload)

    def _encode_dead_letter_record(self, record: DeadLetterRecord) -> bytes:
        return orjson.dumps(record.to_dict())

    def _decode_dead_letter_record(self, payload: bytes) -> DeadLetterRecord:
        data = orjson.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("Dead letter payload must decode to a dict.")
        # 미래 버전이 필드를 추가해도 읽을 수 있도록 알려진 필드만 취한다
        known = {key: value for key, value in data.items() if key in _DEAD_LETTER_RECORD_FIELDS}
        return DeadLetterRecord.from_dict(known)

    # -- 커넥션 상태 -------------------------------------------------------

    async def _get_async_state(self) -> _BrokerState:
        loop = asyncio.get_running_loop()
        if self._async_state is not None:
            if self._async_state_loop is not loop:
                raise RuntimeError(
                    "RabbitMQBroker async APIs must be used from a single event loop. "
                    "Use send_sync for cross-thread producers."
                )
            return self._async_state

        async with self._async_state_lock:
            if self._async_state is None:
                self._async_state = _BrokerState()
                self._async_state_loop = loop
            return self._async_state

    def _connection_url(self, *, connection_name: str) -> str:
        # aiormq는 connection_name(관리 UI 표시명)과 heartbeat를 URL 쿼리에서 읽는다.
        # aio-pika의 client_properties 인자는 URL 문자열과 함께 쓰면 무시된다.
        parsed = urllib.parse.urlsplit(self.url)
        query = dict(urllib.parse.parse_qsl(parsed.query))
        query["name"] = connection_name
        if self.heartbeat_seconds is not None:
            query["heartbeat"] = str(int(self.heartbeat_seconds))
        return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query)))

    async def _connect_robust(self, *, connection_name: str) -> aio_pika.abc.AbstractRobustConnection:
        return await aio_pika.connect_robust(
            self._connection_url(connection_name=connection_name),
            reconnect_interval=self.reconnect_interval_seconds,
        )

    async def _publish_channel(self, state: _BrokerState) -> aio_pika.abc.AbstractChannel:
        if state.closed:
            raise ConnectionError("RabbitMQ broker state is closed.")
        # hot path: 이미 열린 채널은 락 없이 반환한다
        channel = state.publish_channel
        if (
            channel is not None
            and not channel.is_closed
            and state.publish_connection is not None
            and not state.publish_connection.is_closed
        ):
            return channel
        # 최초 사용 경합으로 커넥션이 이중 생성·누수되지 않도록 상태 락으로 직렬화한다
        async with state.lock:
            if state.closed:
                raise ConnectionError("RabbitMQ broker state is closed.")
            if state.publish_connection is None or state.publish_connection.is_closed:
                state.publish_connection = await self._connect_robust(
                    connection_name=f"{self.client_name}:publish",
                )
                state.publish_channel = None
            if state.publish_channel is None or state.publish_channel.is_closed:
                state.publish_channel = await state.publish_connection.channel(
                    publisher_confirms=self.publisher_confirms,
                )
            return state.publish_channel

    async def _consume_connection(self, state: _BrokerState) -> aio_pika.abc.AbstractRobustConnection:
        if state.closed:
            raise ConnectionError("RabbitMQ broker state is closed.")
        connection = state.consume_connection
        if connection is not None and not connection.is_closed:
            return connection
        async with state.lock:
            if state.closed:
                raise ConnectionError("RabbitMQ broker state is closed.")
            if state.consume_connection is None or state.consume_connection.is_closed:
                state.consume_connection = await self._connect_robust(
                    connection_name=f"{self.client_name}:consume",
                )
            return state.consume_connection

    # -- 토폴로지 ----------------------------------------------------------

    def _main_queue_arguments(self) -> Optional[dict[str, Any]]:
        if self.consumer_timeout_seconds is None:
            return None
        return {"x-consumer-timeout": int(self.consumer_timeout_seconds * 1000)}

    async def _declare_main_queue(
        self,
        channel: aio_pika.abc.AbstractChannel,
        queue_name: str,
        *,
        robust: bool = True,
    ):
        return await channel.declare_queue(
            self._queue_name(queue_name),
            durable=True,
            arguments=self._main_queue_arguments(),
            robust=robust,
        )

    async def _declare_delayed_queue(
        self,
        channel: aio_pika.abc.AbstractChannel,
        queue_name: str,
        *,
        robust: bool = True,
    ):
        return await channel.declare_queue(
            self._delayed_queue_name(queue_name),
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self._queue_name(queue_name),
            },
            robust=robust,
        )

    async def _declare_dead_letter_queue(
        self,
        channel: aio_pika.abc.AbstractChannel,
        queue_name: str,
        *,
        robust: bool = True,
    ):
        return await channel.declare_queue(
            self._dead_letter_queue_name(queue_name),
            durable=True,
            robust=robust,
        )

    async def _ensure_topology(self, queue_name: str, *, state: Optional[_BrokerState] = None) -> None:
        if queue_name in self._declared_topologies:
            return

        if state is None:
            state = await self._get_async_state()
        channel = await self._publish_channel(state)
        await self._declare_main_queue(channel, queue_name)
        await self._declare_delayed_queue(channel, queue_name)
        await self._declare_dead_letter_queue(channel, queue_name)
        self._declared_topologies.add(queue_name)

    # -- 전송 --------------------------------------------------------------

    def _validate_no_deduplication(self, message: Message) -> None:
        # RedisBroker._normalize_deduplication과 같은 판정을 사용해, Redis가
        # no-dedup으로 취급하는 퇴화 옵션(dict가 아니거나 id가 비어 있는 경우)은
        # 그대로 통과시키고 실질적 dedupe 요청만 거부한다
        options = message.options
        raw = options.get("deduplication")
        if raw is None:
            job_id = options.get("job_id")
            if job_id in {None, ""}:
                return
        elif not isinstance(raw, dict) or raw.get("id") in {None, ""}:
            return

        raise ValueError(
            "RabbitMQBroker does not support message deduplication "
            "('deduplication'/'job_id' options). Use RedisBroker for deduplicated sends."
        )

    async def _publish_message(
        self,
        message: Message,
        *,
        delay: Optional[float],
        state: Optional[_BrokerState] = None,
    ) -> Message:
        if state is None:
            state = await self._get_async_state()
        self.declare_queue(message.queue_name)
        await self._ensure_topology(message.queue_name, state=state)
        channel = await self._publish_channel(state)

        if delay is not None and delay > 0:
            routing_key = self._delayed_queue_name(message.queue_name)
            expiration: Optional[float] = float(delay)
        else:
            routing_key = self._queue_name(message.queue_name)
            expiration = None

        await channel.default_exchange.publish(
            aio_pika.Message(
                body=self._encode_message(message),
                message_id=message.message_id,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=expiration,
                timestamp=int(message.message_timestamp / 1000),
            ),
            routing_key=routing_key,
        )
        return message

    async def send(self, message: Message, *, delay: Optional[float] = None) -> Message:
        self._validate_no_deduplication(message)
        self.declare_queue(message.queue_name)
        return await self._publish_message(message, delay=delay)

    async def send_for_retry(self, message: Message, *, delay: Optional[float] = None) -> Message:
        self.declare_queue(message.queue_name)
        return await self._publish_message(message, delay=delay)

    def send_sync(self, message: Message, *, delay: Optional[float] = None) -> Message:
        self._validate_no_deduplication(message)
        self.declare_queue(message.queue_name)

        # 상태 선택과 제출을 파사드 락 아래에서 원자적으로 수행해, close()와의
        # 경합으로 정지된 루프에 제출되거나 닫힌 상태가 재생성되는 것을 막는다
        with self._sync_facade_lock:
            state = self._sync_state
            if state is None or state.closed:
                state = self._sync_state = _BrokerState()
            future = self._sync_runner.submit(
                self._publish_message(message, delay=delay, state=state)
            )
        return future.result(30.0)

    # -- 소비 --------------------------------------------------------------

    async def open_consumer(self, queue_name: str, *, prefetch: int = 1) -> Consumer:
        self.declare_queue(queue_name)
        state = await self._get_async_state()
        await self._ensure_topology(queue_name, state=state)
        consumer = _RabbitMQConsumer(
            self,
            queue_name,
            prefetch=prefetch,
            consumer_name=self._consumer_name(queue_name),
        )
        self._consumers.add(consumer)
        return consumer

    def _forget_consumer(self, consumer: "_RabbitMQConsumer") -> None:
        self._consumers.discard(consumer)

    def _local_unacked_count(self, queue_name: str) -> int:
        return sum(
            consumer.unacked_count
            for consumer in self._consumers
            if consumer.queue_name == queue_name
        )

    # -- dead letter -------------------------------------------------------

    def _dead_letter_record_ttl_ms(self, record: DeadLetterRecord, *, now_ms: Optional[int] = None) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        if record.retention_deadline_ms is None:
            record.retention_deadline_ms = now_ms + self.dead_letter_ttl_ms
            return self.dead_letter_ttl_ms

        return max(int(record.retention_deadline_ms) - now_ms, 1)

    async def _store_dead_letter_record(
        self,
        record: DeadLetterRecord,
        *,
        state: Optional[_BrokerState] = None,
    ) -> None:
        if state is None:
            state = await self._get_async_state()
        await self._ensure_topology(record.queue_name, state=state)
        channel = await self._publish_channel(state)
        ttl_ms = self._dead_letter_record_ttl_ms(record)
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=self._encode_dead_letter_record(record),
                message_id=record.dead_letter_id,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=ttl_ms / 1000,
            ),
            routing_key=self._dead_letter_queue_name(record.queue_name),
        )

    def _dead_letter_scan_lock_queue_name(self, queue_name: str) -> str:
        return f"{self.namespace}:dlq-scan-lock:{queue_name}"

    async def _acquire_dead_letter_scan_lock(self, connection, queue_name: str):
        """배타 큐 선언으로 프로세스/브로커 인스턴스 간 DLQ 스캔을 직렬화한다.

        스캔은 basic_get으로 레코드를 잠시 보이지 않게 만들기 때문에, 동시 스캔이
        서로에게 빈 DLQ로 보이는 것을 막아야 한다. 배타 큐의 소유자는 커넥션이므로
        보유자가 크래시해도 커넥션이 닫히며 락이 자동 해제된다.
        """
        lock_queue_name = self._dead_letter_scan_lock_queue_name(queue_name)
        deadline = time.monotonic() + self.dead_letter_scan_lock_timeout_seconds
        delay = 0.05
        while True:
            channel = await connection.channel()
            try:
                await channel.declare_queue(
                    lock_queue_name,
                    exclusive=True,
                    auto_delete=True,
                    robust=False,
                )
                return channel, lock_queue_name
            except ChannelLockedResource:
                # RESOURCE_LOCKED(405)는 채널을 죽이므로 새 채널로 재시도한다
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out acquiring the dead letter scan lock for queue {queue_name!r}."
                    ) from None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.5)

    async def _release_dead_letter_scan_lock(self, lock_channel, lock_queue_name: str) -> None:
        # 배타 큐는 커넥션이 살아 있는 동안 유지되므로 명시적으로 삭제한다
        try:
            await lock_channel.queue_delete(lock_queue_name)
        except _TRANSPORT_ERRORS:
            pass
        try:
            if not lock_channel.is_closed:
                await lock_channel.close()
        except _TRANSPORT_ERRORS:
            pass

    async def _peek_dead_letter_messages(self, channel, queue_name: str):
        """DLQ의 모든 메시지를 ack 없이 basic_get으로 조회한다. 호출자가 ack하거나
        채널을 닫아 다시 큐로 되돌린다."""
        queue = await self._declare_dead_letter_queue(channel, queue_name, robust=False)
        entries = []
        while True:
            try:
                incoming = await queue.get(no_ack=False, fail=False)
            except QueueEmpty:
                break
            if incoming is None:
                break
            entries.append(incoming)
        return entries

    async def _scan_dead_letters(self, queue_name: str, visitor) -> None:
        """일회용 채널에서 DLQ 항목마다 `visitor(incoming, record)`를 실행한다.
        채널을 닫으면 ack되지 않은 항목은 전부 다시 큐로 돌아간다."""
        state = await self._get_async_state()
        connection = await self._consume_connection(state)
        async with self._dead_letter_lock:
            lock_channel, lock_queue_name = await self._acquire_dead_letter_scan_lock(
                connection,
                queue_name,
            )
            channel = await connection.channel()
            try:
                now_ms = int(time.time() * 1000)
                for incoming in await self._peek_dead_letter_messages(channel, queue_name):
                    try:
                        record = self._decode_dead_letter_record(incoming.body)
                    except Exception:
                        # 디코드 실패는 버전 스큐일 수 있으므로 삭제하지 않고 남겨 둔다.
                        # 채널이 닫히면 메시지는 다시 큐로 돌아간다.
                        logger.warning(
                            "Skipping undecodable dead letter payload on queue %r.",
                            queue_name,
                            exc_info=True,
                        )
                        continue

                    if (
                        record.retention_deadline_ms is not None
                        and int(record.retention_deadline_ms) <= now_ms
                    ):
                        await incoming.ack()
                        continue

                    should_continue = await visitor(incoming, record)
                    if not should_continue:
                        break
            finally:
                try:
                    await channel.close()
                except _TRANSPORT_ERRORS:
                    pass
                await self._release_dead_letter_scan_lock(lock_channel, lock_queue_name)

    async def get_dead_letter_records(self, queue_name: str) -> list[DeadLetterRecord]:
        records_by_id: dict[str, DeadLetterRecord] = {}

        async def collect(_incoming, record: DeadLetterRecord) -> bool:
            existing = records_by_id.get(record.dead_letter_id)
            # requeue/purge 도중 crash로 남은 스테일 active 사본이 있으면
            # resolved 사본을 우선한다
            if existing is None or (
                existing.resolution_state == "active"
                and record.resolution_state != "active"
            ):
                records_by_id[record.dead_letter_id] = record
            return True

        await self._scan_dead_letters(queue_name, collect)
        records = [
            record
            for record in records_by_id.values()
            if record.resolution_state == "active"
        ]
        records.sort(key=lambda record: (record.dead_lettered_at_ms, record.dead_letter_id))
        return records

    async def get_dead_letter_record(self, queue_name: str, dead_letter_id: str) -> Optional[DeadLetterRecord]:
        found: list[DeadLetterRecord] = []

        async def collect(_incoming, record: DeadLetterRecord) -> bool:
            if record.dead_letter_id == dead_letter_id and record.queue_name == queue_name:
                found.append(record)
                return False
            return True

        await self._scan_dead_letters(queue_name, collect)
        return found[0] if found else None

    async def get_dead_letters(self, queue_name: str) -> list[Message]:
        messages: list[Message] = []
        for record in await self.get_dead_letter_records(queue_name):
            message = record.to_message()
            if message is not None:
                messages.append(message)
        return messages

    async def _resolve_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        resolution_state: str,
        note: Optional[str],
        requeue_message: bool,
    ) -> Optional[DeadLetterRecord]:
        resolved: list[DeadLetterRecord] = []

        async def resolve(incoming, record: DeadLetterRecord) -> bool:
            if record.dead_letter_id != dead_letter_id or record.queue_name != queue_name:
                return True

            if record.resolution_state != "active":
                resolved.append(record)
                return False

            if requeue_message:
                message = record.to_message()
                if message is None:
                    raise ValueError(
                        "Dead letter record does not contain a message snapshot and cannot be requeued."
                    )
                await self._publish_message(message, delay=None)

            record.resolution_state = resolution_state  # type: ignore[assignment]
            record.resolved_at_ms = int(time.time() * 1000)
            record.resolution_note = note
            await self._store_dead_letter_record(record)
            await incoming.ack()
            resolved.append(record)
            return False

        await self._scan_dead_letters(queue_name, resolve)
        return resolved[0] if resolved else None

    async def requeue_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        return await self._resolve_dead_letter(
            queue_name,
            dead_letter_id,
            resolution_state="requeued",
            note=note,
            requeue_message=True,
        )

    async def purge_dead_letter(
        self,
        queue_name: str,
        dead_letter_id: str,
        *,
        note: Optional[str] = None,
    ) -> Optional[DeadLetterRecord]:
        return await self._resolve_dead_letter(
            queue_name,
            dead_letter_id,
            resolution_state="purged",
            note=note,
            requeue_message=False,
        )

    # -- flush / join ------------------------------------------------------

    async def flush(self, queue_name: str) -> None:
        """메인/지연/DLQ 큐를 purge한다.

        AMQP ``queue.purge``는 ack 대기 중(unacked)인 메시지를 제거하지 못한다.
        컨슈머에게 전달된(프리페치 버퍼 포함) 메시지는 flush 후에도 살아남아
        소유 채널이 닫히거나 nack되면 다시 큐로 돌아온다. 스트림 키 삭제로 PEL까지
        제거하는 RedisBroker.flush와 다른 지점이다.
        """
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        state = await self._get_async_state()
        await self._ensure_topology(queue_name, state=state)
        channel = await self._publish_channel(state)
        for declare in (
            self._declare_main_queue,
            self._declare_delayed_queue,
            self._declare_dead_letter_queue,
        ):
            queue = await declare(channel, queue_name)
            await queue.purge()

    async def _management_queue_stats(self, queue_name: str) -> Optional[dict[str, Any]]:
        try:
            stats = await asyncio.to_thread(self.management.queue_stats, self._queue_name(queue_name))
        except Exception:
            if not self._management_unavailable_logged:
                self._management_unavailable_logged = True
                logger.warning(
                    "RabbitMQ management API is unavailable at %r; join() and queue "
                    "diagnostics fall back to AMQP-visible counts only.",
                    self.management.base_url,
                    exc_info=True,
                )
            return None
        return stats

    async def _queue_counts(self, queue_name: str) -> tuple[int, int]:
        # 폴링 선언은 robust=False: RobustChannel의 재선언 레지스트리에
        # 폴링 횟수만큼 큐 객체가 무한 누적되는 것을 막는다
        state = await self._get_async_state()
        channel = await self._publish_channel(state)
        main_queue = await self._declare_main_queue(channel, queue_name, robust=False)
        delayed_queue = await self._declare_delayed_queue(channel, queue_name, robust=False)
        return (
            int(main_queue.declaration_result.message_count or 0),
            int(delayed_queue.declaration_result.message_count or 0),
        )

    async def join(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        await self._ensure_topology(queue_name)
        sleep_interval = self.join_poll_interval_seconds
        previous_state: Optional[tuple[int, int, int, Optional[int]]] = None
        idle_since: Optional[float] = None

        while True:
            ready_count, delayed_count = await self._queue_counts(queue_name)
            local_unacked = self._local_unacked_count(queue_name)
            stats = await self._management_queue_stats(queue_name)
            management_unacked = (
                None if stats is None else int(stats.get("messages_unacknowledged", 0) or 0)
            )

            busy = bool(
                ready_count
                or delayed_count
                or local_unacked
                or (management_unacked or 0)
            )
            now = time.monotonic()
            if busy:
                idle_since = None
            else:
                if idle_since is None:
                    idle_since = now
                if now - idle_since >= self.join_idle_grace_seconds:
                    return

            state_tuple = (ready_count, delayed_count, local_unacked, management_unacked)
            if state_tuple == previous_state:
                sleep_interval = min(
                    sleep_interval * self.join_poll_interval_multiplier,
                    self.join_poll_interval_max_seconds,
                )
            else:
                sleep_interval = self.join_poll_interval_seconds
            previous_state = state_tuple
            await asyncio.sleep(sleep_interval)

    # -- 진단 --------------------------------------------------------------

    async def list_runtime_queues(self) -> list[str]:
        queue_names = set(self.queues)
        prefix = f"{self.namespace}:queue:"
        try:
            amqp_names = await asyncio.to_thread(self.management.list_queue_names)
        except Exception:
            return sorted(queue_names)

        for amqp_name in amqp_names:
            if amqp_name.startswith(prefix):
                queue_name = amqp_name[len(prefix):]
                if queue_name:
                    queue_names.add(queue_name)
        return sorted(queue_names)

    async def get_queue_runtime_row(
        self,
        queue_name: str,
        *,
        pending_idle_threshold_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        del pending_idle_threshold_ms
        await self._ensure_topology(queue_name)
        ready_count, delayed_count = await self._queue_counts(queue_name)
        stats = await self._management_queue_stats(queue_name)
        management_unacked = 0 if stats is None else int(stats.get("messages_unacknowledged", 0) or 0)

        return {
            "queue_name": queue_name,
            "serving_revision": await self.get_serving_revision(queue_name),
            "stream_length": ready_count,
            "delayed_count": delayed_count,
            "pending_count": max(management_unacked, self._local_unacked_count(queue_name)),
            "pending_stale_count": 0,
            "worker_ids": [],
        }

    async def get_queue_runtime_rows(
        self,
        queue_names: list[str],
        *,
        pending_idle_threshold_ms: Optional[int] = None,
        include_worker_ids: bool = True,
    ) -> list[dict[str, Any]]:
        del include_worker_ids
        return [
            await self.get_queue_runtime_row(
                queue_name,
                pending_idle_threshold_ms=pending_idle_threshold_ms,
            )
            for queue_name in queue_names
        ]

    async def list_worker_runtime_rows(
        self,
        *,
        queue_names: Optional[set[str]] = None,
    ) -> list[dict[str, str]]:
        del queue_names
        return []

    # -- 수명 주기 ---------------------------------------------------------

    async def close(self) -> None:
        for consumer in list(self._consumers):
            try:
                await consumer.close(forget=True)
            except _TRANSPORT_ERRORS:
                pass
        self._consumers.clear()

        state = self._async_state
        self._async_state = None
        self._async_state_loop = None
        if state is not None:
            await state.close()

        # 러너 정지는 스레드 join으로 블로킹되므로 이벤트 루프 밖에서 수행한다.
        # 파사드 락으로 send_sync 제출과 직렬화해 정지된 루프에 제출되는 일이 없게 한다.
        def stop_sync_facade() -> None:
            with self._sync_facade_lock:
                sync_state = self._sync_state
                self._sync_state = None
                if sync_state is not None:
                    self._sync_runner.stop(sync_state.close())
                else:
                    self._sync_runner.stop()

        await asyncio.to_thread(stop_sync_facade)

        self._declared_topologies.clear()


class _RabbitMQConsumer(Consumer):
    def __init__(
        self,
        broker: RabbitMQBroker,
        queue_name: str,
        *,
        prefetch: int,
        consumer_name: str,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = max(int(prefetch), 1)
        self.consumer_name = consumer_name
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._consumer_tag: Optional[str] = None
        self._queue: Optional[aio_pika.abc.AbstractQueue] = None
        self._buffer: asyncio.Queue[Optional[aio_pika.abc.AbstractIncomingMessage]] = asyncio.Queue()
        self._unacked = 0
        self._closed = False
        self._start_lock = asyncio.Lock()

    @property
    def unacked_count(self) -> int:
        return self._unacked

    async def _on_message(self, incoming: aio_pika.abc.AbstractIncomingMessage) -> None:
        self._unacked += 1
        self._buffer.put_nowait(incoming)

    async def _ensure_started(self) -> None:
        if self._closed:
            raise ConnectionError("RabbitMQ consumer is closed.")
        if self._consumer_tag is not None:
            return

        async with self._start_lock:
            if self._closed:
                raise ConnectionError("RabbitMQ consumer is closed.")
            if self._consumer_tag is not None:
                return
            try:
                state = await self.broker._get_async_state()
                connection = await self.broker._consume_connection(state)
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.prefetch)
                queue = await self.broker._declare_main_queue(channel, self.queue_name)
                self._channel = channel
                self._queue = queue
                self._consumer_tag = await queue.consume(
                    self._on_message,
                    consumer_tag=self.consumer_name,
                )
            except _TRANSPORT_ERRORS as exc:
                raise ConnectionError(f"Failed to start RabbitMQ consumer: {exc}") from exc

    async def receive(self, *, limit: int, timeout: Optional[float]) -> list[Delivery]:
        await self._ensure_started()
        limit = max(int(limit), 1)

        incoming_messages: list[aio_pika.abc.AbstractIncomingMessage] = []
        try:
            if timeout is None:
                first = await self._buffer.get()
            else:
                first = await asyncio.wait_for(self._buffer.get(), max(float(timeout), 0.001))
        except asyncio.TimeoutError:
            return []

        if first is None:
            # 다른 대기자도 깨어날 수 있게 센티널을 되돌려 놓는다
            self._buffer.put_nowait(None)
            raise ConnectionError("RabbitMQ consumer is closed.")
        incoming_messages.append(first)

        batch_window = self.broker.receive_batch_window_seconds
        while len(incoming_messages) < limit:
            try:
                item = self._buffer.get_nowait()
            except asyncio.QueueEmpty:
                if batch_window <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self._buffer.get(), batch_window)
                except asyncio.TimeoutError:
                    break
            if item is None:
                self._buffer.put_nowait(None)
                break
            incoming_messages.append(item)

        return await self._build_deliveries(incoming_messages)

    async def _build_deliveries(
        self,
        incoming_messages: list[aio_pika.abc.AbstractIncomingMessage],
    ) -> list[Delivery]:
        deliveries: list[Delivery] = []
        for incoming in incoming_messages:
            try:
                message = self.broker._decode_message(bytes(incoming.body))
            except BaseException as exc:
                await self._dead_letter_undecodable(incoming, exc)
                continue

            deliveries.append(
                Delivery(
                    message=message,
                    redelivered=bool(incoming.redelivered),
                    transport_id=str(incoming.delivery_tag),
                    lease_deadline=None,
                    metadata={
                        "queue_name": self.queue_name,
                        "consumer_name": self.consumer_name,
                        "message_id": message.message_id,
                        "amqp_message": incoming,
                    },
                )
            )
        return deliveries

    async def _dead_letter_undecodable(
        self,
        incoming: aio_pika.abc.AbstractIncomingMessage,
        exc: BaseException,
    ) -> None:
        now_ms = int(time.time() * 1000)
        record = DeadLetterRecord(
            namespace=self.broker.namespace,
            queue_name=self.queue_name,
            actor_name="<unknown>",
            message_id=str(incoming.message_id or ""),
            delivery_id=str(incoming.delivery_tag),
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
            retention_deadline_ms=now_ms + self.broker.dead_letter_ttl_ms,
        )
        await self.broker._store_dead_letter_record(record)
        await self._ack_incoming(incoming)

    async def _ack_incoming(self, incoming: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            await incoming.ack()
        except _TRANSPORT_ERRORS:
            # ack 실패는 채널이 죽어 전달 소유권을 잃었다는 뜻이고, 서버가 곧
            # 재전달한다(재전달은 _on_message로 다시 카운트됨). 존재하지 않는
            # 항목에 대한 XACK가 no-op인 Redis와 같은 시맨틱으로 삼키고,
            # 카운터는 반드시 해제해 join()이 영구 대기하지 않게 한다.
            logger.warning(
                "Failed to ack RabbitMQ delivery on queue %r; the broker will redeliver it.",
                self.queue_name,
                exc_info=True,
            )
        finally:
            self._unacked = max(self._unacked - 1, 0)

    async def ack(self, delivery: Delivery) -> None:
        incoming = delivery.metadata.get("amqp_message")
        if incoming is None:
            return
        await self._ack_incoming(incoming)

    async def reject(self, delivery: Delivery, *, requeue: bool = False) -> None:
        incoming = delivery.metadata.get("amqp_message")

        if requeue:
            await self.broker.send_for_retry(delivery.message)
            if incoming is not None:
                await self._ack_incoming(incoming)
            return

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
        await self.broker._store_dead_letter_record(record)
        if incoming is not None:
            await self._ack_incoming(incoming)

    async def close(self, *, forget: bool = False) -> None:
        del forget
        if self._closed:
            return

        # _ensure_started와 같은 락으로 직렬화해, 시작 중인 컨슈머가 close 이후
        # 채널을 되살리는 레이스를 막는다. cancel/close RPC는 브로커 장애 시
        # robust 재시도로 무기한 행이 될 수 있어 타임아웃을 건다.
        async with self._start_lock:
            if self._closed:
                return
            self._closed = True
            self.broker._forget_consumer(self)
            self._buffer.put_nowait(None)

            if self._queue is not None and self._consumer_tag is not None:
                try:
                    await asyncio.wait_for(
                        self._queue.cancel(self._consumer_tag),
                        DEFAULT_RABBITMQ_CLOSE_TIMEOUT_SECONDS,
                    )
                except (asyncio.TimeoutError, *_TRANSPORT_ERRORS):
                    pass
            if self._channel is not None and not self._channel.is_closed:
                try:
                    await asyncio.wait_for(
                        self._channel.close(),
                        DEFAULT_RABBITMQ_CLOSE_TIMEOUT_SECONDS,
                    )
                except (asyncio.TimeoutError, *_TRANSPORT_ERRORS):
                    pass

            self._unacked = 0
            self._consumer_tag = None
            self._queue = None
            self._channel = None
