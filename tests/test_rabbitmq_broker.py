from __future__ import annotations

import asyncio
import base64
import json
import multiprocessing as mp
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4

import fluxera
from fluxera.errors import QueueNotFound

try:
    import aio_pika
except ImportError:  # pragma: no cover - environment guard
    aio_pika = None


RABBITMQ_URL = os.environ.get("FLUXERA_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
RABBITMQ_MANAGEMENT_URL = os.environ.get("FLUXERA_RABBITMQ_MANAGEMENT_URL", "http://guest:guest@127.0.0.1:15672/")


async def wait_for(predicate, *, timeout: float = 4.0, interval: float = 0.02) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Condition was not met before timeout.")
        await asyncio.sleep(interval)


async def wait_for_async(predicate, *, timeout: float = 4.0, interval: float = 0.02) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Async condition was not met before timeout.")
        await asyncio.sleep(interval)


class _ManagementClient:
    """테스트 검증 전용 최소 RabbitMQ management API 클라이언트."""

    def __init__(self, management_url: str) -> None:
        parsed = urllib.parse.urlsplit(management_url)
        username = urllib.parse.unquote(parsed.username or "guest")
        password = urllib.parse.unquote(parsed.password or "guest")
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 15672
        self.base_url = f"{parsed.scheme}://{host}:{port}"
        credentials = f"{username}:{password}".encode("utf-8")
        self.auth_header = "Basic " + base64.b64encode(credentials).decode("ascii")

    def _request(self, method: str, path: str):
        request = urllib.request.Request(f"{self.base_url}{path}", method=method)
        request.add_header("Authorization", self.auth_header)
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = response.read()
        if not payload:
            return None
        return json.loads(payload)

    def list_queue_names(self, prefix: str) -> list[str]:
        queues = self._request("GET", "/api/queues/%2F") or []
        return [queue["name"] for queue in queues if queue["name"].startswith(prefix)]

    def delete_queue(self, queue_name: str) -> None:
        encoded = urllib.parse.quote(queue_name, safe="")
        try:
            self._request("DELETE", f"/api/queues/%2F/{encoded}")
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise

    def connection_client_names(self) -> list[str]:
        connections = self._request("GET", "/api/connections") or []
        names = []
        for connection in connections:
            properties = connection.get("client_properties") or {}
            name = properties.get("connection_name")
            if name:
                names.append(name)
        return names

    def queue_info(self, queue_name: str) -> dict:
        encoded = urllib.parse.quote(queue_name, safe="")
        return self._request("GET", f"/api/queues/%2F/{encoded}") or {}

    def close_connections_matching(self, connection_name_prefix: str) -> int:
        """client_properties.connection_name이 prefix로 시작하는 커넥션을 서버에서 강제 종료한다."""
        closed = 0
        for connection in self._request("GET", "/api/connections") or []:
            properties = connection.get("client_properties") or {}
            if not str(properties.get("connection_name", "")).startswith(connection_name_prefix):
                continue
            encoded = urllib.parse.quote(connection["name"], safe="")
            try:
                self._request("DELETE", f"/api/connections/{encoded}")
                closed += 1
            except urllib.error.HTTPError as exc:
                if exc.code != 404:
                    raise
        return closed


def crash_after_side_effect(counter_path: str, crash_flag_path: str) -> None:
    with open(counter_path, "a", encoding="utf-8") as counter_file:
        counter_file.write("x")
        counter_file.flush()
        os.fsync(counter_file.fileno())
    if os.path.exists(crash_flag_path):
        os.unlink(crash_flag_path)
        os._exit(17)


def append_marker(counter_path: str) -> None:
    with open(counter_path, "a", encoding="utf-8") as counter_file:
        counter_file.write("x")


async def _run_worker_once(amqp_url: str, namespace: str, *, send_initial: bool, counter_path: str, crash_flag_path: str) -> None:
    broker = fluxera.RabbitMQBroker(amqp_url, namespace=namespace, management_url=RABBITMQ_MANAGEMENT_URL)
    actor = fluxera.actor(
        broker=broker,
        actor_name="crash_after_side_effect",
        queue_name="default",
        execution="thread",
    )(crash_after_side_effect)
    worker = fluxera.Worker(broker, concurrency=1, thread_concurrency=1, process_concurrency=0)

    await worker.start()
    try:
        if send_initial:
            await actor.send(counter_path, crash_flag_path)
        await broker.join(actor.queue_name)
    finally:
        await worker.stop()


def _worker_process_entry(amqp_url: str, namespace: str, send_initial: bool, counter_path: str, crash_flag_path: str) -> None:
    asyncio.run(
        _run_worker_once(
            amqp_url,
            namespace,
            send_initial=send_initial,
            counter_path=counter_path,
            crash_flag_path=crash_flag_path,
        )
    )


@unittest.skipIf(aio_pika is None, "aio-pika is not installed")
class RabbitMQBrokerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.amqp_url = RABBITMQ_URL
        self.namespace = f"fluxera-test-{uuid4().hex}"
        self.management = _ManagementClient(RABBITMQ_MANAGEMENT_URL)
        self.brokers: list[fluxera.RabbitMQBroker] = []

        try:
            connection = await asyncio.wait_for(aio_pika.connect(self.amqp_url), timeout=3.0)
        except Exception as exc:
            self.skipTest(f"RabbitMQ is not available for integration tests: {exc}")
        else:
            await connection.close()

    async def asyncTearDown(self) -> None:
        for broker in self.brokers:
            await broker.close()

        for queue_name in await asyncio.to_thread(self.management.list_queue_names, self.namespace):
            await asyncio.to_thread(self.management.delete_queue, queue_name)

    def make_broker(self, **overrides) -> fluxera.RabbitMQBroker:
        # 브로커의 management 호출도 (기본 유도 포트가 아니라) env로 지정된
        # 서버를 향하게 해, 대체 브로커(LavinMQ 등) 검증 시 엇갈리지 않게 한다
        params = {"namespace": self.namespace, "management_url": RABBITMQ_MANAGEMENT_URL}
        params.update(overrides)
        broker = fluxera.RabbitMQBroker(self.amqp_url, **params)
        self.brokers.append(broker)
        return broker

    async def _passive_message_count(self, queue_name: str) -> int:
        connection = await aio_pika.connect(self.amqp_url)
        try:
            channel = await connection.channel()
            try:
                queue = await channel.declare_queue(queue_name, passive=True)
            except Exception:
                return 0
            return queue.declaration_result.message_count
        finally:
            await connection.close()

    async def test_send_and_receive_roundtrip_preserves_message(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str, *, tag: bytes = b"") -> None:
            del value, tag

        sent = await remember.send_with_options(args=("alpha",), kwargs={"tag": b"\x01\x02"})

        consumer = await broker.open_consumer(remember.queue_name)
        deliveries = await consumer.receive(limit=1, timeout=2.0)

        self.assertEqual(len(deliveries), 1)
        delivery = deliveries[0]
        self.assertEqual(delivery.message, sent)
        self.assertFalse(delivery.redelivered)
        self.assertIsNotNone(delivery.transport_id)
        self.assertEqual(delivery.metadata["queue_name"], "default")
        self.assertNotIn("lease_seconds", delivery.metadata)

        await consumer.ack(delivery)
        await consumer.close()

    async def test_receive_timeout_returns_empty_list(self) -> None:
        broker = self.make_broker()
        broker.declare_queue("default")

        consumer = await broker.open_consumer("default")
        deliveries = await consumer.receive(limit=1, timeout=0.1)

        self.assertEqual(deliveries, [])
        await consumer.close()

    async def test_receive_respects_prefetch_before_ack(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def noop() -> None:
            return None

        for _ in range(5):
            await noop.send()

        consumer = await broker.open_consumer(noop.queue_name, prefetch=2)
        first_batch = await consumer.receive(limit=5, timeout=2.0)
        self.assertEqual(len(first_batch), 2)

        for delivery in first_batch:
            await consumer.ack(delivery)

        second_batch = await consumer.receive(limit=5, timeout=2.0)
        self.assertEqual(len(second_batch), 2)
        for delivery in second_batch:
            await consumer.ack(delivery)

        third_batch = await consumer.receive(limit=5, timeout=2.0)
        self.assertEqual(len(third_batch), 1)
        await consumer.ack(third_batch[0])
        await consumer.close()

    async def test_ack_removes_message_from_queue(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def noop() -> None:
            return None

        await noop.send()
        consumer = await broker.open_consumer(noop.queue_name)
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.ack(delivery)
        await consumer.close()

        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)
        await asyncio.wait_for(broker.join("default"), timeout=4.0)

    async def test_delayed_send_is_not_visible_until_delay_elapses(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        await remember.send_with_options(args=("later",), delay=0.7)

        consumer = await broker.open_consumer(remember.queue_name)
        early = await consumer.receive(limit=1, timeout=0.2)
        self.assertEqual(early, [])
        self.assertEqual(await self._passive_message_count(broker._delayed_queue_name("default")), 1)

        deliveries = await consumer.receive(limit=1, timeout=3.0)
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0].args, ("later",))
        await consumer.ack(deliveries[0])
        await consumer.close()

    async def test_send_for_retry_with_delay_uses_delayed_queue(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        message = remember.message("retry-me")
        await broker.send_for_retry(message, delay=30.0)

        self.assertEqual(await self._passive_message_count(broker._delayed_queue_name("default")), 1)
        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)

    async def test_reject_with_requeue_redelivers_message(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        sent = await remember.send("alpha")
        consumer = await broker.open_consumer(remember.queue_name)
        first = (await consumer.receive(limit=1, timeout=2.0))[0]

        await consumer.reject(first, requeue=True)

        second = (await consumer.receive(limit=1, timeout=2.0))[0]
        self.assertEqual(second.message.message_id, sent.message_id)
        self.assertEqual(second.args, ("alpha",))

        await consumer.ack(second)
        await consumer.close()
        await asyncio.wait_for(broker.join(remember.queue_name), timeout=4.0)

    async def test_reject_without_requeue_moves_message_to_dead_letters(self) -> None:
        broker = self.make_broker()

        async def noop() -> None:
            return None

        actor = fluxera.actor(
            broker=broker,
            actor_name="reject_me",
            queue_name="default",
        )(noop)

        await actor.send()
        consumer = await broker.open_consumer(actor.queue_name)
        deliveries = await consumer.receive(limit=1, timeout=2.0)
        self.assertEqual(len(deliveries), 1)

        await consumer.reject(deliveries[0], requeue=False)
        await asyncio.wait_for(broker.join(actor.queue_name), timeout=4.0)

        dead_letter_records = await broker.get_dead_letter_records(actor.queue_name)
        dead_letters = await broker.get_dead_letters(actor.queue_name)

        self.assertEqual(len(dead_letter_records), 1)
        self.assertEqual(len(dead_letters), 1)
        self.assertEqual(dead_letter_records[0].failure_kind, "operator_reject")
        self.assertEqual(dead_letter_records[0].actor_name, actor.actor_name)
        self.assertEqual(dead_letter_records[0].message_id, deliveries[0].message_id)
        self.assertEqual(dead_letters[0].message_id, deliveries[0].message_id)
        await consumer.close()

    async def test_dead_letter_records_can_be_requeued_and_purged(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, actor_name="admin_me", queue_name="default")
        async def noop(value: str) -> str:
            return value

        await noop.send("first")
        consumer = await broker.open_consumer(noop.queue_name)
        first_delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.reject(first_delivery, requeue=False)

        records = await broker.get_dead_letter_records(noop.queue_name)
        self.assertEqual(len(records), 1)
        first_dead_letter_id = records[0].dead_letter_id

        requeued = await broker.requeue_dead_letter(noop.queue_name, first_dead_letter_id, note="retry once")
        self.assertIsNotNone(requeued)
        assert requeued is not None
        self.assertEqual(requeued.resolution_state, "requeued")
        self.assertEqual(requeued.resolution_note, "retry once")
        self.assertEqual(await broker.get_dead_letter_records(noop.queue_name), [])

        # RabbitMQ는 push 기반이라 requeue된 메시지가 이미 열려 있는 컨슈머에게
        # 전달된다 (pull 기반인 Redis Streams와 다른 지점).
        requeued_delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        self.assertEqual(requeued_delivery.message_id, first_delivery.message_id)
        await consumer.ack(requeued_delivery)
        await asyncio.wait_for(broker.join(noop.queue_name), timeout=4.0)

        await noop.send("second")
        second_delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.reject(second_delivery, requeue=False)
        records = await broker.get_dead_letter_records(noop.queue_name)
        self.assertEqual(len(records), 1)
        second_dead_letter_id = records[0].dead_letter_id

        purged = await broker.purge_dead_letter(noop.queue_name, second_dead_letter_id, note="drop permanently")
        self.assertIsNotNone(purged)
        assert purged is not None
        self.assertEqual(purged.resolution_state, "purged")
        self.assertEqual(purged.resolution_note, "drop permanently")
        self.assertEqual(await broker.get_dead_letter_records(noop.queue_name), [])

        fetched = await broker.get_dead_letter_record(noop.queue_name, second_dead_letter_id)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.resolution_state, "purged")

        await consumer.close()

    async def test_closing_consumer_preserves_pending_delivery_for_redelivery(self) -> None:
        broker = self.make_broker()

        async def noop() -> None:
            return None

        actor = fluxera.actor(
            broker=broker,
            actor_name="preserve_pending",
            queue_name="default",
        )(noop)

        await actor.send()
        consumer1 = await broker.open_consumer(actor.queue_name)
        deliveries = await consumer1.receive(limit=1, timeout=2.0)
        self.assertEqual(len(deliveries), 1)
        await consumer1.close(forget=True)

        consumer2 = await broker.open_consumer(actor.queue_name)
        redeliveries = await consumer2.receive(limit=1, timeout=2.0)

        self.assertEqual(len(redeliveries), 1)
        self.assertTrue(redeliveries[0].redelivered)
        self.assertEqual(redeliveries[0].message.message_id, deliveries[0].message.message_id)

        await consumer2.ack(redeliveries[0])
        await asyncio.wait_for(broker.join(actor.queue_name), timeout=4.0)
        await consumer2.close()

    async def test_decode_error_moves_delivery_to_integrity_dead_letter(self) -> None:
        broker = self.make_broker()
        broker.declare_queue("default")
        await broker._ensure_topology("default")

        connection = await aio_pika.connect(self.amqp_url)
        try:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=b"this is not a fluxera message"),
                routing_key=broker._queue_name("default"),
            )
        finally:
            await connection.close()

        consumer = await broker.open_consumer("default")
        deliveries = await consumer.receive(limit=1, timeout=2.0)
        self.assertEqual(deliveries, [])

        records = await broker.get_dead_letter_records("default")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].failure_kind, "integrity_decode_error")
        self.assertTrue(records[0].payload_available)
        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)
        await consumer.close()

    async def test_flush_purges_queue_delayed_and_dead_letters(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        await remember.send("keep")
        await remember.send_with_options(args=("later",), delay=60.0)

        consumer = await broker.open_consumer(remember.queue_name)
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.reject(delivery, requeue=False)
        await consumer.close()

        await remember.send("queued-again")
        await broker.flush(remember.queue_name)

        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)
        self.assertEqual(await self._passive_message_count(broker._delayed_queue_name("default")), 0)
        self.assertEqual(await broker.get_dead_letter_records(remember.queue_name), [])
        await asyncio.wait_for(broker.join(remember.queue_name), timeout=4.0)

    async def test_flush_and_join_raise_for_undeclared_queue(self) -> None:
        broker = self.make_broker()

        with self.assertRaises(QueueNotFound):
            await broker.flush("missing")

        with self.assertRaises(QueueNotFound):
            await broker.join("missing")

    async def test_join_waits_for_in_flight_delivery(self) -> None:
        broker = self.make_broker()
        started = asyncio.Event()
        release = asyncio.Event()

        @fluxera.actor(broker=broker, queue_name="default")
        async def blocker() -> None:
            started.set()
            await release.wait()

        worker = fluxera.Worker(broker, concurrency=1, process_concurrency=0)
        await worker.start()
        try:
            await blocker.send()
            await started.wait()

            join_task = asyncio.create_task(broker.join(blocker.queue_name))
            await asyncio.sleep(0.8)
            self.assertFalse(join_task.done())

            release.set()
            await asyncio.wait_for(join_task, timeout=10.0)
        finally:
            release.set()
            await worker.stop()

    async def test_worker_processes_messages_end_to_end(self) -> None:
        broker = self.make_broker()
        seen: list[str] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            seen.append(value)

        async with fluxera.Worker(broker, concurrency=8):
            await remember.send("alpha")
            await asyncio.wait_for(broker.join(remember.queue_name), timeout=6.0)

        self.assertEqual(seen, ["alpha"])

    async def test_worker_retries_then_succeeds(self) -> None:
        broker = self.make_broker()
        attempts = 0

        @fluxera.actor(
            broker=broker,
            actor_name="flaky",
            queue_name="default",
            max_retries=1,
            min_backoff=0,
        )
        async def flaky() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("retry once")

        message = await flaky.send()
        worker = fluxera.Worker(broker, concurrency=1, process_concurrency=0)
        await worker.start()
        try:
            await asyncio.wait_for(broker.join(flaky.queue_name), timeout=8.0)
        finally:
            await worker.stop()

        self.assertEqual(attempts, 2)
        self.assertEqual(len(worker.record_history[message.message_id]), 2)

    async def test_worker_exhausted_retries_dead_letter_the_message(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(
            broker=broker,
            actor_name="always_fails",
            queue_name="default",
            max_retries=1,
            min_backoff=0,
        )
        async def always_fails() -> None:
            raise RuntimeError("permanent failure")

        message = await always_fails.send()
        worker = fluxera.Worker(broker, concurrency=1, process_concurrency=0)
        await worker.start()
        try:
            await asyncio.wait_for(broker.join(always_fails.queue_name), timeout=8.0)
        finally:
            await worker.stop()

        records = await broker.get_dead_letter_records(always_fails.queue_name)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].failure_kind, "exception")
        self.assertEqual(records[0].message_id, message.message_id)
        self.assertEqual(records[0].attempt, 1)
        self.assertEqual(records[0].exception_type, "RuntimeError")

    async def test_process_actor_runs_with_rabbitmq_broker(self) -> None:
        broker = self.make_broker()
        counter_path = os.path.join(tempfile.gettempdir(), f"{self.namespace}-process-counter")
        self.addCleanup(lambda: os.path.exists(counter_path) and os.unlink(counter_path))
        actor = fluxera.actor(
            broker=broker,
            actor_name="append_marker",
            queue_name="default",
            execution="process",
        )(append_marker)

        worker = fluxera.Worker(
            broker,
            concurrency=1,
            async_concurrency=0,
            thread_concurrency=0,
            process_concurrency=1,
        )
        await worker.start()
        try:
            await actor.send(counter_path)
            await asyncio.wait_for(broker.join(actor.queue_name), timeout=8.0)
        finally:
            await worker.stop()

        with open(counter_path, "r", encoding="utf-8") as counter_file:
            self.assertEqual(counter_file.read(), "x")

    async def test_at_least_once_recovers_after_worker_process_crash(self) -> None:
        namespace = f"{self.namespace}-crash"
        counter_path = os.path.join(tempfile.gettempdir(), f"{namespace}-counter")
        crash_flag_path = os.path.join(tempfile.gettempdir(), f"{namespace}-crash-flag")
        with open(crash_flag_path, "w", encoding="utf-8") as crash_file:
            crash_file.write("1")
        self.addCleanup(lambda: os.path.exists(counter_path) and os.unlink(counter_path))
        self.addCleanup(lambda: os.path.exists(crash_flag_path) and os.unlink(crash_flag_path))

        ctx = mp.get_context("spawn")
        first_worker = ctx.Process(
            target=_worker_process_entry,
            args=(self.amqp_url, namespace, True, counter_path, crash_flag_path),
            name="fluxera-rabbitmq-crash-worker-1",
        )
        first_worker.start()
        await asyncio.to_thread(first_worker.join, 15.0)

        self.assertFalse(first_worker.is_alive())
        self.assertNotEqual(first_worker.exitcode, 0)

        second_worker = ctx.Process(
            target=_worker_process_entry,
            args=(self.amqp_url, namespace, False, counter_path, crash_flag_path),
            name="fluxera-rabbitmq-crash-worker-2",
        )
        second_worker.start()
        await asyncio.to_thread(second_worker.join, 15.0)

        self.assertFalse(second_worker.is_alive())
        self.assertEqual(second_worker.exitcode, 0)

        with open(counter_path, "r", encoding="utf-8") as counter_file:
            self.assertEqual(counter_file.read(), "xx")

        for queue_name in await asyncio.to_thread(self.management.list_queue_names, namespace):
            await asyncio.to_thread(self.management.delete_queue, queue_name)

    async def test_send_sync_can_reuse_broker_across_threads(self) -> None:
        broker = self.make_broker()
        errors: list[BaseException] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        def send_once(index: int) -> None:
            try:
                remember.send_sync(f"value-{index}")
            except BaseException as exc:
                errors.append(exc)

        for index in range(5):
            thread = threading.Thread(target=send_once, args=(index,))
            thread.start()
            await asyncio.to_thread(thread.join)

        self.assertEqual(errors, [])
        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 5)

    async def test_concurrent_sends_share_one_channel_without_loss(self) -> None:
        # 동시 send는 하나의 발행 채널에서 confirm 대기가 겹치며(자동 배칭),
        # confirm/delivery_tag가 꼬이지 않고 전부 유실 없이 적재되어야 한다.
        # 배칭의 처리량 이득 수치는 asyncio debug 모드가 없는
        # benchmarks/broker_transport_compare.py의 enqueue-concurrent 시나리오로 잰다.
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def sink(index: int) -> None:
            del index

        total = 1000
        for offset in range(0, total, 200):
            await asyncio.gather(*(sink.send(offset + j) for j in range(200)))

        self.assertEqual(
            await self._passive_message_count(broker._queue_name("default")),
            total,
        )

    async def test_publisher_confirms_disabled_still_delivers(self) -> None:
        # confirm을 끄면 fire-and-forget으로 발행된다 (더 빠르지만 브로커 crash 시
        # 미확정 발행 유실 가능). 정상 경로에서는 전부 전달되어야 한다.
        broker = self.make_broker(publisher_confirms=False)
        seen: list[str] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            seen.append(value)

        async with fluxera.Worker(broker, concurrency=8, process_concurrency=0):
            for index in range(20):
                await remember.send(f"value-{index}")
            await asyncio.wait_for(broker.join(remember.queue_name), timeout=10.0)

        self.assertEqual(len(seen), 20)

    async def test_deduplication_options_are_rejected(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        with self.assertRaises(ValueError):
            await remember.send_with_options(args=("alpha",), job_id="job-1")

        with self.assertRaises(ValueError):
            await remember.send_with_options(
                args=("alpha",),
                deduplication={"id": "search-refresh", "mode": "simple"},
            )

        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)

    async def test_queue_runtime_row_reports_counts(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        await remember.send("ready-1")
        await remember.send("ready-2")
        await remember.send_with_options(args=("later",), delay=60.0)

        consumer = await broker.open_consumer(remember.queue_name)
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]

        async def row_reflects_state() -> bool:
            row = await broker.get_queue_runtime_row("default")
            return (
                row["stream_length"] == 1
                and row["delayed_count"] == 1
                and row["pending_count"] == 1
            )

        await wait_for_async(row_reflects_state, timeout=6.0)

        row = await broker.get_queue_runtime_row("default")
        self.assertEqual(row["queue_name"], "default")
        self.assertIsNone(row["serving_revision"])
        self.assertEqual(row["pending_stale_count"], 0)
        self.assertEqual(row["worker_ids"], [])

        rows = await broker.get_queue_runtime_rows(["default"])
        self.assertEqual(rows[0]["stream_length"], row["stream_length"])

        await consumer.ack(delivery)
        second = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.ack(second)
        await consumer.close()

    async def test_list_runtime_queues_discovers_namespace_queues(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="alpha")
        async def one() -> None:
            return None

        @fluxera.actor(broker=broker, queue_name="beta")
        async def two() -> None:
            return None

        await one.send()
        await two.send()

        queue_names = await broker.list_runtime_queues()
        self.assertIn("alpha", queue_names)
        self.assertIn("beta", queue_names)

    async def test_close_releases_all_connections(self) -> None:
        broker = self.make_broker(client_name=f"close-check-{self.namespace}")

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        await remember.send("alpha")
        consumer = await broker.open_consumer(remember.queue_name)
        deliveries = await consumer.receive(limit=1, timeout=2.0)
        await consumer.ack(deliveries[0])

        def sync_send() -> None:
            remember.send_sync("beta")

        await asyncio.to_thread(sync_send)

        # management의 커넥션 목록 반영은 수 초 지연되므로, 먼저 커넥션이 실제로
        # 보이는 것을 확인해 이 테스트가 공허하게 통과하지 않도록 한다
        async def broker_connections_visible() -> bool:
            names = await asyncio.to_thread(self.management.connection_client_names)
            return any(name.startswith(f"close-check-{self.namespace}") for name in names)

        await wait_for_async(broker_connections_visible, timeout=15.0, interval=0.5)

        await broker.close()
        self.brokers.remove(broker)

        async def no_broker_connections() -> bool:
            names = await asyncio.to_thread(self.management.connection_client_names)
            return not any(name.startswith(f"close-check-{self.namespace}") for name in names)

        await wait_for_async(no_broker_connections, timeout=15.0, interval=0.5)

    async def test_ack_failure_releases_local_unacked_counter(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def noop() -> None:
            return None

        await noop.send()
        consumer = await broker.open_consumer(noop.queue_name)
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        self.assertEqual(consumer.unacked_count, 1)

        # 채널이 죽어 ack가 실패하는 상황을 시뮬레이션한다. ack 실패는 소유권
        # 상실이므로 (Redis의 no-op XACK처럼) 예외 없이 삼켜지고, 로컬 카운터는
        # 반드시 해제되어 join()이 영구 대기하지 않아야 한다.
        class _FailingAck:
            async def ack(self) -> None:
                raise aio_pika.exceptions.ChannelInvalidStateError("channel is closed")

        delivery.metadata["amqp_message"] = _FailingAck()
        await consumer.ack(delivery)
        self.assertEqual(consumer.unacked_count, 0)

        # 실제 채널은 살아 있으므로 닫아서 서버가 requeue하게 한 뒤 정리한다
        await consumer.close()
        rescuer = await broker.open_consumer(noop.queue_name)
        redelivered = (await rescuer.receive(limit=1, timeout=2.0))[0]
        self.assertTrue(redelivered.redelivered)
        await rescuer.ack(redelivered)
        await rescuer.close()
        await asyncio.wait_for(broker.join(noop.queue_name), timeout=6.0)

    async def test_degenerate_deduplication_options_are_allowed(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        # RedisBroker가 no-dedup으로 취급하는 퇴화 옵션들은 그대로 통과해야 한다
        await remember.send_with_options(args=("a",), deduplication={"id": ""})
        await remember.send_with_options(args=("b",), deduplication="not-a-dict")
        await remember.send_with_options(args=("c",), job_id="")

        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 3)

    async def test_undecodable_dead_letter_payload_survives_scans(self) -> None:
        broker = self.make_broker()
        broker.declare_queue("default")
        await broker._ensure_topology("default")

        connection = await aio_pika.connect(self.amqp_url)
        try:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=b"not a dead letter record"),
                routing_key=broker._dead_letter_queue_name("default"),
            )
        finally:
            await connection.close()

        # 디코드 불가 페이로드는 (버전 스큐 가능성 때문에) 삭제 없이 보존되어야 한다
        self.assertEqual(await broker.get_dead_letter_records("default"), [])
        self.assertEqual(await broker.get_dead_letter_records("default"), [])
        self.assertEqual(
            await self._passive_message_count(broker._dead_letter_queue_name("default")),
            1,
        )

    async def test_dead_letter_scan_lock_serializes_concurrent_brokers(self) -> None:
        broker_a = self.make_broker(consumer_name_prefix="scan-a")
        broker_b = self.make_broker(consumer_name_prefix="scan-b")

        @fluxera.actor(broker=broker_a, actor_name="scan_target", queue_name="default")
        async def noop() -> None:
            return None

        await noop.send()
        consumer = await broker_a.open_consumer("default")
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.reject(delivery, requeue=False)
        await consumer.close()

        records = await broker_a.get_dead_letter_records("default")
        self.assertEqual(len(records), 1)
        dead_letter_id = records[0].dead_letter_id
        broker_b.declare_queue("default")

        # 동시 스캔: 락이 없으면 B가 A의 스캔 중 빈 DLQ를 보고 None을 반환할 수 있다
        list_results, purged = await asyncio.gather(
            broker_a.get_dead_letter_records("default"),
            broker_b.purge_dead_letter("default", dead_letter_id, note="concurrent"),
        )
        self.assertEqual(len(list_results), 1)
        self.assertIsNotNone(purged)
        assert purged is not None
        self.assertEqual(purged.resolution_state, "purged")

    async def test_requeue_dead_letter_registers_queue_on_fresh_broker(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, actor_name="fresh_requeue", queue_name="default")
        async def noop() -> None:
            return None

        await noop.send()
        consumer = await broker.open_consumer("default")
        delivery = (await consumer.receive(limit=1, timeout=2.0))[0]
        await consumer.reject(delivery, requeue=False)
        await consumer.close()
        records = await broker.get_dead_letter_records("default")
        self.assertEqual(len(records), 1)

        # 큐를 선언한 적 없는 새 브로커 인스턴스로 requeue → 큐가 등록되어
        # 이후 flush/join이 QueueNotFound를 던지지 않아야 한다
        fresh = self.make_broker(consumer_name_prefix="fresh")
        requeued = await fresh.requeue_dead_letter("default", records[0].dead_letter_id)
        self.assertIsNotNone(requeued)
        self.assertIn("default", fresh.queues)
        await fresh.flush("default")

    async def test_closed_consumer_wakes_all_blocked_receives(self) -> None:
        broker = self.make_broker()
        broker.declare_queue("default")

        consumer = await broker.open_consumer("default")
        await consumer.receive(limit=1, timeout=0.05)

        async def blocked_receive():
            with self.assertRaises(ConnectionError):
                await consumer.receive(limit=1, timeout=None)

        waiters = [asyncio.create_task(blocked_receive()) for _ in range(2)]
        await asyncio.sleep(0.1)
        await consumer.close()
        await asyncio.wait_for(asyncio.gather(*waiters), timeout=2.0)

    async def test_consumer_timeout_argument_is_applied_to_main_queue(self) -> None:
        # RabbitMQ 기본 consumer timeout은 30분이다. 장시간 태스크(예: 5분 타임아웃
        # API 6개 순차 호출)는 이 경계를 넘을 수 있으므로 큐 단위로 상향할 수 있어야 한다.
        broker = self.make_broker(consumer_timeout_seconds=3600.0)
        broker.declare_queue("default")
        await broker._ensure_topology("default")

        info = await asyncio.to_thread(self.management.queue_info, broker._queue_name("default"))
        self.assertEqual(info.get("arguments", {}).get("x-consumer-timeout"), 3_600_000)

    async def test_server_forced_connection_close_redelivers_in_flight_message(self) -> None:
        # 서버가 consume 커넥션을 강제로 끊는 상황(브로커 재시작, LB 유휴 킬,
        # consumer timeout 강제와 같은 계열)에서 in-flight 메시지가 유실되지 않고
        # 재전달되어야 한다. 원본 실행의 ack 실패는 삼켜지고 워커는 계속 동작해야 한다.
        client_name = f"forced-close-{self.namespace}"
        broker = self.make_broker(client_name=client_name, reconnect_interval_seconds=0.5)
        executions: list[int] = []
        started = asyncio.Event()
        release = asyncio.Event()

        @fluxera.actor(broker=broker, queue_name="default")
        async def long_task() -> None:
            executions.append(len(executions))
            started.set()
            await release.wait()

        worker = fluxera.Worker(broker, concurrency=2, process_concurrency=0, poll_timeout=0.05)
        await worker.start()
        try:
            await long_task.send()
            await asyncio.wait_for(started.wait(), timeout=5.0)

            # management API의 커넥션 목록 반영에는 약간의 지연이 있어 재시도한다
            async def force_close() -> bool:
                closed = await asyncio.to_thread(
                    self.management.close_connections_matching,
                    f"{client_name}:consume",
                )
                return closed >= 1

            await wait_for_async(force_close, timeout=10.0, interval=0.25)

            # 재전달된 사본이 (원본이 아직 블록된 채로) 다시 실행되기 시작해야 한다
            await wait_for(lambda: len(executions) >= 2, timeout=15.0)

            release.set()
            await asyncio.wait_for(broker.join(long_task.queue_name), timeout=15.0)
        finally:
            release.set()
            await worker.stop()

        self.assertEqual(len(executions), 2)

    async def test_worker_shutdown_mid_task_requeues_for_next_worker(self) -> None:
        # 실행 중 워커가 셧다운되면 기본 on_cancel="requeue" 정책으로 메시지가
        # 다시 큐에 들어가고, 다음 워커가 이어서 처리해야 한다 (at-least-once).
        broker1 = self.make_broker(consumer_name_prefix="stop-a")
        broker2 = self.make_broker(consumer_name_prefix="stop-b")
        attempts = 0
        first_started = asyncio.Event()
        done = asyncio.Event()

        async def handler() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                first_started.set()
                await asyncio.Event().wait()
            else:
                done.set()

        actor1 = fluxera.actor(
            broker=broker1,
            actor_name="shutdown_requeue",
            queue_name="default",
        )(handler)
        fluxera.actor(
            broker=broker2,
            actor_name="shutdown_requeue",
            queue_name="default",
        )(handler)

        worker1 = fluxera.Worker(broker1, concurrency=1, process_concurrency=0)
        await worker1.start()
        await actor1.send()
        await asyncio.wait_for(first_started.wait(), timeout=5.0)
        await worker1.stop(timeout=0.1)

        worker2 = fluxera.Worker(broker2, concurrency=1, process_concurrency=0)
        await worker2.start()
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
            await asyncio.wait_for(broker2.join("default"), timeout=10.0)
        finally:
            await worker2.stop()

        self.assertEqual(attempts, 2)

    async def test_task_spanning_many_heartbeat_intervals_completes_exactly_once(self) -> None:
        # 1시간짜리 태스크의 스케일 축소 불변식: 태스크가 heartbeat 윈도우
        # (stuck 임계값 = (heartbeat+1)*3 = 6s)를 여러 번 넘겨도, 이벤트 루프가
        # 살아 있는 한 heartbeat가 계속 흘러 커넥션이 유지되고 정확히 1회만
        # 실행되어야 한다. 실제 1시간 태스크는 이 불변식의 시간 축 확장이다.
        broker = self.make_broker(heartbeat_seconds=1.0)
        executions: list[int] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def very_long() -> None:
            executions.append(len(executions))
            await asyncio.sleep(8.0)

        worker = fluxera.Worker(broker, concurrency=2, process_concurrency=0)
        await worker.start()
        try:
            await very_long.send()
            await asyncio.wait_for(broker.join(very_long.queue_name), timeout=30.0)
        finally:
            await worker.stop()

        self.assertEqual(len(executions), 1)
        self.assertEqual(await broker.get_dead_letter_records("default"), [])
        self.assertEqual(await self._passive_message_count(broker._queue_name("default")), 0)

    async def test_long_running_async_task_is_not_spuriously_redelivered(self) -> None:
        # 커넥션이 건강한 동안에는 장시간 태스크(내부 배칭·robust 재연결 로직이
        # 개입할 시간이 충분한 길이)가 정확히 한 번만 실행되어야 한다.
        broker = self.make_broker()
        executions: list[int] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def slow() -> None:
            executions.append(len(executions))
            await asyncio.sleep(4.0)

        worker = fluxera.Worker(broker, concurrency=4, process_concurrency=0)
        await worker.start()
        try:
            await slow.send()
            await asyncio.wait_for(broker.join(slow.queue_name), timeout=20.0)
        finally:
            await worker.stop()

        self.assertEqual(len(executions), 1)
        self.assertEqual(await broker.get_dead_letter_records("default"), [])

    async def test_worker_transient_connection_error_recovers(self) -> None:
        broker = self.make_broker()
        seen: list[str] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            seen.append(value)

        worker = fluxera.Worker(broker, concurrency=2, process_concurrency=0, poll_timeout=0.05)
        await worker.start()
        try:
            await remember.send("before")
            await wait_for(lambda: "before" in seen)

            consumer = worker.consumers[remember.queue_name]
            await consumer.close(forget=True)

            await remember.send("after")
            await wait_for(lambda: "after" in seen, timeout=8.0)
        finally:
            await worker.stop()

        self.assertEqual(sorted(seen), ["after", "before"])
