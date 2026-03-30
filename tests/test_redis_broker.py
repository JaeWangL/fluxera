from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import subprocess
import sys
import threading
import unittest
from uuid import uuid4

import orjson
import redis as sync_redis
import redis.asyncio as redis_async

import fluxera


async def wait_for(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Condition was not met before timeout.")
        await asyncio.sleep(interval)


async def wait_for_async(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Async condition was not met before timeout.")
        await asyncio.sleep(interval)


def crash_after_side_effect(redis_url: str, counter_key: str, crash_key: str) -> None:
    client = sync_redis.Redis.from_url(redis_url)
    try:
        client.incr(counter_key)
        if client.get(crash_key):
            client.delete(crash_key)
            os._exit(17)
    finally:
        client.close()


def increment_counter(redis_url: str, counter_key: str) -> None:
    client = sync_redis.Redis.from_url(redis_url)
    try:
        client.incr(counter_key)
    finally:
        client.close()


async def _pending_count_at_least(broker: fluxera.RedisBroker, queue_name: str, minimum: int) -> bool:
    pending = await broker.client.xpending(broker._stream_key(queue_name), broker.group_name)
    return int(pending["pending"]) >= minimum


async def _run_worker_once(redis_url: str, namespace: str, *, send_initial: bool) -> None:
    broker = fluxera.RedisBroker(redis_url, namespace=namespace, lease_seconds=0.1, consumer_name_prefix="crash-test")
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
            counter_key = f"{namespace}:counter"
            crash_key = f"{namespace}:should-crash"
            await actor.send(redis_url, counter_key, crash_key)

        await broker.join(actor.queue_name)
    finally:
        await worker.stop()


def _worker_process_entry(redis_url: str, namespace: str, send_initial: bool) -> None:
    asyncio.run(_run_worker_once(redis_url, namespace, send_initial=send_initial))


class RedisBrokerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis_url = os.environ.get("FLUXERA_REDIS_URL", "redis://127.0.0.1:6379/15")
        self.namespace = f"fluxera-test-{uuid4().hex}"
        self.redis = redis_async.from_url(self.redis_url, decode_responses=False)
        self.brokers: list[fluxera.RedisBroker] = []

        try:
            await self.redis.ping()
        except Exception as exc:
            await self.redis.aclose()
            self.skipTest(f"Redis is not available for integration tests: {exc}")

    async def asyncTearDown(self) -> None:
        for broker in self.brokers:
            await broker.close()

        cursor = 0
        keys: list[bytes] = []
        pattern = f"{self.namespace}:*"
        while True:
            cursor, batch = await self.redis.scan(cursor=cursor, match=pattern, count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        if keys:
            await self.redis.delete(*keys)

        await self.redis.aclose()

    def make_broker(self, **overrides) -> fluxera.RedisBroker:
        params = {
            "namespace": self.namespace,
            "lease_seconds": 0.2,
        }
        params.update(overrides)
        broker = fluxera.RedisBroker(
            self.redis_url,
            **params,
        )
        self.brokers.append(broker)
        return broker

    async def run_fluxera_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        repo_root = os.path.dirname(os.path.dirname(__file__))
        env = os.environ.copy()
        env.setdefault("PYTHONPATH", repo_root)
        return await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "fluxera", *args],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    async def test_worker_processes_messages_from_redis_streams(self) -> None:
        broker = self.make_broker()
        seen: list[str] = []

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            seen.append(value)

        async with fluxera.Worker(broker, concurrency=8):
            await remember.send("alpha")
            await broker.join(remember.queue_name)

        self.assertEqual(seen, ["alpha"])
        self.assertEqual(await broker.client.xlen(broker._stream_key(remember.queue_name)), 0)

    async def test_send_sync_can_reuse_global_redis_broker_across_threads(self) -> None:
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
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(await broker.client.xlen(broker._stream_key(remember.queue_name)), 5)

    async def test_v2_key_layout_uses_message_registry_for_streams_and_delayed(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        immediate = remember.message("alpha")
        delayed = remember.message("beta")
        await broker.send(immediate)
        await broker.send(delayed, delay=60.0)

        stream_entries = await self.redis.xrange(broker._stream_key(remember.queue_name))
        self.assertEqual(len(stream_entries), 1)
        self.assertNotIn(b"payload", stream_entries[0][1])
        self.assertEqual(stream_entries[0][1][b"message_id"].decode("utf-8"), immediate.message_id)

        delayed_members = await self.redis.zrange(broker._delayed_key(remember.queue_name), 0, -1)
        self.assertEqual(delayed_members, [delayed.message_id.encode("utf-8")])

        immediate_payload = await self.redis.get(broker._message_key(immediate.message_id))
        delayed_payload = await self.redis.get(broker._message_key(delayed.message_id))
        self.assertEqual(broker._decode_message(immediate_payload), immediate)
        self.assertEqual(broker._decode_message(delayed_payload), delayed)

    async def test_simple_dedupe_releases_key_on_terminal_ack(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        first = await remember.send_with_options(args=("alpha",), job_id="job-1")
        await remember.send_with_options(args=("beta",), job_id="job-1")
        stream_entries = await self.redis.xrange(broker._stream_key(remember.queue_name))
        self.assertEqual(len(stream_entries), 1)

        dedupe_key = broker._dedupe_key(remember.queue_name, remember.actor_name, "job-1")
        self.assertEqual(await self.redis.get(dedupe_key), first.message_id.encode("utf-8"))

        consumer = await broker.open_consumer(remember.queue_name)
        delivery = (await consumer.receive(limit=1, timeout=1.0))[0]
        self.assertEqual(delivery.message.message_id, first.message_id)
        await consumer.ack(delivery)
        await broker.join(remember.queue_name)

        self.assertIsNone(await self.redis.get(dedupe_key))
        await consumer.close()

    async def test_debounce_replaces_delayed_message_payload(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, queue_name="default")
        async def remember(value: str) -> None:
            del value

        first = await remember.send_with_options(
            args=("alpha",),
            delay=60.0,
            deduplication={"id": "search-refresh", "mode": "debounce", "replace": True},
        )
        second = await remember.send_with_options(
            args=("beta",),
            delay=60.0,
            deduplication={"id": "search-refresh", "mode": "debounce", "replace": True},
        )

        delayed_members = await self.redis.zrange(broker._delayed_key(remember.queue_name), 0, -1)
        self.assertEqual(delayed_members, [second.message_id.encode("utf-8")])
        self.assertIsNone(await self.redis.get(broker._message_key(first.message_id)))

        payload = await self.redis.get(broker._message_key(second.message_id))
        self.assertEqual(broker._decode_message(payload), second)

        dedupe_key = broker._dedupe_key(remember.queue_name, remember.actor_name, "search-refresh")
        self.assertEqual(await self.redis.get(dedupe_key), second.message_id.encode("utf-8"))

    async def test_idempotency_wrappers_round_trip_running_busy_and_completed_states(self) -> None:
        broker = self.make_broker()

        acquired = await broker.idempotency_begin(
            actor_name="remember",
            idempotency_key="idem-1",
            owner="worker-a",
            message_id="message-a",
            attempt=1,
            lease_ms=200,
        )
        self.assertEqual(acquired.status, "acquired")
        self.assertEqual(acquired.fence_token, 1)

        busy = await broker.idempotency_begin(
            actor_name="remember",
            idempotency_key="idem-1",
            owner="worker-b",
            message_id="message-b",
            attempt=1,
            lease_ms=200,
        )
        self.assertEqual(busy.status, "busy")
        self.assertEqual(busy.owner, "worker-a")

        heartbeat = await broker.idempotency_heartbeat(
            actor_name="remember",
            idempotency_key="idem-1",
            owner="worker-a",
            fence_token=acquired.fence_token,
            lease_ms=200,
        )
        self.assertEqual(heartbeat.status, "ok")

        committed = await broker.idempotency_commit(
            actor_name="remember",
            idempotency_key="idem-1",
            owner="worker-a",
            fence_token=acquired.fence_token,
            result_ttl_ms=1_000,
            result_ref="result://remember/idem-1",
            result_digest="sha256:demo",
        )
        self.assertEqual(committed.status, "ok")

        completed = await broker.idempotency_begin(
            actor_name="remember",
            idempotency_key="idem-1",
            owner="worker-c",
            message_id="message-c",
            attempt=1,
            lease_ms=200,
        )
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.result_ref, "result://remember/idem-1")
        self.assertEqual(completed.result_digest, "sha256:demo")

    async def test_process_actor_runs_with_async_redis_broker(self) -> None:
        broker = self.make_broker()
        counter_key = f"{self.namespace}:process-counter"
        actor = fluxera.actor(
            broker=broker,
            actor_name="increment_counter",
            queue_name="default",
            execution="process",
        )(increment_counter)

        worker = fluxera.Worker(
            broker,
            concurrency=1,
            async_concurrency=0,
            thread_concurrency=0,
            process_concurrency=1,
        )
        await worker.start()
        try:
            await actor.send(self.redis_url, counter_key)
            await broker.join(actor.queue_name)
        finally:
            await worker.stop()

        self.assertEqual(int(await self.redis.get(counter_key)), 1)

    async def test_serving_revision_control_plane_bootstraps_and_promotes(self) -> None:
        broker = self.make_broker()

        serving = await broker.ensure_serving_revision("default", "rev-a")
        self.assertEqual(serving, "rev-a")
        self.assertEqual(await broker.get_serving_revision("default"), "rev-a")

        serving = await broker.ensure_serving_revision("default", "rev-b")
        self.assertEqual(serving, "rev-a")

        promoted = await broker.promote_serving_revision("default", "rev-b", expected_revision="rev-a")
        self.assertTrue(promoted)
        self.assertEqual(await broker.get_serving_revision("default"), "rev-b")

        worker_id = "worker-a"
        await broker.register_worker_revision(
            worker_id=worker_id,
            worker_revision="rev-b",
            queue_states={"default": "accepting"},
        )
        worker_fields = await self.redis.hgetall(broker._worker_key(worker_id))
        self.assertEqual(worker_fields[b"worker_revision"], b"rev-b")
        self.assertEqual(worker_fields[b"accepting_queues"], b"default")

        registered_workers = await self.redis.zrange(broker._workers_key("default"), 0, -1)
        self.assertEqual(registered_workers, [worker_id.encode("utf-8")])

        await broker.unregister_worker_revision(worker_id=worker_id, queue_names={"default"})
        self.assertEqual(await self.redis.exists(broker._worker_key(worker_id)), 0)

    async def test_revision_cli_get_and_promote(self) -> None:
        broker = self.make_broker()
        await broker.ensure_serving_revision("default", "rev-a")

        get_result = await self.run_fluxera_cli(
            "revision",
            "get",
            "--redis-url",
            self.redis_url,
            "--namespace",
            self.namespace,
            "--queue",
            "default",
            "--format",
            "json",
        )
        self.assertEqual(get_result.returncode, 0, get_result.stderr)
        get_payload = orjson.loads(get_result.stdout)
        self.assertEqual(get_payload["serving_revision"], "rev-a")
        self.assertTrue(get_payload["exists"])

        promote_result = await self.run_fluxera_cli(
            "revision",
            "promote",
            "--redis-url",
            self.redis_url,
            "--namespace",
            self.namespace,
            "--queue",
            "default",
            "--revision",
            "rev-b",
            "--expected-revision",
            "rev-a",
            "--format",
            "json",
        )
        self.assertEqual(promote_result.returncode, 0, promote_result.stderr)
        promote_payload = orjson.loads(promote_result.stdout)
        self.assertTrue(promote_payload["updated"])
        self.assertEqual(promote_payload["serving_revision"], "rev-b")
        self.assertEqual(await broker.get_serving_revision("default"), "rev-b")

        failed_promote = await self.run_fluxera_cli(
            "revision",
            "promote",
            "--redis-url",
            self.redis_url,
            "--namespace",
            self.namespace,
            "--queue",
            "default",
            "--revision",
            "rev-c",
            "--expected-revision",
            "rev-a",
            "--format",
            "json",
        )
        self.assertEqual(failed_promote.returncode, 2, failed_promote.stderr)
        failed_payload = orjson.loads(failed_promote.stdout)
        self.assertFalse(failed_payload["updated"])
        self.assertEqual(failed_payload["serving_revision"], "rev-b")

    async def test_revision_promotion_drains_unstarted_backlog_to_new_worker(self) -> None:
        broker_old = self.make_broker(consumer_name_prefix="old")
        broker_new = self.make_broker(consumer_name_prefix="new")
        started = asyncio.Event()
        release = asyncio.Event()
        old_seen: list[str] = []
        new_seen: list[str] = []

        async def old_handler(value: str) -> None:
            if value == "block":
                started.set()
                await release.wait()
            old_seen.append(value)

        async def new_handler(value: str) -> None:
            new_seen.append(value)

        old_actor = fluxera.actor(
            broker=broker_old,
            actor_name="handoff",
            queue_name="default",
        )(old_handler)
        fluxera.actor(
            broker=broker_new,
            actor_name="handoff",
            queue_name="default",
        )(new_handler)

        worker_old = fluxera.Worker(
            broker_old,
            async_concurrency=1,
            thread_concurrency=0,
            process_concurrency=0,
            max_in_flight=2,
            prefetch=2,
            worker_revision="rev-old",
            revision_poll_interval=0.05,
        )
        worker_new = fluxera.Worker(
            broker_new,
            async_concurrency=1,
            thread_concurrency=0,
            process_concurrency=0,
            max_in_flight=2,
            prefetch=2,
            worker_revision="rev-new",
            revision_poll_interval=0.05,
        )

        await worker_old.start()
        await worker_new.start()
        try:
            await wait_for(lambda: worker_old.queue_states.get("default") == "accepting")
            await wait_for(lambda: worker_new.queue_states.get("default") == "draining")

            await old_actor.send("block")
            await started.wait()
            await old_actor.send("queued")
            await wait_for_async(
                lambda: _pending_count_at_least(broker_old, "default", 2),
            )

            promoted = await broker_new.promote_serving_revision("default", "rev-new", expected_revision="rev-old")
            self.assertTrue(promoted)

            await wait_for(lambda: worker_old.queue_states.get("default") == "draining")
            await wait_for(lambda: worker_new.queue_states.get("default") == "accepting")

            release.set()
            await wait_for(lambda: "queued" in new_seen)
            await broker_new.join(old_actor.queue_name)
        finally:
            release.set()
            await worker_old.stop()
            await worker_new.stop()

        self.assertEqual(old_seen, ["block"])
        self.assertEqual(new_seen, ["queued"])

    async def test_stale_pending_delivery_is_reclaimed_by_another_consumer(self) -> None:
        broker = self.make_broker(lease_seconds=0.1)

        async def noop() -> None:
            return None

        actor = fluxera.actor(
            broker=broker,
            actor_name="noop",
            queue_name="default",
        )(noop)

        await actor.send()
        consumer1 = await broker.open_consumer(actor.queue_name)
        deliveries = await consumer1.receive(limit=1, timeout=1.0)
        self.assertEqual(len(deliveries), 1)
        self.assertFalse(deliveries[0].redelivered)

        broker2 = self.make_broker(lease_seconds=0.1, consumer_name_prefix="peer")
        consumer2 = await broker2.open_consumer(actor.queue_name)
        await asyncio.sleep(0.15)
        redeliveries = await consumer2.receive(limit=1, timeout=1.0)

        self.assertEqual(len(redeliveries), 1)
        self.assertTrue(redeliveries[0].redelivered)
        self.assertEqual(redeliveries[0].message.message_id, deliveries[0].message.message_id)

        await consumer2.ack(redeliveries[0])
        await broker.join(actor.queue_name)
        await consumer1.close()
        await consumer2.close()

    async def test_worker_renews_lease_for_long_running_task(self) -> None:
        broker1 = self.make_broker(lease_seconds=0.15, consumer_name_prefix="worker-a")
        broker2 = self.make_broker(lease_seconds=0.15, consumer_name_prefix="worker-b")
        started = asyncio.Event()
        release = asyncio.Event()
        attempts: list[int] = []

        async def long_running() -> None:
            attempts.append(1)
            started.set()
            await release.wait()

        actor1 = fluxera.actor(
            broker=broker1,
            actor_name="long_running",
            queue_name="default",
        )(long_running)
        fluxera.actor(
            broker=broker2,
            actor_name="long_running",
            queue_name="default",
        )(long_running)

        worker1 = fluxera.Worker(broker1, concurrency=1)
        worker2 = fluxera.Worker(broker2, concurrency=1)
        await worker1.start()
        await worker2.start()
        try:
            await actor1.send()
            await started.wait()
            await asyncio.sleep(0.4)
            self.assertEqual(len(attempts), 1)
            release.set()
            await broker1.join(actor1.queue_name)
        finally:
            release.set()
            await worker1.stop()
            await worker2.stop()

        self.assertEqual(len(attempts), 1)

    async def test_consumer_does_not_redeliver_its_own_stale_in_flight_message(self) -> None:
        broker = self.make_broker(lease_seconds=0.05)

        async def noop(value: str) -> str:
            return value

        actor = fluxera.actor(
            broker=broker,
            actor_name="no_duplicate",
            queue_name="default",
        )(noop)

        await actor.send("first")
        consumer = await broker.open_consumer(actor.queue_name)
        first_delivery = (await consumer.receive(limit=1, timeout=1.0))[0]

        await asyncio.sleep(0.08)
        await actor.send("second")
        next_delivery = (await consumer.receive(limit=1, timeout=1.0))[0]

        self.assertEqual(first_delivery.args, ("first",))
        self.assertEqual(next_delivery.args, ("second",))
        self.assertFalse(next_delivery.redelivered)

        await consumer.ack(next_delivery)
        await consumer.reject(first_delivery, requeue=False)
        await broker.join(actor.queue_name)
        await consumer.close()

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
        deliveries = await consumer.receive(limit=1, timeout=1.0)
        self.assertEqual(len(deliveries), 1)

        await consumer.reject(deliveries[0], requeue=False)
        await broker.join(actor.queue_name)
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
        first_delivery = (await consumer.receive(limit=1, timeout=1.0))[0]
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

        second_consumer = await broker.open_consumer(noop.queue_name)
        requeued_delivery = (await second_consumer.receive(limit=1, timeout=1.0))[0]
        self.assertEqual(requeued_delivery.message_id, first_delivery.message_id)
        await second_consumer.ack(requeued_delivery)
        await broker.join(noop.queue_name)

        await noop.send("second")
        second_delivery = (await consumer.receive(limit=1, timeout=1.0))[0]
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

        await second_consumer.close()
        await consumer.close()

    async def test_missing_message_payload_moves_delivery_to_integrity_dead_letter(self) -> None:
        broker = self.make_broker()

        @fluxera.actor(broker=broker, actor_name="integrity_test", queue_name="default")
        async def noop() -> None:
            return None

        message = await noop.send()
        await self.redis.delete(broker._message_key(message.message_id))

        consumer = await broker.open_consumer(noop.queue_name)
        deliveries = await consumer.receive(limit=1, timeout=1.0)
        self.assertEqual(deliveries, [])

        records = await broker.get_dead_letter_records(noop.queue_name)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].failure_kind, "integrity_missing_payload")
        self.assertEqual(records[0].message_id, message.message_id)
        self.assertFalse(records[0].payload_available)

        self.assertEqual(await self.redis.xlen(broker._stream_key(noop.queue_name)), 0)
        await consumer.close()

    async def test_at_least_once_recovers_after_worker_process_crash(self) -> None:
        namespace = f"{self.namespace}-crash"
        counter_key = f"{namespace}:counter"
        crash_key = f"{namespace}:should-crash"
        await self.redis.set(crash_key, b"1")

        ctx = mp.get_context("spawn")
        first_worker = ctx.Process(
            target=_worker_process_entry,
            args=(self.redis_url, namespace, True),
            name="fluxera-redis-crash-worker-1",
        )
        first_worker.start()
        first_worker.join(5.0)

        self.assertFalse(first_worker.is_alive())
        self.assertNotEqual(first_worker.exitcode, 0)

        await asyncio.sleep(0.15)

        second_worker = ctx.Process(
            target=_worker_process_entry,
            args=(self.redis_url, namespace, False),
            name="fluxera-redis-crash-worker-2",
        )
        second_worker.start()
        second_worker.join(5.0)

        self.assertFalse(second_worker.is_alive())
        self.assertEqual(second_worker.exitcode, 0)
        self.assertEqual(int(await self.redis.get(counter_key)), 2)

        broker = self.make_broker(namespace=namespace, lease_seconds=0.1)
        actor = fluxera.actor(
            broker=broker,
            actor_name="crash_after_side_effect",
            queue_name="default",
            execution="thread",
        )(crash_after_side_effect)
        await broker.join(actor.queue_name)
