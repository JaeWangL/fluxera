from __future__ import annotations

import asyncio
import os
import time
import unittest

import fluxera


def process_identity(delay: float) -> int:
    time.sleep(delay)
    return os.getpid()


def process_finished_at(delay: float) -> float:
    time.sleep(delay)
    return time.time()


def process_sleep(delay: float) -> None:
    time.sleep(delay)


async def wait_for_async(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("Timed out waiting for condition.")


class StubBrokerWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_worker_processes_many_messages_concurrently(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)

        active = 0
        max_active = 0
        lock = asyncio.Lock()

        @fluxera.actor
        async def sleeper(delay: float) -> None:
            nonlocal active, max_active

            async with lock:
                active += 1
                max_active = max(max_active, active)

            await asyncio.sleep(delay)

            async with lock:
                active -= 1

        async with fluxera.Worker(broker, concurrency=20):
            for _ in range(20):
                await sleeper.send(0.05)

            await broker.join(sleeper.queue_name)

        self.assertGreaterEqual(max_active, 10)

    async def test_sync_actor_uses_thread_offload_by_default(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)

        active = 0
        max_active = 0

        @fluxera.actor
        def blocking(delay: float) -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                time.sleep(delay)
            finally:
                active -= 1

        start = time.perf_counter()
        async with fluxera.Worker(broker, concurrency=4):
            for _ in range(4):
                await blocking.send(0.05)

            await broker.join(blocking.queue_name)

        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 0.15)
        self.assertGreaterEqual(max_active, 2)

    async def test_process_actor_runs_in_process_pool(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        identify = fluxera.actor(
            broker=broker,
            actor_name="identify",
            queue_name="cpu",
            execution="process",
        )(process_identity)

        worker = fluxera.Worker(broker, concurrency=2, process_concurrency=2)
        start = time.perf_counter()
        await worker.start()
        try:
            await identify.send(0.05)
            await identify.send(0.05)
            await broker.join(identify.queue_name)
        finally:
            await worker.stop()

        elapsed = time.perf_counter() - start
        pids = [record.result for record in worker.records.values() if record.delivery.actor_name == "identify"]
        self.assertEqual(len(pids), 2)
        self.assertTrue(all(pid != os.getpid() for pid in pids))
        self.assertLess(elapsed, 0.18)

    async def test_process_lane_does_not_block_while_async_lane_is_full(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        io_completed: list[float] = []

        @fluxera.actor(queue_name="mixed")
        async def io_wait(delay: float) -> None:
            await asyncio.sleep(delay)
            io_completed.append(time.time())

        cpu_task = fluxera.actor(
            broker=broker,
            actor_name="cpu_task",
            queue_name="mixed",
            execution="process",
        )(process_finished_at)

        worker = fluxera.Worker(
            broker,
            async_concurrency=2,
            thread_concurrency=1,
            process_concurrency=1,
        )
        await worker.start()
        try:
            await io_wait.send(0.2)
            await io_wait.send(0.2)
            await cpu_task.send(0.05)
            await broker.join(io_wait.queue_name)
        finally:
            await worker.stop()

        cpu_finished = max(record.result for record in worker.records.values() if record.delivery.actor_name == "cpu_task")
        self.assertEqual(len(io_completed), 2)
        self.assertLess(cpu_finished, min(io_completed))

    async def test_process_timeout_recycles_slot_for_next_task(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        slow = fluxera.actor(
            broker=broker,
            actor_name="slow_process",
            queue_name="cpu",
            execution="process",
            timeout=50,
        )(process_sleep)
        fast = fluxera.actor(
            broker=broker,
            actor_name="fast_process",
            queue_name="cpu",
            execution="process",
        )(process_identity)

        worker = fluxera.Worker(broker, process_concurrency=1)
        await worker.start()
        try:
            await slow.send(0.2)
            await broker.join(slow.queue_name)

            start = time.perf_counter()
            await fast.send(0.01)
            await broker.join(fast.queue_name)
            elapsed = time.perf_counter() - start
        finally:
            await worker.stop()

        self.assertLess(elapsed, 0.12)
        self.assertEqual(len(broker.dead_letters), 1)
        fast_records = [record for record in worker.records.values() if record.delivery.actor_name == "fast_process"]
        self.assertEqual(len(fast_records), 1)
        self.assertNotEqual(fast_records[0].result, os.getpid())

    async def test_process_shutdown_cancellation_requeues_by_default(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        sleeper = fluxera.actor(
            broker=broker,
            actor_name="process_sleeper",
            queue_name="cpu",
            execution="process",
        )(process_sleep)

        worker = fluxera.Worker(broker, process_concurrency=1)
        await worker.start()
        await sleeper.send(0.5)

        start = time.perf_counter()
        await worker.stop(timeout=0.01)
        elapsed = time.perf_counter() - start

        self.assertLess(elapsed, 0.25)
        self.assertEqual(broker.queue_objects[sleeper.queue_name].qsize(), 1)
        self.assertEqual(broker.dead_letters, [])

    async def test_retry_policy_retries_then_succeeds(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        attempts = 0

        @fluxera.actor(max_retries=2)
        async def flaky() -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("try again")

        worker = fluxera.Worker(broker, concurrency=4)
        await worker.start()
        try:
            await flaky.send()
            await broker.join(flaky.queue_name)
        finally:
            await worker.stop()

        self.assertEqual(attempts, 3)
        self.assertEqual(broker.dead_letters, [])
        latest_record = worker.records[next(iter(worker.records))]
        self.assertEqual(latest_record.attempt, 2)
        self.assertEqual(latest_record.state, "succeeded")

    async def test_timeout_policy_retries_then_dead_letters(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        attempts = 0

        @fluxera.actor(timeout=10, max_retries=1, retry_on_timeout=True)
        async def too_slow() -> None:
            nonlocal attempts
            attempts += 1
            await asyncio.sleep(0.05)

        worker = fluxera.Worker(broker, concurrency=2)
        await worker.start()
        try:
            await too_slow.send()
            await broker.join(too_slow.queue_name)
        finally:
            await worker.stop()

        self.assertEqual(attempts, 2)
        self.assertEqual(len(broker.dead_letters), 1)
        latest_record = worker.records[next(iter(worker.records))]
        self.assertEqual(latest_record.attempt, 1)
        self.assertEqual(latest_record.final_action, "reject")

    async def test_shutdown_cancellation_requeues_by_default(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        started = asyncio.Event()
        release = asyncio.Event()

        @fluxera.actor
        async def hanging() -> None:
            started.set()
            await release.wait()

        worker = fluxera.Worker(broker, concurrency=1)
        await worker.start()
        await hanging.send()
        await started.wait()
        await worker.stop(timeout=0.01)

        self.assertEqual(broker.queue_objects[hanging.queue_name].qsize(), 1)
        self.assertEqual(broker.dead_letters, [])

    async def test_revision_control_plane_bootstraps_and_promotes(self) -> None:
        broker = fluxera.StubBroker()

        self.assertEqual(await broker.ensure_serving_revision("default", "rev-a"), "rev-a")
        self.assertEqual(await broker.get_serving_revision("default"), "rev-a")
        self.assertFalse(
            await broker.promote_serving_revision(
                "default",
                "rev-c",
                expected_revision="rev-b",
            )
        )
        self.assertTrue(
            await broker.promote_serving_revision(
                "default",
                "rev-b",
                expected_revision="rev-a",
            )
        )
        self.assertEqual(await broker.get_serving_revision("default"), "rev-b")

        await broker.register_worker_revision(
            worker_id="stub-worker",
            worker_revision="rev-b",
            queue_states={"default": "accepting"},
        )
        self.assertEqual(broker.worker_revisions["stub-worker"], "rev-b")
        self.assertEqual(broker.worker_queue_states["stub-worker"], {"default": "accepting"})

        await broker.unregister_worker_revision(worker_id="stub-worker", queue_names={"default"})
        self.assertNotIn("stub-worker", broker.worker_revisions)
        self.assertNotIn("stub-worker", broker.worker_queue_states)

    async def test_revision_promotion_drains_unstarted_backlog_to_new_worker(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        started = asyncio.Event()
        release = asyncio.Event()

        @fluxera.actor(queue_name="rollout")
        async def rollout_task(label: str) -> str:
            if label == "block":
                started.set()
                await release.wait()
            return label

        old_worker = fluxera.Worker(
            broker,
            queues={rollout_task.queue_name},
            async_concurrency=1,
            thread_concurrency=0,
            process_concurrency=0,
            max_in_flight=2,
            prefetch=2,
            worker_revision="rev-old",
            worker_id="stub-old",
            revision_poll_interval=0.01,
        )
        new_worker = fluxera.Worker(
            broker,
            queues={rollout_task.queue_name},
            async_concurrency=1,
            thread_concurrency=0,
            process_concurrency=0,
            max_in_flight=2,
            prefetch=2,
            worker_revision="rev-new",
            worker_id="stub-new",
            revision_poll_interval=0.01,
        )
        await old_worker.start()
        await new_worker.start()
        try:
            blocked = await rollout_task.send("block")
            queued = await rollout_task.send("queued")
            await started.wait()
            await wait_for_async(lambda: broker.queue_objects[rollout_task.queue_name].qsize() == 0)

            self.assertTrue(
                await broker.promote_serving_revision(
                    rollout_task.queue_name,
                    "rev-new",
                    expected_revision="rev-old",
                )
            )
            await wait_for_async(lambda: old_worker.queue_states.get(rollout_task.queue_name) == "draining")
            await wait_for_async(lambda: new_worker.queue_states.get(rollout_task.queue_name) == "accepting")

            release.set()
            await broker.join(rollout_task.queue_name)
            await wait_for_async(
                lambda: queued.message_id in new_worker.records
                and new_worker.records[queued.message_id].state == "succeeded"
            )
        finally:
            await old_worker.stop()
            await new_worker.stop()

        self.assertIn(blocked.message_id, old_worker.records)
        self.assertIn(queued.message_id, new_worker.records)
        self.assertNotIn(queued.message_id, old_worker.records)
