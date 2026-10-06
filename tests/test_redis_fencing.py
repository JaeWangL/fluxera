"""Acquisition fencing against a real Redis server (no fakeredis Lua emulation)."""
from __future__ import annotations

import asyncio
import os
import unittest
from uuid import uuid4
from unittest.mock import AsyncMock, patch

import redis.asyncio as redis

import fluxera
from fluxera.errors import DeliveryOwnershipLost, RateLimitExceeded


class RedisFencingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.url = os.environ.get("FLUXERA_REDIS_URL", "redis://127.0.0.1:6379/15")
        self.client = redis.from_url(self.url)
        try:
            await self.client.ping()
        except Exception as exc:
            await self.client.aclose()
            self.skipTest(f"Redis unavailable: {exc}")
        self.ns = f"fluxera-fencing-test-{uuid4().hex}"
        self.broker = fluxera.RedisBroker(self.url, namespace=self.ns, lease_seconds=1)
        self.a = await self.broker.open_consumer("q")
        self.b = await self.broker.open_consumer("q")
        self.stream = self.broker._stream_key("q")
        self.group = self.broker.group_name

    async def asyncTearDown(self):
        await self.a.close()
        await self.b.close()
        await self.broker.close()
        keys = [key async for key in self.client.scan_iter(match=self.ns + ":*")]
        if keys:
            await self.client.delete(*keys)
        await self.client.aclose()

    async def delivered(self, options=None):
        message = fluxera.Message(queue_name="q", actor_name="noop", options=options or {})
        await self.broker.send(message)
        return (await self.a.receive(limit=1, timeout=.1))[0]

    async def reclaim(self, delivery, target=None):
        target = target or self.b
        pending = (await self.client.xpending_range(self.stream, self.group,
                                                  delivery.transport_id, delivery.transport_id, 1))[0]
        await self.client.xclaim(self.stream, self.group, pending["consumer"], 0,
                                 [delivery.transport_id], idle=2000, justid=True)
        target.claim_cursor = "0-0"
        return (await target._claim_stale(limit=1))[0]

    async def assert_untouched(self, old, new):
        rows = await self.client.xpending_range(self.stream, self.group, "-", "+", 10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["consumer"].decode(), self.b.consumer_name)
        self.assertEqual(rows[0]["times_delivered"], new.metadata["redis_delivery_generation"])
        self.assertEqual(await self.client.xlen(self.stream), 1)
        self.assertTrue(await self.client.exists(self.broker._message_key(old.message_id)))
        self.assertEqual(await self.broker.get_dead_letter_records("q"), [])

    async def test_stale_ack_cannot_delete_new_owners_payload_or_dedupe(self):
        old = await self.delivered({"deduplication": {"id": "unique", "mode": "simple"}})
        new = await self.reclaim(old)
        with self.assertRaises(DeliveryOwnershipLost):
            await self.a.ack(old)
        await self.assert_untouched(old, new)
        self.assertEqual(await self.client.get(self.broker._dedupe_key("q", "noop", "unique")),
                         old.message_id.encode())
        await self.b.ack(new)

    async def test_stale_renewal_cannot_take_ownership_back(self):
        old = await self.delivered()
        new = await self.reclaim(old)
        with self.assertRaises(DeliveryOwnershipLost):
            await self.a.extend_lease(old, seconds=1)
        await self.assert_untouched(old, new)
        await self.b.ack(new)

    async def test_stale_reject_retry_and_defer_make_no_writes(self):
        for operation in ("reject", "requeue", "retry", "defer"):
            with self.subTest(operation=operation):
                old = await self.delivered()
                new = await self.reclaim(old)
                with self.assertRaises(DeliveryOwnershipLost):
                    if operation == "reject":
                        await self.a.reject(old)
                    elif operation == "requeue":
                        await self.a.reject(old, requeue=True)
                    else:
                        await self.broker.retry_delivery(
                            self.a, old, old.message.copy(options={"attempt": 9}),
                            delay=10 if operation == "defer" else None,
                        )
                await self.assert_untouched(old, new)
                self.assertEqual(await self.client.zcard(self.broker._delayed_key("q")), 0)
                self.assertNotIn("attempt", self.broker._decode_message(
                    await self.client.get(self.broker._message_key(old.message_id))).options)
                await self.b.ack(new)

    async def test_same_consumer_aba_is_fenced_by_acquisition_generation(self):
        old = await self.delivered()
        second = await self.reclaim(old)
        # An old coroutine survives while its consumer has a new acquisition.
        self.a.active_ids.discard(old.transport_id)
        third = await self.reclaim(second, self.a)
        self.assertGreater(third.metadata["redis_delivery_generation"],
                           old.metadata["redis_delivery_generation"])
        # A batched reply must identify the generation, not just the stream ID.
        results = await asyncio.gather(self.a.extend_lease(old, seconds=1),
                                       self.a.extend_lease(third, seconds=1), return_exceptions=True)
        self.assertIsInstance(results[0], DeliveryOwnershipLost)
        self.assertIsNone(results[1])
        with self.assertRaises(DeliveryOwnershipLost):
            await self.a.ack(old)
        self.assertIn(third.transport_id, self.a.active_ids)
        await self.a.close(forget=True)
        await self.a.ensure_ownership(third)
        await self.a.ack(third)

    async def test_self_reclamation_does_not_invalidate_queued_acquisitions(self):
        delivery = await self.delivered()
        await self.client.xclaim(self.stream, self.group, self.a.consumer_name, 0,
                                 [delivery.transport_id], idle=2000, justid=True)
        self.assertEqual(await self.a._claim_stale(limit=1), [])
        await self.a.ensure_ownership(delivery)
        await self.a.ack(delivery)

    async def test_deleted_pending_entry_does_not_starve_later_reclamation(self):
        ghost = await self.delivered()
        live = await self.delivered()
        await self.client.xclaim(self.stream, self.group, self.a.consumer_name, 0,
                                 [ghost.transport_id, live.transport_id], idle=2000, justid=True)
        await self.client.xdel(self.stream, ghost.transport_id)
        self.broker.pending_scan_size = 1
        # Redis 6.2 can retain the deleted PEL row; the cursor must pass it.
        self.assertEqual(await self.b._claim_stale(limit=1), [])
        reclaimed = await self.b._claim_stale(limit=1)
        self.assertEqual([item.transport_id for item in reclaimed], [live.transport_id])
        await self.b.ack(reclaimed[0])

    async def test_real_redis_acquisition_counter_and_batched_mixed_renewal(self):
        first = await self.delivered()
        second = await self.delivered()
        self.assertEqual(first.metadata["redis_delivery_generation"], 1)
        await self.a.extend_lease(first, seconds=1)
        current = (await self.client.xpending_range(self.stream, self.group,
                                                   first.transport_id, first.transport_id, 1))[0]
        self.assertEqual(current["times_delivered"], 1)
        new = await self.reclaim(first)
        self.assertEqual(new.metadata["redis_delivery_generation"], 2)
        results = await asyncio.gather(self.a.extend_lease(first, seconds=1),
                                       self.a.extend_lease(second, seconds=1), return_exceptions=True)
        self.assertIsInstance(results[0], DeliveryOwnershipLost)
        self.assertIsNone(results[1])
        await self.a.ack(second)
        await self.b.ack(new)

    async def test_retry_is_atomic_and_survives_a_lost_response_without_a_second_enqueue(self):
        delivery = await self.delivered()
        original = self.broker.scripts.acknowledge_delivery
        async def commit_then_disconnect(**kwargs):
            await original(**kwargs)
            raise ConnectionError("reply lost after commit")
        with patch.object(self.broker.scripts, "acknowledge_delivery", commit_then_disconnect):
            with self.assertRaises(ConnectionError):
                await self.broker.retry_delivery(self.a, delivery, delivery.message.copy(options={"attempt": 1}))
        with self.assertRaises(DeliveryOwnershipLost):
            await self.broker.retry_delivery(self.a, delivery, delivery.message.copy(options={"attempt": 1}))
        self.assertEqual(await self.client.xlen(self.stream), 1)
        successor = (await self.b.receive(limit=1, timeout=.1))[0]
        self.assertEqual(successor.options["attempt"], 1)
        self.assertEqual(await self.client.get(self.broker._message_ref_key(delivery.message_id)), b"1")
        await self.b.ack(successor)

    async def test_stale_worker_cannot_emit_outcomes_on_success_failure_or_defer(self):
        for outcome in ("success", "failure", "terminal_failure", "defer", "cancel"):
            with self.subTest(outcome=outcome):
                entered, finish = asyncio.Event(), asyncio.Event()
                callbacks = []
                async def callback(context):
                    callbacks.append(context)
                async def action():
                    entered.set()
                    await finish.wait()
                    if outcome in ("failure", "terminal_failure"):
                        raise ValueError("failed work")
                    if outcome == "defer":
                        raise RateLimitExceeded("busy")
                    if outcome == "cancel":
                        raise asyncio.CancelledError
                actor = fluxera.actor(broker=self.broker, actor_name="action_" + outcome, queue_name="q",
                                      max_retries=0 if outcome == "terminal_failure" else 1,
                                      min_backoff=0, on_success=callback, on_failure=callback,
                                      on_retry_scheduled=callback, on_retry_exhausted=callback,
                                      on_dead_lettered=callback)(action)
                worker = fluxera.Worker(self.broker, concurrency=1, process_concurrency=0)
                await worker.start()
                try:
                    # The heartbeat cannot renew while the simulated connection is down.
                    with patch.object(self.broker.scripts, "renew_deliveries", AsyncMock(side_effect=ConnectionError)):
                        message = await actor.send()
                        await asyncio.wait_for(entered.wait(), 3)
                        rows = await self.client.xpending_range(self.stream, self.group, "-", "+", 1)
                        transport_id = rows[0]["message_id"]
                        await self.client.xclaim(self.stream, self.group, rows[0]["consumer"], 0,
                                                 [transport_id], idle=2000, justid=True)
                        self.b.claim_cursor = "0-0"
                        new = (await self.b._claim_stale(limit=1))[0]
                        finish.set()
                        for _ in range(100):
                            if worker.records[message.message_id].finished_at_ms is not None:
                                break
                            await asyncio.sleep(.01)
                        self.assertEqual(worker.records[message.message_id].state, "ownership_lost")
                        self.assertEqual(callbacks, [])
                        self.assertEqual(await self.client.xlen(self.stream), 1)
                        self.assertEqual(await self.client.zcard(self.broker._delayed_key("q")), 0)
                        self.assertEqual(await self.broker.get_dead_letter_records("q"), [])
                        await self.b.ack(new)
                finally:
                    finish.set()
                    await worker.stop()
                # Worker.stop closes the broker; Redis clients reconnect safely.
