from __future__ import annotations

import unittest

import fluxera


class ActorTests(unittest.IsolatedAsyncioTestCase):
    async def test_actor_send_and_stub_worker_process_messages(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)
        seen: list[int] = []

        @fluxera.actor
        async def record(value: int) -> None:
            seen.append(value)

        async with fluxera.Worker(broker, concurrency=8):
            for value in (1, 2, 3):
                await record.send(value)

            await broker.join(record.queue_name)

        self.assertEqual(sorted(seen), [1, 2, 3])

    def test_actor_send_sync_outside_event_loop(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)

        @fluxera.actor
        async def noop() -> None:
            return None

        message = noop.send_sync()
        self.assertEqual(message.actor_name, "noop")

    def test_actor_message_with_options_merges_actor_defaults(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)

        @fluxera.actor(queue_name="jobs", max_retries=2)
        async def noop() -> None:
            return None

        message = noop.message_with_options(timeout=25, args=())
        self.assertEqual(message.queue_name, "jobs")
        self.assertEqual(message.options["max_retries"], 2)
        self.assertEqual(message.options["timeout"], 25)
