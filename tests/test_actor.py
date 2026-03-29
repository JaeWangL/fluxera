from __future__ import annotations

import unittest

import fluxera
from fluxera.encoder import JSONMessageEncoder


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

    def test_actor_message_with_options_keeps_serializable_defaults_only(self) -> None:
        broker = fluxera.StubBroker()
        fluxera.set_broker(broker)

        def retry_when(attempt: int, exc: BaseException, record: fluxera.TaskRecord) -> bool:
            del attempt, exc, record
            return False

        @fluxera.actor(
            queue_name="jobs",
            max_retries=2,
            throws=(ValueError,),
            retry_for=(RuntimeError,),
            retry_when=retry_when,
            on_success="success_actor",
        )
        async def noop() -> None:
            return None

        message = noop.message_with_options(timeout=25, args=())
        self.assertEqual(message.queue_name, "jobs")
        self.assertEqual(message.options["max_retries"], 2)
        self.assertEqual(message.options["timeout"], 25)
        self.assertEqual(message.options["on_success"], "success_actor")
        self.assertNotIn("throws", message.options)
        self.assertNotIn("retry_for", message.options)
        self.assertNotIn("retry_when", message.options)
        self.assertIsInstance(JSONMessageEncoder().dumps(message), bytes)
