from __future__ import annotations

import unittest

from fluxera.encoder import JSONMessageEncoder, PickleMessageEncoder
from fluxera.message import Message


class MessageEncoderTests(unittest.TestCase):
    def test_json_encoder_round_trips_supported_message_shapes(self) -> None:
        message = Message(
            queue_name="default",
            actor_name="encode_me",
            args=(1, b"bytes", ("nested", 2)),
            kwargs={"payload": [True, None, {"token": b"x"}]},
            options={"attempt": 3, "tags": ("a", "b")},
        )

        encoder = JSONMessageEncoder()
        encoded = encoder.dumps(message)
        decoded = encoder.loads(encoded)

        self.assertEqual(decoded, message)

    def test_pickle_encoder_round_trips_messages(self) -> None:
        message = Message(queue_name="default", actor_name="pickle_me", args=(1,), kwargs={"x": 2})

        encoder = PickleMessageEncoder()
        encoded = encoder.dumps(message)
        decoded = encoder.loads(encoded)

        self.assertEqual(decoded, message)
