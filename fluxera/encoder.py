from __future__ import annotations

import base64
import json
import pickle
from dataclasses import asdict
from typing import Protocol

from .message import Message


class MessageEncoder(Protocol):
    def dumps(self, message: Message) -> bytes:
        raise NotImplementedError

    def loads(self, payload: bytes) -> Message:
        raise NotImplementedError


class JSONMessageEncoder:
    """JSON encoder for trusted message primitives."""

    def dumps(self, message: Message) -> bytes:
        payload = self._encode_message(message)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")

    def loads(self, payload: bytes) -> Message:
        raw = json.loads(payload.decode("utf-8"))
        return self._decode_message(raw)

    def _encode_message(self, message: Message) -> dict[str, object]:
        data = asdict(message)
        return {
            "queue_name": data["queue_name"],
            "actor_name": data["actor_name"],
            "args": self._encode_value(data["args"]),
            "kwargs": self._encode_value(data["kwargs"]),
            "options": self._encode_value(data["options"]),
            "message_id": data["message_id"],
            "message_timestamp": data["message_timestamp"],
        }

    def _decode_message(self, data: dict[str, object]) -> Message:
        return Message(
            queue_name=str(data["queue_name"]),
            actor_name=str(data["actor_name"]),
            args=tuple(self._decode_value(data["args"])),
            kwargs=dict(self._decode_value(data["kwargs"])),
            options=dict(self._decode_value(data["options"])),
            message_id=str(data["message_id"]),
            message_timestamp=int(data["message_timestamp"]),
        )

    def _encode_value(self, value):
        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        if isinstance(value, bytes):
            return {"__fluxera_bytes__": base64.b64encode(value).decode("ascii")}

        if isinstance(value, tuple):
            return {"__fluxera_tuple__": [self._encode_value(item) for item in value]}

        if isinstance(value, list):
            return [self._encode_value(item) for item in value]

        if isinstance(value, dict):
            encoded: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError(f"Redis message encoding only supports string dict keys, got {type(key)!r}.")
                encoded[key] = self._encode_value(item)
            return encoded

        raise TypeError(
            "RedisBroker default JSON encoding only supports JSON primitives, bytes, lists, tuples, and dicts. "
            f"Got {type(value)!r}."
        )

    def _decode_value(self, value):
        if isinstance(value, list):
            return [self._decode_value(item) for item in value]

        if isinstance(value, dict):
            if set(value) == {"__fluxera_bytes__"}:
                return base64.b64decode(value["__fluxera_bytes__"])
            if set(value) == {"__fluxera_tuple__"}:
                return tuple(self._decode_value(item) for item in value["__fluxera_tuple__"])
            return {key: self._decode_value(item) for key, item in value.items()}

        return value


class PickleMessageEncoder:
    """Pickle encoder for fully trusted environments."""

    def dumps(self, message: Message) -> bytes:
        return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, payload: bytes) -> Message:
        message = pickle.loads(payload)
        if not isinstance(message, Message):
            raise TypeError(f"Expected a Message payload, got {type(message)!r}.")
        return message
