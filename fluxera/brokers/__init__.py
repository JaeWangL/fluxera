from __future__ import annotations

from typing import Any

from .redis import RedisBroker
from .stub import StubBroker

__all__ = ["RabbitMQBroker", "RedisBroker", "StubBroker"]


def __getattr__(name: str) -> Any:
    if name == "RabbitMQBroker":
        from .rabbitmq import RabbitMQBroker

        return RabbitMQBroker
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
