from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4


def current_millis() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class Message:
    """A serializable message payload destined for an actor."""

    queue_name: str
    actor_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid4()))
    message_timestamp: int = field(default_factory=current_millis)

    def copy(
        self,
        *,
        queue_name: Optional[str] = None,
        actor_name: Optional[str] = None,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> "Message":
        next_kwargs = self.kwargs.copy()
        if kwargs is not None:
            next_kwargs = kwargs.copy()

        next_options = self.options.copy()
        if options is not None:
            next_options.update(options)

        return Message(
            queue_name=queue_name or self.queue_name,
            actor_name=actor_name or self.actor_name,
            args=args if args is not None else self.args,
            kwargs=next_kwargs,
            options=next_options,
            message_id=self.message_id,
            message_timestamp=self.message_timestamp,
        )

