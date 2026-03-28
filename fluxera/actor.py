from __future__ import annotations

import asyncio
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Protocol, TypeVar, Union, overload

from .broker import Broker, get_broker
from .message import Message

if TYPE_CHECKING:
    from .runtime.worker import Worker


P = TypeVar("P")
R = TypeVar("R")
ExecutionMode = str


class Actor:
    """Thin wrapper around callables that stores queueing metadata."""

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        broker: Broker,
        actor_name: str,
        queue_name: str,
        execution: Optional[ExecutionMode] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> None:
        if actor_name in broker.actors:
            raise ValueError(f"An actor named {actor_name!r} is already registered.")

        inferred_execution = "async" if iscoroutinefunction(fn) else "thread"
        self.fn = fn
        self.broker = broker
        self.actor_name = actor_name
        self.queue_name = queue_name
        self.execution = execution or inferred_execution
        self.options = options or {}

        if self.execution == "async" and not iscoroutinefunction(fn):
            raise TypeError("Execution mode 'async' requires an async function.")

        self.broker.declare_actor(self)

    def message(self, *args: Any, **kwargs: Any) -> Message:
        return self.message_with_options(args=args, kwargs=kwargs)

    def message_with_options(
        self,
        *,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        **options: Any,
    ) -> Message:
        return Message(
            queue_name=self.queue_name,
            actor_name=self.actor_name,
            args=args,
            kwargs=(kwargs or {}).copy(),
            options={**self.options, **options},
        )

    async def send(self, *args: Any, **kwargs: Any) -> Message:
        return await self.send_with_options(args=args, kwargs=kwargs)

    async def send_with_options(
        self,
        *,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        delay: Optional[float] = None,
        **options: Any,
    ) -> Message:
        return await self.broker.send(
            self.message_with_options(args=args, kwargs=kwargs, **options),
            delay=delay,
        )

    def send_sync(self, *args: Any, **kwargs: Any) -> Message:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.send(*args, **kwargs))

        raise RuntimeError("send_sync cannot be called while an event loop is already running.")

    def send_with_options_sync(
        self,
        *,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        delay: Optional[float] = None,
        **options: Any,
    ) -> Message:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.send_with_options(args=args, kwargs=kwargs, delay=delay, **options))

        raise RuntimeError("send_with_options_sync cannot be called while an event loop is already running.")

    async def run(self, *args: Any, worker: Optional["Worker"] = None, **kwargs: Any) -> Any:
        if self.execution == "async":
            async_fn = self.fn
            return await async_fn(*args, **kwargs)

        if self.execution == "thread":
            if worker is not None:
                return await worker.run_in_thread(self.fn, *args, **kwargs)
            return await asyncio.to_thread(self.fn, *args, **kwargs)

        if self.execution == "process":
            if worker is None:
                raise RuntimeError("Process execution requires a worker runtime.")
            return await worker.run_in_process(self.fn, *args, **kwargs)

        raise RuntimeError(f"Unknown execution mode {self.execution!r}.")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            "Actor(fn=%r, actor_name=%r, queue_name=%r, execution=%r)"
            % (self.fn, self.actor_name, self.queue_name, self.execution)
        )


class ActorDecorator(Protocol):
    def __call__(self, fn: Callable[..., Any]) -> Actor: ...


@overload
def actor(fn: Callable[..., Any]) -> Actor:
    pass


@overload
def actor(
    *,
    broker: Optional[Broker] = None,
    actor_name: Optional[str] = None,
    queue_name: str = "default",
    execution: Optional[ExecutionMode] = None,
    **options: Any,
) -> ActorDecorator:
    pass


def actor(
    fn: Optional[Callable[..., Any]] = None,
    *,
    broker: Optional[Broker] = None,
    actor_name: Optional[str] = None,
    queue_name: str = "default",
    execution: Optional[ExecutionMode] = None,
    **options: Any,
) -> Union[Actor, ActorDecorator]:
    def decorator(inner_fn: Callable[..., Any]) -> Actor:
        target_broker = broker or get_broker()
        return Actor(
            inner_fn,
            broker=target_broker,
            actor_name=actor_name or inner_fn.__name__,
            queue_name=queue_name,
            execution=execution,
            options=options,
        )

    if fn is None:
        return decorator

    return decorator(fn)
