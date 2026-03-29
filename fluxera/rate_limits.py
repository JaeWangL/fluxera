from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Iterator

import redis
import redis.asyncio as redis_async

from .errors import RateLimitExceeded


class ConcurrentRateLimiter:
    """Redis-backed concurrent rate limiter.

    This can be used as a distributed mutex when ``limit=1`` or as a small
    distributed concurrency counter when ``limit > 1``.
    """

    _ACQUIRE_SCRIPT = """
    local limit = tonumber(ARGV[1])
    local ttl_ms = tonumber(ARGV[2])
    local current = redis.call('get', KEYS[1])

    if not current then
        redis.call('psetex', KEYS[1], ttl_ms, 1)
        return 1
    end

    current = tonumber(current)
    if current >= limit then
        return 0
    end

    current = redis.call('incr', KEYS[1])
    redis.call('pexpire', KEYS[1], ttl_ms)
    return 1
    """

    _RELEASE_SCRIPT = """
    local current = redis.call('get', KEYS[1])
    if not current then
        return 0
    end

    current = tonumber(current)
    if current <= 1 then
        redis.call('del', KEYS[1])
        return 1
    end

    redis.call('decr', KEYS[1])
    return 1
    """

    def __init__(
        self,
        client: Any,
        key: str,
        *,
        limit: int = 1,
        ttl: int = 900_000,
        ttl_ms: int | None = None,
        prefix: str = "fluxera:concurrency",
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be positive.")

        resolved_client = client
        if not hasattr(resolved_client, "eval"):
            nested_client = getattr(client, "client", None)
            if nested_client is not None and hasattr(nested_client, "eval"):
                resolved_client = nested_client
            else:
                raise TypeError("ConcurrentRateLimiter requires a Redis client or a broker exposing .client.eval().")

        self.client = resolved_client
        self.limit = int(limit)
        self.ttl_ms = max(int(ttl_ms if ttl_ms is not None else ttl), 1)
        self.key = f"{prefix}:{key}" if prefix else key

    @property
    def is_async_client(self) -> bool:
        return isinstance(self.client, redis_async.Redis)

    @contextmanager
    def acquire(self, *, raise_on_failure: bool = True) -> Iterator[bool]:
        if self.is_async_client:
            raise RuntimeError("acquire() requires a synchronous Redis client. Use aacquire() for async clients.")

        acquired = self._acquire_sync()
        if raise_on_failure and not acquired:
            raise RateLimitExceeded(f"rate limit exceeded for key {self.key!r}")

        try:
            yield acquired
        finally:
            if acquired:
                self._release_sync()

    @asynccontextmanager
    async def aacquire(self, *, raise_on_failure: bool = True):
        acquired = await self._acquire_async()
        if raise_on_failure and not acquired:
            raise RateLimitExceeded(f"rate limit exceeded for key {self.key!r}")

        try:
            yield acquired
        finally:
            if acquired:
                await self._release_async()

    def _acquire_sync(self) -> bool:
        return bool(
            self.client.eval(
                self._ACQUIRE_SCRIPT,
                1,
                self.key,
                self.limit,
                self.ttl_ms,
            )
        )

    def _release_sync(self) -> bool:
        return bool(self.client.eval(self._RELEASE_SCRIPT, 1, self.key))

    async def _acquire_async(self) -> bool:
        if self.is_async_client:
            return bool(
                await self.client.eval(
                    self._ACQUIRE_SCRIPT,
                    1,
                    self.key,
                    self.limit,
                    self.ttl_ms,
                )
            )

        return await asyncio.to_thread(self._acquire_sync)

    async def _release_async(self) -> bool:
        if self.is_async_client:
            return bool(await self.client.eval(self._RELEASE_SCRIPT, 1, self.key))

        return await asyncio.to_thread(self._release_sync)


__all__ = [
    "ConcurrentRateLimiter",
]
