from __future__ import annotations

import os
import unittest
from uuid import uuid4

import redis
import redis.asyncio as redis_async

import fluxera


class ConcurrentRateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis_url = "redis://127.0.0.1:6379/15"
        self.sync_client = redis.Redis.from_url(self.redis_url)
        self.async_client = redis_async.Redis.from_url(self.redis_url, decode_responses=False)
        self.test_keys: list[str] = []
        self.brokers: list[fluxera.RedisBroker] = []

        try:
            await self.async_client.ping()
        except Exception as exc:
            self.sync_client.close()
            await self.async_client.aclose()
            self.skipTest(f"Redis is not available for integration tests: {exc}")

    async def asyncTearDown(self) -> None:
        if self.test_keys:
            self.sync_client.delete(*self.test_keys)

        for broker in self.brokers:
            await broker.close()

        self.sync_client.close()
        await self.async_client.aclose()

    def unique_key(self) -> str:
        key = f"fluxera:concurrency:test:{uuid4().hex}"
        self.test_keys.append(key)
        return key

    def test_sync_mutex_acquire_and_release(self) -> None:
        key = self.unique_key()
        limiter_a = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=1, ttl_ms=1000)
        limiter_b = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=1, ttl_ms=1000)

        with limiter_a.acquire(raise_on_failure=False) as acquired_a:
            self.assertTrue(acquired_a)
            with limiter_b.acquire(raise_on_failure=False) as acquired_b:
                self.assertFalse(acquired_b)

        with limiter_b.acquire(raise_on_failure=False) as acquired_after_release:
            self.assertTrue(acquired_after_release)

    def test_sync_limit_greater_than_one(self) -> None:
        key = self.unique_key()
        limiter_a = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=2, ttl_ms=1000)
        limiter_b = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=2, ttl_ms=1000)
        limiter_c = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=2, ttl_ms=1000)

        with limiter_a.acquire(raise_on_failure=False) as acquired_a:
            self.assertTrue(acquired_a)
            with limiter_b.acquire(raise_on_failure=False) as acquired_b:
                self.assertTrue(acquired_b)
                with limiter_c.acquire(raise_on_failure=False) as acquired_c:
                    self.assertFalse(acquired_c)

    def test_sync_acquire_raises_rate_limit_exceeded(self) -> None:
        key = self.unique_key()
        limiter_a = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=1, ttl_ms=1000)
        limiter_b = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="", limit=1, ttl_ms=1000)

        with limiter_a.acquire():
            with self.assertRaises(fluxera.RateLimitExceeded):
                with limiter_b.acquire():
                    self.fail("second limiter should not have been acquired")

    async def test_async_acquire_with_broker_client(self) -> None:
        namespace = f"fluxera-rate-limit-{uuid4().hex}"
        broker = fluxera.RedisBroker(self.redis_url, namespace=namespace)
        self.brokers.append(broker)
        key = self.unique_key()

        limiter_a = fluxera.ConcurrentRateLimiter(broker, key, prefix="", limit=1, ttl_ms=1000)
        limiter_b = fluxera.ConcurrentRateLimiter(broker, key, prefix="", limit=1, ttl_ms=1000)

        async with limiter_a.aacquire(raise_on_failure=False) as acquired_a:
            self.assertTrue(acquired_a)
            async with limiter_b.aacquire(raise_on_failure=False) as acquired_b:
                self.assertFalse(acquired_b)

        async with limiter_b.aacquire(raise_on_failure=False) as acquired_after_release:
            self.assertTrue(acquired_after_release)

    def test_sync_acquire_rejects_async_client(self) -> None:
        key = self.unique_key()
        limiter = fluxera.ConcurrentRateLimiter(self.async_client, key, prefix="", limit=1, ttl_ms=1000)

        with self.assertRaises(RuntimeError):
            with limiter.acquire():
                self.fail("sync acquire should not accept redis.asyncio clients")

    def test_default_ttl_uses_legacy_two_hour_baseline_and_env_override(self) -> None:
        key = self.unique_key()
        previous = os.environ.get("WORKER_CONCURRENCY_LOCK_TTL_MS")

        try:
            os.environ.pop("WORKER_CONCURRENCY_LOCK_TTL_MS", None)
            limiter = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="")
            self.assertEqual(limiter.ttl_ms, 2 * 60 * 60 * 1000)

            os.environ["WORKER_CONCURRENCY_LOCK_TTL_MS"] = "12345"
            overridden = fluxera.ConcurrentRateLimiter(self.sync_client, key, prefix="")
            self.assertEqual(overridden.ttl_ms, 12345)
        finally:
            if previous is None:
                os.environ.pop("WORKER_CONCURRENCY_LOCK_TTL_MS", None)
            else:
                os.environ["WORKER_CONCURRENCY_LOCK_TTL_MS"] = previous
