from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DRAMATIQ_ROOT = PROJECT_ROOT.parent / "dramatiq"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(DRAMATIQ_ROOT) not in sys.path:
    sys.path.insert(0, str(DRAMATIQ_ROOT))

import dramatiq
from dramatiq import Worker as DramatiqWorker
from dramatiq.brokers.stub import StubBroker as DramatiqStubBroker
from dramatiq.middleware.asyncio import AsyncIO

import fluxera


MESSAGE_COUNT = 200
SLEEP_SECS = 0.01


async def run_fluxera_benchmark(*, concurrency: int) -> float:
    broker = fluxera.StubBroker()
    fluxera.set_broker(broker)

    @fluxera.actor
    async def sleeper(delay: float) -> None:
        await asyncio.sleep(delay)

    worker = fluxera.Worker(broker, concurrency=concurrency)
    await worker.start()
    try:
        start = time.perf_counter()
        for _ in range(MESSAGE_COUNT):
            await sleeper.send(SLEEP_SECS)

        await broker.join(sleeper.queue_name)
        return time.perf_counter() - start
    finally:
        await worker.stop()


def run_dramatiq_benchmark(*, worker_threads: int) -> float:
    broker = DramatiqStubBroker(middleware=[])
    broker.add_middleware(AsyncIO())
    dramatiq.set_broker(broker)

    @dramatiq.actor
    async def sleeper(delay: float) -> None:
        await asyncio.sleep(delay)

    worker = DramatiqWorker(broker, worker_threads=worker_threads, worker_timeout=50)
    worker.start()
    try:
        start = time.perf_counter()
        for _ in range(MESSAGE_COUNT):
            sleeper.send(SLEEP_SECS)

        broker.join(sleeper.queue_name)
        return time.perf_counter() - start
    finally:
        worker.stop()


def main() -> int:
    fluxera_elapsed = asyncio.run(run_fluxera_benchmark(concurrency=MESSAGE_COUNT))
    dramatiq_default_elapsed = run_dramatiq_benchmark(worker_threads=8)
    dramatiq_high_thread_elapsed = run_dramatiq_benchmark(worker_threads=MESSAGE_COUNT)

    print("Stub benchmark for async I/O-heavy workload")
    print(f"messages={MESSAGE_COUNT} sleep={SLEEP_SECS:.3f}s")
    print(f"fluxera(concurrency={MESSAGE_COUNT}): {fluxera_elapsed:.4f}s")
    print(f"dramatiq(worker_threads=8): {dramatiq_default_elapsed:.4f}s")
    print(f"dramatiq(worker_threads={MESSAGE_COUNT}): {dramatiq_high_thread_elapsed:.4f}s")

    if fluxera_elapsed > 0:
        print(f"fluxera vs dramatiq(8 threads): {dramatiq_default_elapsed / fluxera_elapsed:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
