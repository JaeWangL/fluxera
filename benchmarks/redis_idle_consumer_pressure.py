from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

import redis.asyncio as redis_async

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fluxera


def _command_calls(info: dict, command: str) -> int:
    payload = info.get(f"cmdstat_{command.lower()}", {})
    if isinstance(payload, dict):
        return int(payload.get("calls", 0))
    return 0


async def _delete_namespace(client: redis_async.Redis, namespace: str) -> None:
    keys: list[bytes] = []
    async for key in client.scan_iter(match=f"{namespace}:*", count=1_000):
        keys.append(key)
        if len(keys) >= 1_000:
            await client.delete(*keys)
            keys.clear()
    if keys:
        await client.delete(*keys)


def _declare_queues(broker: fluxera.RedisBroker, queue_count: int) -> list[str]:
    queue_names = [f"queue-{index:02d}" for index in range(queue_count)]
    for index, queue_name in enumerate(queue_names):
        async def noop(value: int = 0) -> None:
            del value

        fluxera.actor(
            broker=broker,
            actor_name=f"idle_noop_{index}",
            queue_name=queue_name,
        )(noop)
    return queue_names


def _build_broker(
    redis_url: str,
    *,
    namespace: str,
    optimized: bool,
) -> fluxera.RedisBroker:
    if optimized:
        return fluxera.RedisBroker(
            redis_url,
            namespace=namespace,
            client_name=f"fluxera-idle-bench:{namespace}",
        )

    return fluxera.RedisBroker(
        redis_url,
        namespace=namespace,
        client_name=f"fluxera-idle-bench-baseline:{namespace}",
        promote_due_interval_seconds=0,
        stale_claim_interval_seconds=0,
    )


async def _sample_clients(
    client: redis_async.Redis,
    *,
    stop_event: asyncio.Event,
    interval_seconds: float,
) -> dict[str, int]:
    max_connected = 0
    max_blocked = 0
    while not stop_event.is_set():
        info = await client.info("clients")
        max_connected = max(max_connected, int(info.get("connected_clients", 0)))
        max_blocked = max(max_blocked, int(info.get("blocked_clients", 0)))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
    return {
        "max_connected_clients": max_connected,
        "max_blocked_clients": max_blocked,
    }


async def _run_scenario(
    redis_url: str,
    *,
    scenario: str,
    queue_count: int,
    duration_seconds: float,
    warmup_seconds: float,
    poll_timeout: float,
    optimized_max_receives: int,
    optimized_idle_backoff_max: float,
) -> dict[str, object]:
    optimized = scenario == "optimized"
    namespace = f"fluxera-idle-bench-{scenario}-{uuid4().hex}"
    metrics_client = redis_async.from_url(redis_url, decode_responses=False)
    broker = _build_broker(redis_url, namespace=namespace, optimized=optimized)
    queue_names = _declare_queues(broker, queue_count)

    worker = fluxera.Worker(
        broker,
        queues=set(queue_names),
        concurrency=32,
        thread_concurrency=0,
        process_concurrency=0,
        prefetch=32,
        poll_timeout=poll_timeout,
        consumer_idle_backoff_max=optimized_idle_backoff_max if optimized else 0,
        max_concurrent_consumer_receives=optimized_max_receives if optimized else queue_count,
        worker_revision=f"bench-{scenario}",
        revision_poll_interval=1.0,
    )

    try:
        await _delete_namespace(metrics_client, namespace)
        await worker.start()
        await asyncio.sleep(warmup_seconds)
        await metrics_client.config_resetstat()

        stop_event = asyncio.Event()
        sampler_task = asyncio.create_task(
            _sample_clients(
                metrics_client,
                stop_event=stop_event,
                interval_seconds=0.05,
            )
        )
        started = time.perf_counter()
        await asyncio.sleep(duration_seconds)
        elapsed = time.perf_counter() - started
        stop_event.set()
        client_samples = await sampler_task

        stats = await metrics_client.info("commandstats")
        totals = await metrics_client.info()
        total_commands = int(totals.get("total_commands_processed", 0))
        return {
            "scenario": scenario,
            "queues": queue_count,
            "elapsed_seconds": round(elapsed, 3),
            "total_commands_processed": total_commands,
            "commands_per_second": round(total_commands / elapsed, 2),
            "xreadgroup_calls": _command_calls(stats, "xreadgroup"),
            "xautoclaim_calls": _command_calls(stats, "xautoclaim"),
            "evalsha_calls": _command_calls(stats, "evalsha"),
            "hset_calls": _command_calls(stats, "hset"),
            **client_samples,
        }
    finally:
        await worker.stop()
        await broker.close()
        await _delete_namespace(metrics_client, namespace)
        await metrics_client.aclose()


async def _main_async(args: argparse.Namespace) -> int:
    probe = redis_async.from_url(args.redis_url, decode_responses=False)
    try:
        await probe.ping()
    finally:
        await probe.aclose()

    scenarios = args.scenario
    results = []
    for scenario in scenarios:
        result = await _run_scenario(
            args.redis_url,
            scenario=scenario,
            queue_count=args.queues,
            duration_seconds=args.duration,
            warmup_seconds=args.warmup,
            poll_timeout=args.poll_timeout,
            optimized_max_receives=args.optimized_max_receives,
            optimized_idle_backoff_max=args.optimized_idle_backoff_max,
        )
        results.append(result)
        print(json.dumps(result, sort_keys=True))

    if len(results) == 2:
        baseline = next((item for item in results if item["scenario"] == "baseline"), None)
        optimized = next((item for item in results if item["scenario"] == "optimized"), None)
        if baseline and optimized:
            baseline_cps = float(baseline["commands_per_second"])
            optimized_cps = float(optimized["commands_per_second"])
            reduction = 0.0 if baseline_cps <= 0 else (1.0 - optimized_cps / baseline_cps) * 100.0
            print(json.dumps({"commands_per_second_reduction_percent": round(reduction, 2)}, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure idle Redis pressure from many Fluxera queues.",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("FLUXERA_REDIS_URL", "redis://127.0.0.1:6379/15"),
    )
    parser.add_argument("--queues", type=int, default=58)
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--poll-timeout", type=float, default=0.1)
    parser.add_argument("--optimized-max-receives", type=int, default=16)
    parser.add_argument("--optimized-idle-backoff-max", type=float, default=1.0)
    parser.add_argument(
        "--scenario",
        choices=("baseline", "optimized"),
        nargs="+",
        default=["baseline", "optimized"],
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
