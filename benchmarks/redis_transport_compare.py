from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import resource
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Optional
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DRAMATIQ_ROOT = PROJECT_ROOT.parent / "dramatiq"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(DRAMATIQ_ROOT) not in sys.path:
    sys.path.insert(0, str(DRAMATIQ_ROOT))

import fluxera


ASYNC_FANOUT = "async_fanout"
MIXED_LONG_SHORT = "mixed_long_short"
STALE_RECLAIM = "stale_reclaim"


@dataclass(slots=True)
class Scenario:
    name: str
    workload: str
    fluxera_concurrency: int
    dramatiq_thread_variants: tuple[int, ...] = ()
    messages: int = 0
    inner_concurrency: int = 0
    sleep_secs: float = 0.0
    long_messages: int = 0
    long_inner_concurrency: int = 0
    long_sleep_secs: float = 0.0
    short_messages: int = 0
    short_inner_concurrency: int = 0
    short_sleep_secs: float = 0.0
    stage_delay_secs: float = 0.0
    reclaim_messages: int = 0
    reclaim_batch_size: int = 0
    reclaim_lease_secs: float = 0.0
    compare_dramatiq: bool = True


@dataclass(slots=True)
class RunMetrics:
    framework: str
    variant: str
    scenario: str
    workload: str
    wall_time_sec: float
    cpu_time_sec: float
    peak_rss_bytes: int
    peak_thread_count: int
    event_loop_count: int
    extra_loop_threads: int
    messages_total: int
    work_units_total: int
    messages_per_sec: float
    work_units_per_sec: float
    short_drain_time_sec: Optional[float] = None
    reclaimed_total: int = 0
    reclaim_only_time_sec: Optional[float] = None


def current_rss_bytes() -> int:
    output = subprocess.check_output(["ps", "-ax", "-o", "pid=,ppid=,rss="], text=True)
    rss_by_pid: dict[int, int] = {}
    children_by_ppid: dict[int, list[int]] = {}

    for line in output.splitlines():
        raw_pid, raw_ppid, raw_rss = line.split()
        pid = int(raw_pid)
        ppid = int(raw_ppid)
        rss_by_pid[pid] = int(raw_rss) * 1024
        children_by_ppid.setdefault(ppid, []).append(pid)

    total_rss = 0
    stack = [os.getpid()]
    visited: set[int] = set()
    while stack:
        pid = stack.pop()
        if pid in visited:
            continue
        visited.add(pid)
        total_rss += rss_by_pid.get(pid, 0)
        stack.extend(children_by_ppid.get(pid, ()))

    return total_rss


def peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(peak)
    return int(peak) * 1024


def cpu_time_sec() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime + children.ru_utime + children.ru_stime


async def wait_many(delay: float, inner: int) -> None:
    await asyncio.gather(*(asyncio.sleep(delay) for _ in range(inner)))


def build_scenarios(long_io_secs: float) -> list[Scenario]:
    return [
        Scenario(
            name="redis-fanout",
            workload=ASYNC_FANOUT,
            fluxera_concurrency=256,
            dramatiq_thread_variants=(8, 32),
            messages=240,
            inner_concurrency=64,
            sleep_secs=0.005,
        ),
        Scenario(
            name="redis-mixed-long-short",
            workload=MIXED_LONG_SHORT,
            fluxera_concurrency=96,
            dramatiq_thread_variants=(8, 32),
            long_messages=12,
            long_inner_concurrency=64,
            long_sleep_secs=long_io_secs,
            short_messages=120,
            short_inner_concurrency=6,
            short_sleep_secs=0.005,
            stage_delay_secs=min(max(long_io_secs * 0.05, 0.05), 0.15),
        ),
        Scenario(
            name="redis-stale-reclaim",
            workload=STALE_RECLAIM,
            fluxera_concurrency=0,
            reclaim_messages=2048,
            reclaim_batch_size=128,
            reclaim_lease_secs=0.05,
            compare_dramatiq=False,
        ),
    ]


def should_run_scenario(scenario: Scenario, filters: list[str]) -> bool:
    if not filters:
        return True
    return any(pattern in scenario.name for pattern in filters)


async def _delete_namespace_keys(client, namespace: str) -> None:
    cursor = 0
    keys: list[bytes] = []
    while True:
        cursor, batch = await client.scan(cursor=cursor, match=f"{namespace}:*", count=512)
        keys.extend(batch)
        if cursor == 0:
            break

    if keys:
        await client.delete(*keys)


async def _run_fluxera_scenario(scenario: Scenario, redis_url: str) -> RunMetrics:
    namespace = f"fluxera-bench:{scenario.name}:{uuid4().hex}"
    broker = fluxera.RedisBroker(redis_url, namespace=namespace)
    short_completed: list[float] = []
    short_submit_at: Optional[float] = None

    peak_threads = len(threading.enumerate())
    wall_start = time.perf_counter()
    cpu_start = cpu_time_sec()

    try:
        if scenario.workload == ASYNC_FANOUT:

            @fluxera.actor(broker=broker, queue_name="fanout")
            async def fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)

            worker = fluxera.Worker(broker, concurrency=scenario.fluxera_concurrency)
            await worker.start()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.messages):
                    await fanout.send(scenario.sleep_secs, scenario.inner_concurrency)
                await broker.join(fanout.queue_name)
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                await worker.stop()

            wall = time.perf_counter() - wall_start
            cpu = cpu_time_sec() - cpu_start
            peak_rss = max(current_rss_bytes(), peak_rss_bytes())
            work_units = scenario.messages * scenario.inner_concurrency
            return RunMetrics(
                framework="fluxera",
                variant=f"c={scenario.fluxera_concurrency}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu,
                peak_rss_bytes=peak_rss,
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                event_loop_count=1,
                extra_loop_threads=0,
                messages_total=scenario.messages,
                work_units_total=work_units,
                messages_per_sec=scenario.messages / wall,
                work_units_per_sec=work_units / wall,
            )

        if scenario.workload == MIXED_LONG_SHORT:

            @fluxera.actor(broker=broker, actor_name="long_fanout", queue_name="mixed")
            async def long_fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)

            @fluxera.actor(broker=broker, actor_name="short_fanout", queue_name="mixed")
            async def short_fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)
                short_completed.append(time.perf_counter())

            worker = fluxera.Worker(broker, concurrency=scenario.fluxera_concurrency)
            await worker.start()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.long_messages):
                    await long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
                await asyncio.sleep(scenario.stage_delay_secs)
                short_submit_at = time.perf_counter()
                for _ in range(scenario.short_messages):
                    await short_fanout.send(scenario.short_sleep_secs, scenario.short_inner_concurrency)
                await broker.join(long_fanout.queue_name)
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                await worker.stop()

            wall = time.perf_counter() - wall_start
            cpu = cpu_time_sec() - cpu_start
            peak_rss = max(current_rss_bytes(), peak_rss_bytes())
            message_total = scenario.long_messages + scenario.short_messages
            work_units = (
                scenario.long_messages * scenario.long_inner_concurrency
                + scenario.short_messages * scenario.short_inner_concurrency
            )
            short_drain = None
            if short_submit_at is not None and short_completed:
                short_drain = max(short_completed) - short_submit_at

            return RunMetrics(
                framework="fluxera",
                variant=f"c={scenario.fluxera_concurrency}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu,
                peak_rss_bytes=peak_rss,
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                event_loop_count=1,
                extra_loop_threads=0,
                messages_total=message_total,
                work_units_total=work_units,
                messages_per_sec=message_total / wall,
                work_units_per_sec=work_units / wall,
                short_drain_time_sec=short_drain,
            )

        if scenario.workload == STALE_RECLAIM:

            @fluxera.actor(broker=broker, actor_name="noop", queue_name="reclaim")
            async def noop() -> None:
                return None

            broker.lease_seconds = scenario.reclaim_lease_secs
            for _ in range(scenario.reclaim_messages):
                await noop.send()

            consumer1 = await broker.open_consumer(noop.queue_name, prefetch=scenario.reclaim_batch_size)
            delivered = 0
            try:
                while delivered < scenario.reclaim_messages:
                    batch = await consumer1.receive(
                        limit=min(scenario.reclaim_batch_size, scenario.reclaim_messages - delivered),
                        timeout=1.0,
                    )
                    delivered += len(batch)

                await asyncio.sleep(scenario.reclaim_lease_secs * 1.5)
                consumer2 = await broker.open_consumer(noop.queue_name, prefetch=scenario.reclaim_batch_size)
                reclaimed = 0
                reclaim_start = time.perf_counter()
                try:
                    while reclaimed < scenario.reclaim_messages:
                        batch = await consumer2.receive(
                            limit=min(scenario.reclaim_batch_size, scenario.reclaim_messages - reclaimed),
                            timeout=1.0,
                        )
                        if not batch:
                            raise RuntimeError("Timed out while reclaiming stale deliveries.")
                        for delivery in batch:
                            if not delivery.redelivered:
                                raise RuntimeError("Expected reclaimed delivery to be marked redelivered.")
                            await consumer2.ack(delivery)
                        reclaimed += len(batch)
                finally:
                    await consumer2.close()
            finally:
                await consumer1.close()

            reclaim_wall = time.perf_counter() - reclaim_start
            wall = time.perf_counter() - wall_start
            cpu = cpu_time_sec() - cpu_start
            peak_rss = max(current_rss_bytes(), peak_rss_bytes())
            return RunMetrics(
                framework="fluxera",
                variant=f"reclaim={scenario.reclaim_batch_size}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu,
                peak_rss_bytes=peak_rss,
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                event_loop_count=1,
                extra_loop_threads=0,
                messages_total=scenario.reclaim_messages,
                work_units_total=scenario.reclaim_messages,
                messages_per_sec=scenario.reclaim_messages / wall,
                work_units_per_sec=scenario.reclaim_messages / wall,
                reclaimed_total=scenario.reclaim_messages,
                reclaim_only_time_sec=reclaim_wall,
            )

        raise RuntimeError(f"Unknown workload {scenario.workload!r}.")
    finally:
        await _delete_namespace_keys(broker.client, namespace)
        await broker.close()


def _fluxera_entry(scenario_dict: dict[str, Any], redis_url: str, result_queue) -> None:
    try:
        metrics = asyncio.run(_run_fluxera_scenario(Scenario(**scenario_dict), redis_url))
        result_queue.put(("ok", asdict(metrics)))
    except BaseException:
        result_queue.put(("error", traceback.format_exc()))


def _dramatiq_entry(scenario_dict: dict[str, Any], redis_url: str, worker_threads: int, result_queue) -> None:
    import asyncio as py_asyncio
    import redis
    import dramatiq
    from dramatiq import Worker as DramatiqWorker
    from dramatiq.asyncio import get_event_loop_thread
    from dramatiq.brokers.redis import RedisBroker as DramatiqRedisBroker
    from dramatiq.middleware.asyncio import AsyncIO

    scenario = Scenario(**scenario_dict)
    namespace = f"dramatiq-bench:{scenario.name}:{uuid4().hex}"
    broker = DramatiqRedisBroker(url=redis_url, namespace=namespace, middleware=[])
    broker.add_middleware(AsyncIO())
    dramatiq.set_broker(broker)
    short_completed: list[float] = []
    short_submit_at: Optional[float] = None
    completion_lock = threading.Lock()
    peak_threads = len(threading.enumerate())
    wall_start = time.perf_counter()
    cpu_start = cpu_time_sec()

    try:
        if scenario.workload == ASYNC_FANOUT:

            @dramatiq.actor(queue_name="fanout")
            async def fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))

            worker = DramatiqWorker(broker, worker_threads=worker_threads, worker_timeout=50)
            worker.start()
            event_loop_thread = get_event_loop_thread()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.messages):
                    fanout.send(scenario.sleep_secs, scenario.inner_concurrency)
                broker.join(fanout.queue_name)
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                worker.stop()

            wall = time.perf_counter() - wall_start
            cpu = cpu_time_sec() - cpu_start
            peak_rss = max(current_rss_bytes(), peak_rss_bytes())
            work_units = scenario.messages * scenario.inner_concurrency
            return_queue = RunMetrics(
                framework="dramatiq",
                variant=f"t={worker_threads}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu,
                peak_rss_bytes=peak_rss,
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                event_loop_count=1,
                extra_loop_threads=1 if event_loop_thread is not None else 0,
                messages_total=scenario.messages,
                work_units_total=work_units,
                messages_per_sec=scenario.messages / wall,
                work_units_per_sec=work_units / wall,
            )
            result_queue.put(("ok", asdict(return_queue)))
            return

        if scenario.workload == MIXED_LONG_SHORT:

            @dramatiq.actor(actor_name="long_fanout", queue_name="mixed")
            async def long_fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))

            @dramatiq.actor(actor_name="short_fanout", queue_name="mixed")
            async def short_fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))
                with completion_lock:
                    short_completed.append(time.perf_counter())

            worker = DramatiqWorker(broker, worker_threads=worker_threads, worker_timeout=50)
            worker.start()
            event_loop_thread = get_event_loop_thread()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.long_messages):
                    long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
                time.sleep(scenario.stage_delay_secs)
                short_submit_at = time.perf_counter()
                for _ in range(scenario.short_messages):
                    short_fanout.send(scenario.short_sleep_secs, scenario.short_inner_concurrency)
                broker.join(long_fanout.queue_name)
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                worker.stop()

            wall = time.perf_counter() - wall_start
            cpu = cpu_time_sec() - cpu_start
            peak_rss = max(current_rss_bytes(), peak_rss_bytes())
            message_total = scenario.long_messages + scenario.short_messages
            work_units = (
                scenario.long_messages * scenario.long_inner_concurrency
                + scenario.short_messages * scenario.short_inner_concurrency
            )
            short_drain = None
            if short_submit_at is not None and short_completed:
                short_drain = max(short_completed) - short_submit_at

            return_queue = RunMetrics(
                framework="dramatiq",
                variant=f"t={worker_threads}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu,
                peak_rss_bytes=peak_rss,
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                event_loop_count=1,
                extra_loop_threads=1 if event_loop_thread is not None else 0,
                messages_total=message_total,
                work_units_total=work_units,
                messages_per_sec=message_total / wall,
                work_units_per_sec=work_units / wall,
                short_drain_time_sec=short_drain,
            )
            result_queue.put(("ok", asdict(return_queue)))
            return

        raise RuntimeError(f"Unsupported Dramatiq workload {scenario.workload!r}.")
    except BaseException:
        result_queue.put(("error", traceback.format_exc()))
    finally:
        redis_client = redis.Redis.from_url(redis_url)
        keys = list(redis_client.scan_iter(f"{namespace}:*"))
        if keys:
            redis_client.delete(*keys)
        redis_client.close()


def run_once(framework: str, scenario: Scenario, redis_url: str, worker_threads: Optional[int] = None) -> RunMetrics:
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()

    if framework == "fluxera":
        args = (asdict(scenario), redis_url, result_queue)
        target = _fluxera_entry
    elif framework == "dramatiq":
        if worker_threads is None:
            raise ValueError("worker_threads must be provided for Dramatiq runs.")
        args = (asdict(scenario), redis_url, worker_threads, result_queue)
        target = _dramatiq_entry
    else:
        raise RuntimeError(f"Unknown framework {framework!r}.")

    process = ctx.Process(target=target, args=args, name=f"{framework}-{scenario.name}")
    process.start()
    status, payload = result_queue.get()
    process.join()
    if process.exitcode not in (0, None):
        raise RuntimeError(f"{framework} benchmark process exited with code {process.exitcode}.")
    if status != "ok":
        raise RuntimeError(f"{framework} benchmark failed:\n{payload}")
    return RunMetrics(**payload)


def summarize_runs(runs: list[RunMetrics]) -> dict[str, Any]:
    return {
        "framework": runs[0].framework,
        "variant": runs[0].variant,
        "scenario": runs[0].scenario,
        "workload": runs[0].workload,
        "runs": len(runs),
        "wall_time_sec": mean(run.wall_time_sec for run in runs),
        "cpu_time_sec": mean(run.cpu_time_sec for run in runs),
        "peak_rss_mib": mean(run.peak_rss_bytes for run in runs) / (1024 * 1024),
        "peak_thread_count": mean(run.peak_thread_count for run in runs),
        "event_loop_count": mean(run.event_loop_count for run in runs),
        "extra_loop_threads": mean(run.extra_loop_threads for run in runs),
        "messages_total": runs[0].messages_total,
        "work_units_total": runs[0].work_units_total,
        "messages_per_sec": mean(run.messages_per_sec for run in runs),
        "work_units_per_sec": mean(run.work_units_per_sec for run in runs),
        "short_drain_time_sec": mean(
            run.short_drain_time_sec for run in runs if run.short_drain_time_sec is not None
        ) if any(run.short_drain_time_sec is not None for run in runs) else None,
        "reclaimed_total": runs[0].reclaimed_total,
        "reclaim_only_time_sec": mean(
            run.reclaim_only_time_sec for run in runs if run.reclaim_only_time_sec is not None
        ) if any(run.reclaim_only_time_sec is not None for run in runs) else None,
    }


def render_summary(summary: dict[str, Any]) -> str:
    parts = [
        f"{summary['framework']:<8}[{summary['variant']:<12}]",
        f"runs={summary['runs']}",
        f"wall={summary['wall_time_sec']:.3f}s",
        f"cpu={summary['cpu_time_sec']:.3f}s",
        f"peak_rss={summary['peak_rss_mib']:.2f}MiB",
        f"peak_threads={summary['peak_thread_count']:.1f}",
        f"loops={summary['event_loop_count']:.1f}",
        f"extra_loop_threads={summary['extra_loop_threads']:.1f}",
        f"msg/s={summary['messages_per_sec']:.1f}",
        f"work/s={summary['work_units_per_sec']:.1f}",
    ]
    if summary["short_drain_time_sec"] is not None:
        parts.append(f"short_drain={summary['short_drain_time_sec']:.3f}s")
    if summary["reclaim_only_time_sec"] is not None:
        parts.append(
            f"reclaim={summary['reclaimed_total']} in {summary['reclaim_only_time_sec']:.3f}s"
        )
    return " ".join(parts)


def render_ratio(base: dict[str, Any], other: dict[str, Any]) -> str:
    parts = [
        f"{other['framework']}[{other['variant']}]/fluxera",
        f"wall={other['wall_time_sec'] / base['wall_time_sec']:.2f}x",
        f"peak_rss={other['peak_rss_mib'] / base['peak_rss_mib']:.2f}x",
        f"threads={other['peak_thread_count'] / max(base['peak_thread_count'], 1e-9):.2f}x",
    ]
    if base["short_drain_time_sec"] is not None and other["short_drain_time_sec"] is not None:
        parts.append(f"short_drain={other['short_drain_time_sec'] / base['short_drain_time_sec']:.2f}x")
    return " ".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Fluxera Redis transport before/after hardening and compare with Dramatiq.")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/15")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--long-io-secs", type=float, default=2.0)
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Only run scenarios whose name contains this substring. Can be repeated.",
    )
    parser.add_argument(
        "--dramatiq-thread",
        action="append",
        type=int,
        default=[],
        help="Override Dramatiq worker thread variants for comparable scenarios. Can be repeated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenarios = [scenario for scenario in build_scenarios(args.long_io_secs) if should_run_scenario(scenario, args.scenario)]
    if not scenarios:
        print("No scenarios matched the provided filters.")
        return 1

    print(f"Redis URL: {args.redis_url}")
    print(f"Repeats: {args.repeat}")
    print(f"Long I/O: {args.long_io_secs:.3f}s")

    for scenario in scenarios:
        print()
        print(f"Scenario: {scenario.name}")
        fluxera_runs = [run_once("fluxera", scenario, args.redis_url) for _ in range(args.repeat)]
        fluxera_summary = summarize_runs(fluxera_runs)
        print("  " + render_summary(fluxera_summary))

        if not scenario.compare_dramatiq:
            continue

        thread_variants = tuple(args.dramatiq_thread) or scenario.dramatiq_thread_variants
        dramatiq_summaries = []
        for worker_threads in thread_variants:
            runs = [run_once("dramatiq", scenario, args.redis_url, worker_threads=worker_threads) for _ in range(args.repeat)]
            summary = summarize_runs(runs)
            dramatiq_summaries.append(summary)
            print("  " + render_summary(summary))

        for summary in dramatiq_summaries:
            print("  " + render_ratio(fluxera_summary, summary))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
