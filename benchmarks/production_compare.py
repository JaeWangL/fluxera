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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DRAMATIQ_ROOT = PROJECT_ROOT.parent / "dramatiq"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(DRAMATIQ_ROOT) not in sys.path:
    sys.path.insert(0, str(DRAMATIQ_ROOT))

import fluxera


ASYNC_FANOUT = "async_fanout"
CPU_BOUND = "cpu_bound"
MIXED_LONG_SHORT = "mixed_long_short"
MIXED_LONG_CPU = "mixed_long_cpu"


@dataclass(slots=True)
class Scenario:
    name: str
    workload: str
    worker_processes: int
    fluxera_concurrency: int = 256
    fluxera_thread_concurrency: Optional[int] = None
    fluxera_process_concurrency: Optional[int] = None
    dramatiq_thread_variants: tuple[int, ...] = (8,)
    messages_per_worker: int = 0
    inner_concurrency: int = 0
    sleep_secs: float = 0.0
    cpu_iterations: int = 0
    cpu_messages_per_worker: int = 0
    long_messages_per_worker: int = 0
    long_inner_concurrency: int = 0
    long_sleep_secs: float = 0.0
    short_messages_per_worker: int = 0
    short_inner_concurrency: int = 0
    short_sleep_secs: float = 0.0
    stage_delay_secs: float = 0.0


@dataclass(slots=True)
class WorkerMetrics:
    framework: str
    variant: str
    scenario: str
    workload: str
    pid: int
    wall_time_sec: float
    cpu_time_sec: float
    startup_rss_bytes: int
    peak_rss_bytes: int
    startup_thread_count: int
    peak_thread_count: int
    event_loop_count: int
    extra_loop_threads: int
    messages_submitted: int
    work_units_total: int
    short_messages_submitted: int = 0
    short_drain_time_sec: Optional[float] = None
    cpu_drain_time_sec: Optional[float] = None
    long_tail_time_sec: Optional[float] = None


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


def crunch_cpu(iterations: int) -> int:
    acc = 0
    for index in range(iterations):
        acc = ((acc * 1664525) + index + 1013904223) & 0xFFFFFFFF
    return acc


async def wait_many(delay: float, inner: int) -> None:
    await asyncio.gather(*(asyncio.sleep(delay) for _ in range(inner)))


def process_cpu_task(iterations: int) -> None:
    crunch_cpu(iterations)


def process_cpu_finished_at(iterations: int) -> float:
    crunch_cpu(iterations)
    return time.time()


def describe_scenario(scenario: Scenario) -> str:
    if scenario.workload == ASYNC_FANOUT:
        return (
            f"workers={scenario.worker_processes} "
            f"messages/worker={scenario.messages_per_worker} "
            f"inner_gather={scenario.inner_concurrency} "
            f"sleep={scenario.sleep_secs:.3f}s"
        )

    if scenario.workload == CPU_BOUND:
        return (
            f"workers={scenario.worker_processes} "
            f"messages/worker={scenario.messages_per_worker} "
            f"cpu_iterations={scenario.cpu_iterations}"
        )

    if scenario.workload == MIXED_LONG_SHORT:
        return (
            f"workers={scenario.worker_processes} "
            f"long_msgs/worker={scenario.long_messages_per_worker} "
            f"long_gather={scenario.long_inner_concurrency} "
            f"long_sleep={scenario.long_sleep_secs:.3f}s "
            f"short_msgs/worker={scenario.short_messages_per_worker} "
            f"short_gather={scenario.short_inner_concurrency} "
            f"short_sleep={scenario.short_sleep_secs:.3f}s"
        )

    if scenario.workload == MIXED_LONG_CPU:
        return (
            f"workers={scenario.worker_processes} "
            f"long_msgs/worker={scenario.long_messages_per_worker} "
            f"long_gather={scenario.long_inner_concurrency} "
            f"long_sleep={scenario.long_sleep_secs:.3f}s "
            f"cpu_msgs/worker={scenario.cpu_messages_per_worker} "
            f"cpu_iterations={scenario.cpu_iterations}"
        )

    raise RuntimeError(f"Unknown workload {scenario.workload!r}.")


def build_scenarios(profile: str, long_io_secs: Optional[float]) -> list[Scenario]:
    if long_io_secs is None:
        defaults = {
            "smoke": 3.0,
            "soak": 15.0,
            "production": 180.0,
        }
        long_io_secs = defaults[profile]

    stage_delay_secs = min(max(long_io_secs * 0.05, 0.05), 0.25)

    return [
        Scenario(
            name="single-worker-fanout250",
            workload=ASYNC_FANOUT,
            worker_processes=1,
            messages_per_worker=80,
            inner_concurrency=250,
            sleep_secs=0.01,
            fluxera_concurrency=256,
            dramatiq_thread_variants=(8, 32),
        ),
        Scenario(
            name="single-worker-cpu-bound",
            workload=CPU_BOUND,
            worker_processes=1,
            messages_per_worker=120,
            cpu_iterations=600_000,
            fluxera_concurrency=32,
            fluxera_process_concurrency=4,
            dramatiq_thread_variants=(8, 32),
        ),
        Scenario(
            name="thirty-workers-cpu-bound",
            workload=CPU_BOUND,
            worker_processes=30,
            messages_per_worker=12,
            cpu_iterations=400_000,
            fluxera_concurrency=32,
            fluxera_process_concurrency=2,
            dramatiq_thread_variants=(8, 32),
        ),
        Scenario(
            name="single-worker-mixed-long-io",
            workload=MIXED_LONG_SHORT,
            worker_processes=1,
            fluxera_concurrency=64,
            dramatiq_thread_variants=(8, 32, 64),
            long_messages_per_worker=16,
            long_inner_concurrency=100,
            long_sleep_secs=long_io_secs,
            short_messages_per_worker=120,
            short_inner_concurrency=10,
            short_sleep_secs=0.01,
            stage_delay_secs=stage_delay_secs,
        ),
        Scenario(
            name="single-worker-mixed-long-io-plus-cpu",
            workload=MIXED_LONG_CPU,
            worker_processes=1,
            fluxera_concurrency=64,
            fluxera_process_concurrency=4,
            dramatiq_thread_variants=(8, 32, 64),
            long_messages_per_worker=16,
            long_inner_concurrency=100,
            long_sleep_secs=long_io_secs,
            cpu_messages_per_worker=24,
            cpu_iterations=600_000,
            stage_delay_secs=stage_delay_secs,
        ),
        Scenario(
            name="thirty-workers-mixed-long-io",
            workload=MIXED_LONG_SHORT,
            worker_processes=30,
            fluxera_concurrency=64,
            dramatiq_thread_variants=(8, 32),
            long_messages_per_worker=10,
            long_inner_concurrency=100,
            long_sleep_secs=long_io_secs,
            short_messages_per_worker=16,
            short_inner_concurrency=10,
            short_sleep_secs=0.01,
            stage_delay_secs=stage_delay_secs,
        ),
    ]


async def _run_fluxera_scenario(scenario: Scenario) -> WorkerMetrics:
    broker = fluxera.StubBroker()
    fluxera.set_broker(broker)

    short_completed: list[float] = []
    long_completed: list[float] = []
    short_submit_at: Optional[float] = None
    cpu_submit_at: Optional[float] = None

    messages_submitted = 0
    work_units_total = 0
    queue_names: set[str] = set()

    if scenario.workload == ASYNC_FANOUT:

        @fluxera.actor(queue_name="fanout")
        async def fanout(delay: float, inner: int) -> None:
            await wait_many(delay, inner)

        queue_names.add(fanout.queue_name)

        async def submit_workload() -> None:
            nonlocal messages_submitted, work_units_total
            for _ in range(scenario.messages_per_worker):
                await fanout.send(scenario.sleep_secs, scenario.inner_concurrency)
            messages_submitted = scenario.messages_per_worker
            work_units_total = scenario.messages_per_worker * scenario.inner_concurrency

    elif scenario.workload == CPU_BOUND:
        crunch = fluxera.actor(
            broker=broker,
            actor_name="process_crunch",
            queue_name="cpu",
            execution="process",
        )(process_cpu_task)

        queue_names.add(crunch.queue_name)

        async def submit_workload() -> None:
            nonlocal messages_submitted, work_units_total
            for _ in range(scenario.messages_per_worker):
                await crunch.send(scenario.cpu_iterations)
            messages_submitted = scenario.messages_per_worker
            work_units_total = scenario.messages_per_worker * scenario.cpu_iterations

    elif scenario.workload == MIXED_LONG_SHORT:

        @fluxera.actor(actor_name="long_fanout", queue_name="mixed")
        async def long_fanout(delay: float, inner: int) -> None:
            await wait_many(delay, inner)
            long_completed.append(time.perf_counter())

        @fluxera.actor(actor_name="short_fanout", queue_name="mixed")
        async def short_fanout(delay: float, inner: int) -> None:
            await wait_many(delay, inner)
            short_completed.append(time.perf_counter())

        queue_names.add(long_fanout.queue_name)

        async def submit_workload() -> None:
            nonlocal messages_submitted, work_units_total, short_submit_at
            for _ in range(scenario.long_messages_per_worker):
                await long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
            await asyncio.sleep(scenario.stage_delay_secs)
            short_submit_at = time.perf_counter()
            for _ in range(scenario.short_messages_per_worker):
                await short_fanout.send(scenario.short_sleep_secs, scenario.short_inner_concurrency)
            messages_submitted = scenario.long_messages_per_worker + scenario.short_messages_per_worker
            work_units_total = (
                scenario.long_messages_per_worker * scenario.long_inner_concurrency
                + scenario.short_messages_per_worker * scenario.short_inner_concurrency
            )

    elif scenario.workload == MIXED_LONG_CPU:

        @fluxera.actor(actor_name="long_fanout", queue_name="mixed-cpu")
        async def long_fanout(delay: float, inner: int) -> None:
            await wait_many(delay, inner)
            long_completed.append(time.time())

        cpu_task = fluxera.actor(
            broker=broker,
            actor_name="cpu_task",
            queue_name="mixed-cpu",
            execution="process",
        )(process_cpu_finished_at)

        queue_names.add(long_fanout.queue_name)

        async def submit_workload() -> None:
            nonlocal messages_submitted, work_units_total, cpu_submit_at
            for _ in range(scenario.long_messages_per_worker):
                await long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
            await asyncio.sleep(scenario.stage_delay_secs)
            cpu_submit_at = time.time()
            for _ in range(scenario.cpu_messages_per_worker):
                await cpu_task.send(scenario.cpu_iterations)
            messages_submitted = scenario.long_messages_per_worker + scenario.cpu_messages_per_worker
            work_units_total = (
                scenario.long_messages_per_worker * scenario.long_inner_concurrency
                + scenario.cpu_messages_per_worker * scenario.cpu_iterations
            )

    else:
        raise RuntimeError(f"Unknown workload {scenario.workload!r}.")

    worker = fluxera.Worker(
        broker,
        async_concurrency=scenario.fluxera_concurrency,
        thread_concurrency=scenario.fluxera_thread_concurrency,
        process_concurrency=scenario.fluxera_process_concurrency,
    )
    wall_start = time.perf_counter()
    cpu_start = cpu_time_sec()
    await worker.start()
    startup_rss = current_rss_bytes()
    startup_thread_count = len(threading.enumerate())
    peak_thread_count = startup_thread_count

    await submit_workload()
    peak_thread_count = max(peak_thread_count, len(threading.enumerate()))

    for queue_name in queue_names:
        await broker.join(queue_name)

    peak_thread_count = max(peak_thread_count, len(threading.enumerate()))
    peak_rss = max(current_rss_bytes(), peak_rss_bytes(), startup_rss)
    await worker.stop()
    peak_thread_count = max(peak_thread_count, len(threading.enumerate()))

    short_drain_time_sec = None
    cpu_drain_time_sec = None
    long_tail_time_sec = None
    if short_submit_at is not None and short_completed:
        short_drain_time_sec = max(short_completed) - short_submit_at
    if cpu_submit_at is not None:
        cpu_completed = [
            record.result
            for record in worker.records.values()
            if record.delivery.actor_name == "cpu_task" and record.result is not None
        ]
        if cpu_completed:
            cpu_drain_time_sec = max(cpu_completed) - cpu_submit_at
    if short_submit_at is not None and long_completed:
        long_tail_time_sec = max(0.0, max(long_completed) - short_submit_at)
    elif cpu_submit_at is not None and long_completed:
        long_tail_time_sec = max(0.0, max(long_completed) - cpu_submit_at)

    return WorkerMetrics(
        framework="fluxera",
        variant=f"c={scenario.fluxera_concurrency}",
        scenario=scenario.name,
        workload=scenario.workload,
        pid=os.getpid(),
        wall_time_sec=time.perf_counter() - wall_start,
        cpu_time_sec=cpu_time_sec() - cpu_start,
        startup_rss_bytes=startup_rss,
        peak_rss_bytes=peak_rss,
        startup_thread_count=startup_thread_count,
        peak_thread_count=peak_thread_count,
        event_loop_count=1,
        extra_loop_threads=0,
        messages_submitted=messages_submitted,
        work_units_total=work_units_total,
        short_messages_submitted=scenario.short_messages_per_worker,
        short_drain_time_sec=short_drain_time_sec,
        cpu_drain_time_sec=cpu_drain_time_sec,
        long_tail_time_sec=long_tail_time_sec,
    )


def _fluxera_entry(scenario_dict: dict[str, Any], result_queue) -> None:
    try:
        metrics = asyncio.run(_run_fluxera_scenario(Scenario(**scenario_dict)))
        result_queue.put(("ok", asdict(metrics)))
    except BaseException:
        result_queue.put(("error", traceback.format_exc()))


def _dramatiq_entry(scenario_dict: dict[str, Any], worker_threads: int, result_queue) -> None:
    import asyncio as py_asyncio
    import dramatiq
    from dramatiq import Worker as DramatiqWorker
    from dramatiq.asyncio import get_event_loop_thread
    from dramatiq.brokers.stub import StubBroker as DramatiqStubBroker
    from dramatiq.middleware.asyncio import AsyncIO

    try:
        scenario = Scenario(**scenario_dict)
        short_completed: list[float] = []
        long_completed: list[float] = []
        completion_lock = threading.Lock()
        short_submit_at: Optional[float] = None
        cpu_submit_at: Optional[float] = None
        cpu_completed: list[float] = []

        messages_submitted = 0
        work_units_total = 0
        queue_names: set[str] = set()

        broker = DramatiqStubBroker(middleware=[])
        async_enabled = scenario.workload in {ASYNC_FANOUT, MIXED_LONG_SHORT, MIXED_LONG_CPU}
        if async_enabled:
            broker.add_middleware(AsyncIO())
        dramatiq.set_broker(broker)

        if scenario.workload == ASYNC_FANOUT:

            @dramatiq.actor(queue_name="fanout")
            async def fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))

            queue_names.add(fanout.queue_name)

            def submit_workload() -> None:
                nonlocal messages_submitted, work_units_total
                for _ in range(scenario.messages_per_worker):
                    fanout.send(scenario.sleep_secs, scenario.inner_concurrency)
                messages_submitted = scenario.messages_per_worker
                work_units_total = scenario.messages_per_worker * scenario.inner_concurrency

        elif scenario.workload == CPU_BOUND:

            @dramatiq.actor(queue_name="cpu")
            def crunch(iterations: int) -> None:
                crunch_cpu(iterations)

            queue_names.add(crunch.queue_name)

            def submit_workload() -> None:
                nonlocal messages_submitted, work_units_total
                for _ in range(scenario.messages_per_worker):
                    crunch.send(scenario.cpu_iterations)
                messages_submitted = scenario.messages_per_worker
                work_units_total = scenario.messages_per_worker * scenario.cpu_iterations

        elif scenario.workload == MIXED_LONG_SHORT:

            @dramatiq.actor(actor_name="long_fanout", queue_name="mixed")
            async def long_fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))
                with completion_lock:
                    long_completed.append(time.perf_counter())

            @dramatiq.actor(actor_name="short_fanout", queue_name="mixed")
            async def short_fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))
                with completion_lock:
                    short_completed.append(time.perf_counter())

            queue_names.add(long_fanout.queue_name)

            def submit_workload() -> None:
                nonlocal messages_submitted, work_units_total, short_submit_at
                for _ in range(scenario.long_messages_per_worker):
                    long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
                time.sleep(scenario.stage_delay_secs)
                short_submit_at = time.perf_counter()
                for _ in range(scenario.short_messages_per_worker):
                    short_fanout.send(scenario.short_sleep_secs, scenario.short_inner_concurrency)
                messages_submitted = scenario.long_messages_per_worker + scenario.short_messages_per_worker
                work_units_total = (
                    scenario.long_messages_per_worker * scenario.long_inner_concurrency
                    + scenario.short_messages_per_worker * scenario.short_inner_concurrency
                )

        elif scenario.workload == MIXED_LONG_CPU:

            @dramatiq.actor(actor_name="long_fanout", queue_name="mixed-cpu")
            async def long_fanout(delay: float, inner: int) -> None:
                await py_asyncio.gather(*(py_asyncio.sleep(delay) for _ in range(inner)))
                with completion_lock:
                    long_completed.append(time.time())

            @dramatiq.actor(actor_name="cpu_task", queue_name="mixed-cpu")
            def cpu_task(iterations: int) -> None:
                crunch_cpu(iterations)
                with completion_lock:
                    cpu_completed.append(time.time())

            queue_names.add(long_fanout.queue_name)

            def submit_workload() -> None:
                nonlocal messages_submitted, work_units_total, cpu_submit_at
                for _ in range(scenario.long_messages_per_worker):
                    long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
                time.sleep(scenario.stage_delay_secs)
                cpu_submit_at = time.time()
                for _ in range(scenario.cpu_messages_per_worker):
                    cpu_task.send(scenario.cpu_iterations)
                messages_submitted = scenario.long_messages_per_worker + scenario.cpu_messages_per_worker
                work_units_total = (
                    scenario.long_messages_per_worker * scenario.long_inner_concurrency
                    + scenario.cpu_messages_per_worker * scenario.cpu_iterations
                )

        else:
            raise RuntimeError(f"Unknown workload {scenario.workload!r}.")

        wall_start = time.perf_counter()
        cpu_start = cpu_time_sec()
        worker = DramatiqWorker(broker, worker_threads=worker_threads, worker_timeout=50)
        worker.start()
        startup_rss = current_rss_bytes()
        startup_thread_count = len(threading.enumerate())
        peak_thread_count = startup_thread_count
        event_loop_thread = get_event_loop_thread() if async_enabled else None

        submit_workload()
        peak_thread_count = max(peak_thread_count, len(threading.enumerate()))

        for queue_name in queue_names:
            broker.join(queue_name)

        peak_thread_count = max(peak_thread_count, len(threading.enumerate()))
        peak_rss = max(current_rss_bytes(), peak_rss_bytes(), startup_rss)
        worker.stop()
        peak_thread_count = max(peak_thread_count, len(threading.enumerate()))

        short_drain_time_sec = None
        cpu_drain_time_sec = None
        long_tail_time_sec = None
        if short_submit_at is not None and short_completed:
            short_drain_time_sec = max(short_completed) - short_submit_at
        if cpu_submit_at is not None and cpu_completed:
            cpu_drain_time_sec = max(cpu_completed) - cpu_submit_at
        if short_submit_at is not None and long_completed:
            long_tail_time_sec = max(0.0, max(long_completed) - short_submit_at)
        elif cpu_submit_at is not None and long_completed:
            long_tail_time_sec = max(0.0, max(long_completed) - cpu_submit_at)

        metrics = WorkerMetrics(
            framework="dramatiq",
            variant=f"t={worker_threads}",
            scenario=scenario.name,
            workload=scenario.workload,
            pid=os.getpid(),
            wall_time_sec=time.perf_counter() - wall_start,
            cpu_time_sec=cpu_time_sec() - cpu_start,
            startup_rss_bytes=startup_rss,
            peak_rss_bytes=peak_rss,
            startup_thread_count=startup_thread_count,
            peak_thread_count=peak_thread_count,
            event_loop_count=1 if event_loop_thread is not None else 0,
            extra_loop_threads=1 if event_loop_thread is not None else 0,
            messages_submitted=messages_submitted,
            work_units_total=work_units_total,
            short_messages_submitted=scenario.short_messages_per_worker,
            short_drain_time_sec=short_drain_time_sec,
            cpu_drain_time_sec=cpu_drain_time_sec,
            long_tail_time_sec=long_tail_time_sec,
        )
        result_queue.put(("ok", asdict(metrics)))
    except BaseException:
        result_queue.put(("error", traceback.format_exc()))


def run_framework(framework: str, scenario: Scenario, worker_threads: int | None = None) -> dict[str, Any]:
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    if framework == "fluxera":
        target = _fluxera_entry
        args = (asdict(scenario), result_queue)
        variant = f"c={scenario.fluxera_concurrency}"
    else:
        selected_threads = worker_threads or scenario.dramatiq_thread_variants[0]
        target = _dramatiq_entry
        args = (asdict(scenario), selected_threads, result_queue)
        variant = f"t={selected_threads}"

    processes = [
        ctx.Process(target=target, args=args, name=f"{framework}-{variant}-{scenario.name}-{index}")
        for index in range(scenario.worker_processes)
    ]

    wall_start = time.perf_counter()
    for process in processes:
        process.start()

    results = []
    for _ in processes:
        status, payload = result_queue.get()
        if status != "ok":
            for process in processes:
                process.kill()
            raise RuntimeError(f"{framework} benchmark worker failed:\n{payload}")
        results.append(WorkerMetrics(**payload))

    for process in processes:
        process.join()

    aggregate_wall = time.perf_counter() - wall_start
    messages_total = sum(result.messages_submitted for result in results)
    work_units_total = sum(result.work_units_total for result in results)
    short_drain_values = [result.short_drain_time_sec for result in results if result.short_drain_time_sec is not None]
    cpu_drain_values = [result.cpu_drain_time_sec for result in results if result.cpu_drain_time_sec is not None]
    long_tail_values = [result.long_tail_time_sec for result in results if result.long_tail_time_sec is not None]

    return {
        "framework": framework,
        "variant": variant,
        "scenario": scenario.name,
        "workload": scenario.workload,
        "workers": len(results),
        "aggregate_wall_time_sec": aggregate_wall,
        "aggregate_cpu_time_sec": sum(result.cpu_time_sec for result in results),
        "cpu_to_wall_ratio": (sum(result.cpu_time_sec for result in results) / aggregate_wall) if aggregate_wall else 0.0,
        "avg_worker_wall_time_sec": mean(result.wall_time_sec for result in results),
        "avg_startup_rss_mib": mean(result.startup_rss_bytes for result in results) / (1024 * 1024),
        "avg_peak_rss_mib": mean(result.peak_rss_bytes for result in results) / (1024 * 1024),
        "max_peak_rss_mib": max(result.peak_rss_bytes for result in results) / (1024 * 1024),
        "avg_startup_thread_count": mean(result.startup_thread_count for result in results),
        "avg_peak_thread_count": mean(result.peak_thread_count for result in results),
        "total_peak_thread_count": sum(result.peak_thread_count for result in results),
        "event_loops_total": sum(result.event_loop_count for result in results),
        "extra_loop_threads_total": sum(result.extra_loop_threads for result in results),
        "messages_total": messages_total,
        "work_units_total": work_units_total,
        "messages_per_sec": (messages_total / aggregate_wall) if messages_total else 0.0,
        "work_units_per_sec": (work_units_total / aggregate_wall) if work_units_total else 0.0,
        "avg_short_drain_time_sec": mean(short_drain_values) if short_drain_values else None,
        "max_short_drain_time_sec": max(short_drain_values) if short_drain_values else None,
        "avg_cpu_drain_time_sec": mean(cpu_drain_values) if cpu_drain_values else None,
        "max_cpu_drain_time_sec": max(cpu_drain_values) if cpu_drain_values else None,
        "avg_long_tail_time_sec": mean(long_tail_values) if long_tail_values else None,
        "max_long_tail_time_sec": max(long_tail_values) if long_tail_values else None,
    }


def render_result(result: dict[str, Any]) -> str:
    parts = [
        f"{result['framework']:<8}[{result['variant']:<5}]",
        f"wall={result['aggregate_wall_time_sec']:.3f}s",
        f"cpu={result['aggregate_cpu_time_sec']:.3f}s",
        f"cpu/wall={result['cpu_to_wall_ratio']:.2f}x",
        f"peak_rss(avg/max)={result['avg_peak_rss_mib']:.2f}/{result['max_peak_rss_mib']:.2f}MiB",
        f"threads(avg_start/avg_peak/total_peak)={result['avg_startup_thread_count']:.1f}/{result['avg_peak_thread_count']:.1f}/{result['total_peak_thread_count']}",
        f"loops={result['event_loops_total']}",
        f"extra_loop_threads={result['extra_loop_threads_total']}",
        f"msg/s={result['messages_per_sec']:.1f}",
        f"work/s={result['work_units_per_sec']:.1f}",
    ]

    if result["avg_short_drain_time_sec"] is not None:
        parts.append(
            "short_drain(avg/max)="
            f"{result['avg_short_drain_time_sec']:.3f}/{result['max_short_drain_time_sec']:.3f}s"
        )
    if result["avg_cpu_drain_time_sec"] is not None:
        parts.append(
            "cpu_drain(avg/max)="
            f"{result['avg_cpu_drain_time_sec']:.3f}/{result['max_cpu_drain_time_sec']:.3f}s"
        )
    if result["avg_long_tail_time_sec"] is not None:
        parts.append(
            "long_tail(avg/max)="
            f"{result['avg_long_tail_time_sec']:.3f}/{result['max_long_tail_time_sec']:.3f}s"
        )

    return " ".join(parts)


def render_ratios(base_result: dict[str, Any], compare_result: dict[str, Any]) -> str:
    parts = [
        f"dramatiq[{compare_result['variant']}]/fluxera",
        f"wall={compare_result['aggregate_wall_time_sec'] / base_result['aggregate_wall_time_sec']:.2f}x",
        f"avg_peak_rss={compare_result['avg_peak_rss_mib'] / base_result['avg_peak_rss_mib']:.2f}x",
        f"peak_threads={compare_result['total_peak_thread_count'] / max(base_result['total_peak_thread_count'], 1):.2f}x",
    ]
    if base_result["avg_short_drain_time_sec"] is not None and compare_result["avg_short_drain_time_sec"] is not None:
        parts.append(
            "short_drain="
            f"{compare_result['avg_short_drain_time_sec'] / base_result['avg_short_drain_time_sec']:.2f}x"
        )
    if base_result["avg_cpu_drain_time_sec"] is not None and compare_result["avg_cpu_drain_time_sec"] is not None:
        parts.append(
            "cpu_drain="
            f"{compare_result['avg_cpu_drain_time_sec'] / base_result['avg_cpu_drain_time_sec']:.2f}x"
        )

    return " ".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Fluxera against Dramatiq under production-shaped workloads.")
    parser.add_argument(
        "--profile",
        choices=("smoke", "soak", "production"),
        default="smoke",
        help="Scenario profile. 'production' uses 180s long I/O waits.",
    )
    parser.add_argument(
        "--long-io-secs",
        type=float,
        default=None,
        help="Override the long I/O wait duration used by mixed long/short scenarios.",
    )
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
        help="Override Dramatiq worker thread variants for all selected scenarios. Can be repeated.",
    )
    return parser.parse_args()


def should_run_scenario(scenario: Scenario, filters: list[str]) -> bool:
    if not filters:
        return True
    return any(pattern in scenario.name for pattern in filters)


def main() -> int:
    args = parse_args()
    scenarios = [scenario for scenario in build_scenarios(args.profile, args.long_io_secs) if should_run_scenario(scenario, args.scenario)]

    if not scenarios:
        print("No scenarios matched the provided filters.")
        return 1

    print(
        "Profile: "
        f"{args.profile} "
        f"(long_io={build_scenarios(args.profile, args.long_io_secs)[-1].long_sleep_secs:.3f}s for mixed scenarios)"
    )

    for scenario in scenarios:
        print()
        print(f"Scenario: {scenario.name}")
        print("  " + describe_scenario(scenario))
        fluxera_result = run_framework("fluxera", scenario)
        print("  " + render_result(fluxera_result))

        dramatiq_variants = tuple(args.dramatiq_thread) or scenario.dramatiq_thread_variants
        dramatiq_results = [
            run_framework("dramatiq", scenario, worker_threads=worker_threads)
            for worker_threads in dramatiq_variants
        ]
        for dramatiq_result in dramatiq_results:
            print("  " + render_result(dramatiq_result))
        for dramatiq_result in dramatiq_results:
            print("  " + render_ratios(fluxera_result, dramatiq_result))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
