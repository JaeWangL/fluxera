# Redis vs RabbitMQ 전송 계층 벤치마크.
#
# redis_transport_compare.py 하네스를 미러링하되 비교 축을
# (fluxera vs dramatiq)가 아니라 (RedisBroker vs RabbitMQBroker)로 바꿨다.
#
# 공정성 노트: broker.join()은 브로커마다 구현이 다르다 (RabbitMQ join은
# management 통계 지연을 흡수하기 위한 idle grace ~1초가 포함됨). join 차이가
# 벤치마크를 오염시키지 않도록, 완료 판정은 액터가 증가시키는 인프로세스
# 카운터로 한다. wall 시간은 전송 시작 → 마지막 메시지 처리 완료까지다.
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import multiprocessing as mp
import os
import resource
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Optional
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import fluxera


ASYNC_FANOUT = "async_fanout"
MIXED_LONG_SHORT = "mixed_long_short"
ENQUEUE_ONLY = "enqueue_only"
ENQUEUE_CONCURRENT = "enqueue_concurrent"

TRANSPORTS = ("redis", "rabbitmq")


@dataclass(slots=True)
class Scenario:
    name: str
    workload: str
    concurrency: int
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
    enqueue_batch: int = 0


@dataclass(slots=True)
class RunMetrics:
    transport: str
    variant: str
    scenario: str
    workload: str
    wall_time_sec: float
    cpu_time_sec: float
    peak_rss_bytes: int
    peak_thread_count: int
    messages_total: int
    work_units_total: int
    messages_per_sec: float
    work_units_per_sec: float
    enqueue_time_sec: Optional[float] = None
    short_drain_time_sec: Optional[float] = None


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


async def wait_until(predicate, *, timeout: float, interval: float = 0.005) -> None:
    deadline = time.perf_counter() + timeout
    while not predicate():
        if time.perf_counter() >= deadline:
            raise RuntimeError("Benchmark timed out while draining messages.")
        await asyncio.sleep(interval)


def build_scenarios(long_io_secs: float) -> list[Scenario]:
    return [
        Scenario(
            name="enqueue-only",
            workload=ENQUEUE_ONLY,
            concurrency=0,
            messages=2000,
        ),
        Scenario(
            # 동시 발행: RabbitMQ는 confirm 대기가 겹쳐 자동 배칭되고,
            # Redis는 커넥션 풀로 병렬화된다
            name="enqueue-concurrent",
            workload=ENQUEUE_CONCURRENT,
            concurrency=0,
            messages=2000,
            enqueue_batch=100,
        ),
        Scenario(
            name="fanout",
            workload=ASYNC_FANOUT,
            concurrency=256,
            messages=240,
            inner_concurrency=64,
            sleep_secs=0.005,
        ),
        Scenario(
            name="mixed-long-short",
            workload=MIXED_LONG_SHORT,
            concurrency=96,
            long_messages=12,
            long_inner_concurrency=64,
            long_sleep_secs=long_io_secs,
            short_messages=120,
            short_inner_concurrency=6,
            short_sleep_secs=0.005,
            stage_delay_secs=min(max(long_io_secs * 0.05, 0.05), 0.15),
        ),
    ]


def make_broker(transport: str, namespace: str, urls: dict[str, str]):
    if transport == "redis":
        return fluxera.RedisBroker(urls["redis"], namespace=namespace)
    if transport == "rabbitmq":
        return fluxera.RabbitMQBroker(urls["rabbitmq"], namespace=namespace)
    raise RuntimeError(f"Unknown transport {transport!r}.")


async def _cleanup_namespace(transport: str, broker, namespace: str, urls: dict[str, str]) -> None:
    if transport == "redis":
        cursor = 0
        keys: list[bytes] = []
        while True:
            cursor, batch = await broker.client.scan(cursor=cursor, match=f"{namespace}:*", count=512)
            keys.extend(batch)
            if cursor == 0:
                break
        if keys:
            await broker.client.delete(*keys)
        return

    # RabbitMQ: management API로 네임스페이스 큐를 삭제한다
    parsed = urllib.parse.urlsplit(urls["rabbitmq"])
    username = parsed.username or "guest"
    password = parsed.password or "guest"
    host = parsed.hostname or "127.0.0.1"
    base = f"http://{host}:15672"
    auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()

    def _delete_all() -> None:
        request = urllib.request.Request(f"{base}/api/queues/%2F?columns=name")
        request.add_header("Authorization", auth)
        with urllib.request.urlopen(request, timeout=5) as response:
            queues = json.loads(response.read() or b"[]")
        for queue in queues:
            name = queue.get("name", "")
            if not name.startswith(namespace):
                continue
            encoded = urllib.parse.quote(name, safe="")
            delete_request = urllib.request.Request(f"{base}/api/queues/%2F/{encoded}", method="DELETE")
            delete_request.add_header("Authorization", auth)
            try:
                urllib.request.urlopen(delete_request, timeout=5)
            except Exception:
                pass

    await asyncio.to_thread(_delete_all)


async def _run_scenario(scenario: Scenario, transport: str, urls: dict[str, str]) -> RunMetrics:
    namespace = f"fluxera-bench-{transport}-{scenario.name}-{uuid4().hex[:8]}"
    broker = make_broker(transport, namespace, urls)

    peak_threads = len(threading.enumerate())
    wall_start = time.perf_counter()
    cpu_start = cpu_time_sec()

    try:
        if scenario.workload in (ENQUEUE_ONLY, ENQUEUE_CONCURRENT):

            @fluxera.actor(broker=broker, queue_name="enqueue")
            async def sink(index: int) -> None:
                del index

            enqueue_start = time.perf_counter()
            if scenario.workload == ENQUEUE_ONLY:
                for index in range(scenario.messages):
                    await sink.send(index)
            else:
                batch = max(scenario.enqueue_batch, 1)
                for offset in range(0, scenario.messages, batch):
                    await asyncio.gather(*(
                        sink.send(offset + j)
                        for j in range(min(batch, scenario.messages - offset))
                    ))
            enqueue_wall = time.perf_counter() - enqueue_start

            wall = time.perf_counter() - wall_start
            return RunMetrics(
                transport=transport,
                variant="enqueue",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu_time_sec() - cpu_start,
                peak_rss_bytes=peak_rss_bytes(),
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                messages_total=scenario.messages,
                work_units_total=scenario.messages,
                messages_per_sec=scenario.messages / enqueue_wall,
                work_units_per_sec=scenario.messages / enqueue_wall,
                enqueue_time_sec=enqueue_wall,
            )

        if scenario.workload == ASYNC_FANOUT:
            completed: list[float] = []

            @fluxera.actor(broker=broker, queue_name="fanout")
            async def fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)
                completed.append(time.perf_counter())

            worker = fluxera.Worker(broker, concurrency=scenario.concurrency, process_concurrency=0)
            await worker.start()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.messages):
                    await fanout.send(scenario.sleep_secs, scenario.inner_concurrency)
                await wait_until(lambda: len(completed) >= scenario.messages, timeout=300.0)
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                await worker.stop()

            wall = time.perf_counter() - wall_start
            work_units = scenario.messages * scenario.inner_concurrency
            return RunMetrics(
                transport=transport,
                variant=f"c={scenario.concurrency}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu_time_sec() - cpu_start,
                peak_rss_bytes=peak_rss_bytes(),
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                messages_total=scenario.messages,
                work_units_total=work_units,
                messages_per_sec=scenario.messages / wall,
                work_units_per_sec=work_units / wall,
            )

        if scenario.workload == MIXED_LONG_SHORT:
            long_completed: list[float] = []
            short_completed: list[float] = []
            short_submit_at: Optional[float] = None

            @fluxera.actor(broker=broker, actor_name="long_fanout", queue_name="mixed")
            async def long_fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)
                long_completed.append(time.perf_counter())

            @fluxera.actor(broker=broker, actor_name="short_fanout", queue_name="mixed")
            async def short_fanout(delay: float, inner: int) -> None:
                await wait_many(delay, inner)
                short_completed.append(time.perf_counter())

            worker = fluxera.Worker(broker, concurrency=scenario.concurrency, process_concurrency=0)
            await worker.start()
            try:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                for _ in range(scenario.long_messages):
                    await long_fanout.send(scenario.long_sleep_secs, scenario.long_inner_concurrency)
                await asyncio.sleep(scenario.stage_delay_secs)
                short_submit_at = time.perf_counter()
                for _ in range(scenario.short_messages):
                    await short_fanout.send(scenario.short_sleep_secs, scenario.short_inner_concurrency)
                await wait_until(
                    lambda: (
                        len(long_completed) >= scenario.long_messages
                        and len(short_completed) >= scenario.short_messages
                    ),
                    timeout=600.0,
                )
            finally:
                peak_threads = max(peak_threads, len(threading.enumerate()))
                await worker.stop()

            wall = time.perf_counter() - wall_start
            message_total = scenario.long_messages + scenario.short_messages
            work_units = (
                scenario.long_messages * scenario.long_inner_concurrency
                + scenario.short_messages * scenario.short_inner_concurrency
            )
            short_drain = None
            if short_submit_at is not None and short_completed:
                short_drain = max(short_completed) - short_submit_at

            return RunMetrics(
                transport=transport,
                variant=f"c={scenario.concurrency}",
                scenario=scenario.name,
                workload=scenario.workload,
                wall_time_sec=wall,
                cpu_time_sec=cpu_time_sec() - cpu_start,
                peak_rss_bytes=peak_rss_bytes(),
                peak_thread_count=max(peak_threads, len(threading.enumerate())),
                messages_total=message_total,
                work_units_total=work_units,
                messages_per_sec=message_total / wall,
                work_units_per_sec=work_units / wall,
                short_drain_time_sec=short_drain,
            )

        raise RuntimeError(f"Unknown workload {scenario.workload!r}.")
    finally:
        try:
            await _cleanup_namespace(transport, broker, namespace, urls)
        finally:
            await broker.close()


def _entry(scenario_dict: dict[str, Any], transport: str, urls: dict[str, str], result_queue) -> None:
    try:
        metrics = asyncio.run(_run_scenario(Scenario(**scenario_dict), transport, urls))
        result_queue.put(("ok", asdict(metrics)))
    except BaseException:
        result_queue.put(("error", traceback.format_exc()))


def run_once(transport: str, scenario: Scenario, urls: dict[str, str]) -> RunMetrics:
    # spawn 프로세스 격리: 클라이언트 라이브러리/이벤트 루프 상태를 실행 간 공유하지 않는다
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    process = ctx.Process(
        target=_entry,
        args=(asdict(scenario), transport, urls, result_queue),
        name=f"{transport}-{scenario.name}",
    )
    process.start()
    status, payload = result_queue.get()
    process.join()
    if process.exitcode not in (0, None):
        raise RuntimeError(f"{transport} benchmark process exited with code {process.exitcode}.")
    if status != "ok":
        raise RuntimeError(f"{transport} benchmark failed:\n{payload}")
    return RunMetrics(**payload)


def summarize_runs(runs: list[RunMetrics]) -> dict[str, Any]:
    return {
        "transport": runs[0].transport,
        "variant": runs[0].variant,
        "scenario": runs[0].scenario,
        "workload": runs[0].workload,
        "runs": len(runs),
        "wall_time_sec": mean(run.wall_time_sec for run in runs),
        "cpu_time_sec": mean(run.cpu_time_sec for run in runs),
        "peak_rss_mib": mean(run.peak_rss_bytes for run in runs) / (1024 * 1024),
        "peak_thread_count": mean(run.peak_thread_count for run in runs),
        "messages_total": runs[0].messages_total,
        "work_units_total": runs[0].work_units_total,
        "messages_per_sec": mean(run.messages_per_sec for run in runs),
        "work_units_per_sec": mean(run.work_units_per_sec for run in runs),
        "enqueue_time_sec": mean(
            run.enqueue_time_sec for run in runs if run.enqueue_time_sec is not None
        ) if any(run.enqueue_time_sec is not None for run in runs) else None,
        "short_drain_time_sec": mean(
            run.short_drain_time_sec for run in runs if run.short_drain_time_sec is not None
        ) if any(run.short_drain_time_sec is not None for run in runs) else None,
    }


def render_summary(summary: dict[str, Any]) -> str:
    parts = [
        f"{summary['transport']:<9}[{summary['variant']:<10}]",
        f"runs={summary['runs']}",
        f"wall={summary['wall_time_sec']:.3f}s",
        f"cpu={summary['cpu_time_sec']:.3f}s",
        f"peak_rss={summary['peak_rss_mib']:.2f}MiB",
        f"msg/s={summary['messages_per_sec']:.1f}",
        f"work/s={summary['work_units_per_sec']:.1f}",
    ]
    if summary["enqueue_time_sec"] is not None:
        parts.append(f"enqueue={summary['enqueue_time_sec']:.3f}s")
    if summary["short_drain_time_sec"] is not None:
        parts.append(f"short_drain={summary['short_drain_time_sec']:.3f}s")
    return " ".join(parts)


def render_ratio(base: dict[str, Any], other: dict[str, Any]) -> str:
    parts = [
        f"{other['transport']}/{base['transport']}",
        f"wall={other['wall_time_sec'] / base['wall_time_sec']:.2f}x",
        f"msg/s={other['messages_per_sec'] / max(base['messages_per_sec'], 1e-9):.2f}x",
    ]
    if base["enqueue_time_sec"] is not None and other["enqueue_time_sec"] is not None:
        parts.append(f"enqueue={other['enqueue_time_sec'] / base['enqueue_time_sec']:.2f}x")
    if base["short_drain_time_sec"] is not None and other["short_drain_time_sec"] is not None:
        parts.append(f"short_drain={other['short_drain_time_sec'] / base['short_drain_time_sec']:.2f}x")
    return " ".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Fluxera transports: RedisBroker vs RabbitMQBroker.")
    parser.add_argument("--redis-url", default=os.environ.get("FLUXERA_REDIS_URL", "redis://127.0.0.1:6379/15"))
    parser.add_argument("--amqp-url", default=os.environ.get("FLUXERA_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/"))
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--long-io-secs", type=float, default=2.0)
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="이름에 이 부분 문자열이 포함된 시나리오만 실행. 반복 지정 가능.",
    )
    parser.add_argument(
        "--transport",
        action="append",
        choices=TRANSPORTS,
        default=[],
        help="실행할 전송 계층. 기본값은 redis와 rabbitmq 모두.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenarios = [
        scenario
        for scenario in build_scenarios(args.long_io_secs)
        if not args.scenario or any(pattern in scenario.name for pattern in args.scenario)
    ]
    if not scenarios:
        print("No scenarios matched the provided filters.")
        return 1

    transports = tuple(args.transport) or TRANSPORTS
    urls = {"redis": args.redis_url, "rabbitmq": args.amqp_url}

    print(f"Redis URL: {urls['redis']}")
    print(f"AMQP URL: {urls['rabbitmq']}")
    print(f"Repeats: {args.repeat}")

    for scenario in scenarios:
        print()
        print(f"Scenario: {scenario.name}")
        summaries: list[dict[str, Any]] = []
        for transport in transports:
            runs = [run_once(transport, scenario, urls) for _ in range(args.repeat)]
            summary = summarize_runs(runs)
            summaries.append(summary)
            print("  " + render_summary(summary))

        if len(summaries) == 2:
            print("  " + render_ratio(summaries[0], summaries[1]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
