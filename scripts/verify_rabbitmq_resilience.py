# RabbitMQ 브로커 at-least-once 복원력 검증 스크립트 (docker 조작 포함)
#
# 시나리오:
#   1. consumer-timeout-enforcement — 서버가 x-consumer-timeout을 실제로 강제해
#      채널을 끊고 재전달하는지 (장시간 태스크의 실제 위험 경로, 분 단위 소요)
#   2. broker-restart — 처리 중 docker restart. 태스크는 장애 중 완료(ack 실패
#      삼킴), 복구 후 재전달·재처리, 유실 없음
#   3. broker-hang — docker pause로 서버 행(생사불명). 낮은 heartbeat로 감지,
#      unpause 후 재전달·재처리, 유실 없음
from __future__ import annotations

import asyncio
import os
import json
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import aio_pika
import fluxera

AMQP_URL = os.environ.get("FLUXERA_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
CONTAINER = os.environ.get("FLUXERA_RABBITMQ_CONTAINER", "fluxera-rabbitmq")
MANAGEMENT_URL = os.environ.get("FLUXERA_RABBITMQ_MANAGEMENT_URL", "http://guest:guest@127.0.0.1:15672/")
results: dict[str, str] = {}


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


async def wait_until(predicate, *, timeout: float, interval: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


async def scenario_consumer_timeout_enforcement() -> None:
    """x-consumer-timeout=10s 큐에서 ack 없이 버티는 delivery를 서버가 실제로
    강제 종료하고 재전달하는지 확인한다. 강제는 주기적으로 평가되므로 최대 수십 초
    걸릴 수 있다."""
    ns = f"flx-ct-{uuid4().hex[:8]}"
    broker = fluxera.RabbitMQBroker(AMQP_URL, namespace=ns, management_url=MANAGEMENT_URL, consumer_timeout_seconds=10.0)

    @fluxera.actor(broker=broker, queue_name="default")
    async def noop() -> None:
        return None

    await noop.send()
    victim = await broker.open_consumer("default")
    got = await victim.receive(limit=1, timeout=3.0)
    assert len(got) == 1

    # ack 없이 방치 → 서버가 consumer timeout으로 채널을 끊고 requeue해야 한다.
    # RobustChannel이 자동 복구·재consume하므로 같은 컨슈머 버퍼로 재전달본이 온다.
    t0 = time.monotonic()
    redelivered = []
    deadline = time.monotonic() + 150.0
    while time.monotonic() < deadline:
        batch = await victim.receive(limit=1, timeout=2.0)
        if batch:
            redelivered = batch
            break

    if redelivered and redelivered[0].redelivered:
        elapsed = time.monotonic() - t0
        await victim.ack(redelivered[0])
        results["consumer_timeout_enforcement"] = f"PASS (재전달까지 {elapsed:.0f}s)"
    else:
        results["consumer_timeout_enforcement"] = "FAIL: 150s 내 재전달 없음"

    await victim.close()
    await broker.close()


async def scenario_broker_restart() -> None:
    """처리 중 브로커 재시작. 태스크는 장애 중 완료(ack 실패는 삼켜짐),
    복구 후 재전달본이 다시 실행되어야 한다."""
    ns = f"flx-restart-{uuid4().hex[:8]}"
    broker = fluxera.RabbitMQBroker(AMQP_URL, namespace=ns, management_url=MANAGEMENT_URL, reconnect_interval_seconds=0.5)
    executions: list[float] = []
    in_task = asyncio.Event()
    release = asyncio.Event()

    @fluxera.actor(broker=broker, queue_name="default")
    async def long_task() -> None:
        executions.append(time.monotonic())
        in_task.set()
        await release.wait()

    worker = fluxera.Worker(broker, concurrency=2, process_concurrency=0, poll_timeout=0.05)
    await worker.start()
    try:
        await long_task.send()
        await asyncio.wait_for(in_task.wait(), timeout=5.0)

        docker("restart", CONTAINER)

        # 장애 중 태스크 완료 → ack는 실패하지만 삼켜져야 한다 (워커 생존)
        release.set()
        await asyncio.sleep(1.0)

        # 복구 후 재전달본이 실행되어야 한다 (release는 이미 set → 즉시 완료)
        ok = await wait_until(lambda: len(executions) >= 2, timeout=60.0)
        if not ok:
            results["broker_restart"] = f"FAIL: 재전달 실행 없음 (executions={len(executions)})"
            return

        await asyncio.wait_for(broker.join("default"), timeout=30.0)
        results["broker_restart"] = f"PASS (실행 {len(executions)}회, 유실 0)"
    finally:
        release.set()
        await worker.stop()


async def scenario_broker_hang() -> None:
    """docker pause로 서버가 살았는지 죽었는지 모르는 상태를 만든다.
    낮은 heartbeat로 클라이언트가 감지하고, unpause 후 재전달·재처리돼야 한다."""
    ns = f"flx-hang-{uuid4().hex[:8]}"
    broker = fluxera.RabbitMQBroker(
        AMQP_URL,
        namespace=ns,
        management_url=MANAGEMENT_URL,
        heartbeat_seconds=3.0,
        reconnect_interval_seconds=0.5,
    )
    executions: list[float] = []
    in_task = asyncio.Event()
    release = asyncio.Event()

    @fluxera.actor(broker=broker, queue_name="default")
    async def long_task() -> None:
        executions.append(time.monotonic())
        in_task.set()
        await release.wait()

    worker = fluxera.Worker(broker, concurrency=2, process_concurrency=0, poll_timeout=0.05)
    await worker.start()
    try:
        await long_task.send()
        await asyncio.wait_for(in_task.wait(), timeout=5.0)

        docker("pause", CONTAINER)
        paused_at = time.monotonic()
        try:
            # 태스크를 pause 내내 붙잡아 둔다. stuck 임계값 (heartbeat+1)*3 = 12s를
            # 확실히 넘겨 클라이언트가 consume 커넥션을 확정적으로 끊게 만든다.
            await asyncio.sleep(20.0)
        finally:
            docker("unpause", CONTAINER)

        # 재연결이 자리잡은 뒤 태스크를 완료시킨다: 원본 실행의 ack는 죽은 채널에
        # 대한 것이라 삼켜지고, 서버는 이미 requeue했으므로 재전달본이 실행돼야 한다.
        await asyncio.sleep(5.0)
        release.set()

        ok = await wait_until(lambda: len(executions) >= 2, timeout=60.0)
        if not ok:
            results["broker_hang"] = f"FAIL: unpause 후 재전달 없음 (executions={len(executions)})"
            return

        await asyncio.wait_for(broker.join("default"), timeout=30.0)
        hang_secs = time.monotonic() - paused_at
        results["broker_hang"] = f"PASS (실행 {len(executions)}회, 행 구간 {hang_secs:.0f}s, 유실 0)"
    finally:
        release.set()
        await worker.stop()


async def cleanup_namespace(prefix: str) -> None:
    import base64
    import urllib.parse
    import urllib.request

    import urllib.parse as _p
    parsed = _p.urlsplit(MANAGEMENT_URL)
    base = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 15672}"
    creds = f"{_p.unquote(parsed.username or 'guest')}:{_p.unquote(parsed.password or 'guest')}"
    auth = "Basic " + base64.b64encode(creds.encode()).decode()

    def run() -> None:
        request = urllib.request.Request(f"{base}/api/queues/%2F?columns=name")
        request.add_header("Authorization", auth)
        with urllib.request.urlopen(request, timeout=5) as response:
            queues = json.loads(response.read() or b"[]")
        for queue in queues:
            name = queue.get("name", "")
            if name.startswith(prefix):
                encoded = urllib.parse.quote(name, safe="")
                delete = urllib.request.Request(
                    f"{base}/api/queues/%2F/{encoded}", method="DELETE"
                )
                delete.add_header("Authorization", auth)
                try:
                    urllib.request.urlopen(delete, timeout=5)
                except Exception:
                    pass

    await asyncio.to_thread(run)


async def main() -> None:
    scenarios = {
        "broker-restart": scenario_broker_restart,
        "broker-hang": scenario_broker_hang,
        "consumer-timeout": scenario_consumer_timeout_enforcement,
    }
    requested = sys.argv[1:] or list(scenarios)
    for name in requested:
        print(f"--- {name} 실행 중 ---", flush=True)
        try:
            await scenarios[name]()
        except Exception as exc:
            results[name.replace("-", "_")] = f"FAIL: {type(exc).__name__}: {exc}"
    await cleanup_namespace("flx-")
    print(json.dumps(results, indent=2, ensure_ascii=False))
    if any(not value.startswith("PASS") for value in results.values()):
        sys.exit(1)


asyncio.run(main())
