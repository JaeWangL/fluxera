# RedisBroker 유실/대역폭 진단 스크립트 (읽기 전용)
#
# 용도: "재시도가 안 되는 것 같다 / 메시지가 유실되는 것 같다"는 증상의 원인을
# 서버 통계와 fluxera 키 공간에서 직접 확인한다. 프로덕션(Redis Cloud 등)에
# 그대로 실행해도 안전하다 — 쓰기 명령을 사용하지 않는다.
#
#   python scripts/diagnose_redis_broker.py --redis-url rediss://... --namespace fluxera
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import orjson
import redis as sync_redis

MONTH_SECONDS = 30 * 24 * 3600


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose RedisBroker message loss and bandwidth usage (read-only).")
    parser.add_argument("--redis-url", default=os.environ.get("FLUXERA_REDIS_URL", "redis://127.0.0.1:6379/15"))
    parser.add_argument("--namespace", default="fluxera")
    parser.add_argument("--sample-seconds", type=float, default=10.0, help="대역폭/명령 레이트 샘플링 시간")
    parser.add_argument("--scan-limit", type=int, default=200, help="큐/페이로드 스캔 상한")
    return parser.parse_args()


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def warn(message: str) -> None:
    print(f"  [경고] {message}")


def ok(message: str) -> None:
    print(f"  [정상] {message}")


def scan_keys(client, pattern: str, limit: int) -> list[bytes]:
    keys: list[bytes] = []
    for key in client.scan_iter(match=pattern, count=200):
        keys.append(key)
        if len(keys) >= limit:
            break
    return keys


def main() -> int:
    args = parse_args()
    client = sync_redis.Redis.from_url(args.redis_url, socket_timeout=10.0)
    ns = args.namespace.strip(":")
    findings: list[str] = []

    # ---- 1. 서버 메모리/eviction 설정 ----
    section("메모리 / eviction 정책")
    info_memory = client.info("memory")
    maxmemory = int(info_memory.get("maxmemory", 0))
    policy = info_memory.get("maxmemory_policy", "unknown")
    used = int(info_memory.get("used_memory", 0))
    print(f"  used_memory={used / 1e6:.1f}MB maxmemory={maxmemory / 1e6:.1f}MB policy={policy}")
    if policy != "noeviction":
        warn(
            f"maxmemory-policy가 {policy!r}다. 큐 워크로드는 반드시 'noeviction'이어야 한다. "
            "volatile-*는 TTL이 걸린 키(메시지 페이로드, dedupe, DLQ 레코드)를, "
            "allkeys-*는 스트림/지연 zset까지 제거한다 → 메시지가 조용히 사라진다."
        )
        findings.append(f"eviction 정책 {policy} (noeviction 필요)")
    else:
        ok("noeviction — eviction에 의한 유실 없음")
    if maxmemory and used / maxmemory > 0.85:
        warn(f"메모리 사용률 {used / maxmemory * 100:.0f}% — eviction/enqueue 실패 임박")
        findings.append("메모리 사용률 85% 초과")

    # ---- 2. 유실의 직접 증거: eviction/만료/커넥션 거부 ----
    section("유실 증거 카운터 (서버 누적)")
    stats = client.info("stats")
    evicted = int(stats.get("evicted_keys", 0))
    expired = int(stats.get("expired_keys", 0))
    rejected = int(stats.get("rejected_connections", 0))
    print(f"  evicted_keys={evicted} expired_keys={expired} rejected_connections={rejected}")
    if evicted:
        warn(
            f"evicted_keys={evicted} — 메모리 압박으로 키가 제거된 이력이 있다. "
            "페이로드가 제거되면 integrity_missing_payload DLQ(복구 불가) 또는 무음 유실이 된다."
        )
        findings.append(f"evicted_keys={evicted} (유실의 직접 증거)")
    if rejected:
        warn(f"rejected_connections={rejected} — 커넥션 한도 초과 이력")
        findings.append(f"rejected_connections={rejected}")

    # ---- 3. 대역폭: 현재 레이트 → 월간 추정 ----
    section(f"대역폭 샘플링 ({args.sample_seconds:.0f}s)")
    stats_before = client.info("stats")
    try:
        cmd_before = client.info("commandstats")
    except Exception:
        cmd_before = {}
    time.sleep(args.sample_seconds)
    stats_after = client.info("stats")
    try:
        cmd_after = client.info("commandstats")
    except Exception:
        cmd_after = {}

    in_delta = int(stats_after.get("total_net_input_bytes", 0)) - int(stats_before.get("total_net_input_bytes", 0))
    out_delta = int(stats_after.get("total_net_output_bytes", 0)) - int(stats_before.get("total_net_output_bytes", 0))
    total_rate = (in_delta + out_delta) / args.sample_seconds
    monthly_gb = total_rate * MONTH_SECONDS / 1e9
    print(
        f"  input={in_delta / args.sample_seconds / 1e3:.1f}KB/s "
        f"output={out_delta / args.sample_seconds / 1e3:.1f}KB/s "
        f"→ 월간 추정 {monthly_gb:.1f}GB (지금 순간의 레이트 기준)"
    )

    if cmd_before and cmd_after:
        deltas = []
        for key, after in cmd_after.items():
            before_calls = int(cmd_before.get(key, {}).get("calls", 0))
            delta_calls = int(after.get("calls", 0)) - before_calls
            if delta_calls > 0:
                deltas.append((delta_calls, key.replace("cmdstat_", "")))
        deltas.sort(reverse=True)
        print("  샘플 구간 명령 호출 상위 (폴링 비용 분석용):")
        for delta_calls, name in deltas[:10]:
            print(f"    {name:<24} {delta_calls / args.sample_seconds:>8.1f} calls/s")

    # ---- 4. fluxera 키 공간: 백로그 / 페이로드 크기 ----
    section(f"네임스페이스 {ns!r} 상태")
    stream_keys = scan_keys(client, f"{ns}:stream:*", args.scan_limit)
    delayed_keys = scan_keys(client, f"{ns}:delayed:*", args.scan_limit)
    now_ms = int(time.time() * 1000)
    total_ready = total_pending = total_delayed = overdue_delayed = 0
    for key in stream_keys:
        total_ready += int(client.xlen(key))
        try:
            groups = client.xinfo_groups(key)
        except sync_redis.ResponseError:
            groups = []
        for group in groups:
            total_pending += int(group.get("pending", 0))
    for key in delayed_keys:
        total_delayed += int(client.zcard(key))
        # 승격 시점이 지났는데 아직 지연 zset에 남은 항목 = 재시도가 멈춘 직접 증거
        overdue_delayed += int(client.zcount(key, 0, now_ms - 60_000))
    print(
        f"  큐 {len(stream_keys)}개: ready={total_ready} pending(unacked)={total_pending} "
        f"delayed={total_delayed} (그중 60초 이상 승격 지연 {overdue_delayed})"
    )
    if overdue_delayed:
        warn(
            f"승격 시점이 60초 이상 지난 지연 메시지 {overdue_delayed}건 — 해당 큐를 소비하는 "
            "워커가 없거나(promote는 컨슈머 폴링이 수행) 워커가 브로커에 닿지 못하고 있다."
        )
        findings.append(f"승격 지연 메시지 {overdue_delayed}건 (재시도 정체)")

    payload_keys = scan_keys(client, f"{ns}:message:*", min(args.scan_limit, 100))
    if payload_keys:
        pipe = client.pipeline(transaction=False)
        for key in payload_keys:
            pipe.strlen(key)
        sizes = sorted(int(size) for size in pipe.execute())
        avg = sum(sizes) / len(sizes)
        p95 = sizes[int(len(sizes) * 0.95) - 1] if len(sizes) > 1 else sizes[0]
        print(f"  페이로드 샘플 {len(sizes)}건: 평균 {avg / 1e3:.1f}KB p95 {p95 / 1e3:.1f}KB 최대 {sizes[-1] / 1e3:.1f}KB")
        # 페이로드는 최소 2회(쓰기+읽기) 전송된다. 재시도·requeue마다 추가 왕복.
        print(f"    → 메시지 100만 건/월 기준 페이로드 트래픽만 약 {avg * 2 * 1e6 / 1e9:.1f}GB")

    # ---- 5. DLQ: 유실 메커니즘 판정 ----
    section("Dead letter 분석")
    dlq_keys = [key for key in scan_keys(client, f"{ns}:dlq:*", args.scan_limit) if b":record:" not in key]
    integrity_missing = integrity_decode = operator_or_exhausted = unreadable = 0
    for key in dlq_keys:
        for dead_letter_id in client.zrange(key, 0, 500):
            payload = client.get(f"{ns}:dlq:record:{dead_letter_id.decode()}")
            if payload is None:
                unreadable += 1
                continue
            try:
                record = orjson.loads(payload)
            except Exception:
                unreadable += 1
                continue
            kind = record.get("failure_kind")
            if kind == "integrity_missing_payload":
                integrity_missing += 1
            elif kind == "integrity_decode_error":
                integrity_decode += 1
            else:
                operator_or_exhausted += 1
    print(
        f"  DLQ {len(dlq_keys)}개 큐: integrity_missing_payload={integrity_missing} "
        f"decode_error={integrity_decode} 일반(재시도 소진 등)={operator_or_exhausted} "
        f"레코드 소실={unreadable}"
    )
    if integrity_missing:
        warn(
            f"integrity_missing_payload {integrity_missing}건 — 페이로드가 TTL 만료 또는 eviction으로 "
            "사라진 뒤 소비된 메시지다. 스냅샷이 없어 requeue_dead_letter로 복구할 수 없다. "
            "'유실처럼 보였다'의 코드상 실체가 바로 이것이다."
        )
        findings.append(f"integrity_missing_payload {integrity_missing}건 (페이로드 소실 확정)")
    if unreadable:
        warn(
            f"DLQ 인덱스에는 있는데 레코드 본문이 없는 항목 {unreadable}건 — DLQ 레코드 자체가 "
            "TTL/eviction으로 사라졌다. 완전 무음 유실 구간이 존재했다는 뜻이다."
        )
        findings.append(f"DLQ 레코드 소실 {unreadable}건 (무음 유실 증거)")

    # ---- 판정 ----
    section("판정")
    if findings:
        for finding in findings:
            print(f"  - {finding}")
        print(
            "\n  권장 조치: (1) eviction 정책을 noeviction으로 변경하고 메모리 티어 확보, "
            "(2) 백로그가 24시간을 넘을 수 있으면 message_ttl_seconds 상향, "
            "(3) DLQ의 integrity_* 레코드를 모니터링/알림에 연결, "
            "(4) 대역폭 초과분은 명령 호출 상위 목록(폴링)과 페이로드 크기 중 큰 쪽부터 줄인다."
        )
        return 1
    print("  유실 증거 없음. 대역폭/백로그 수치를 위에서 확인하라.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
