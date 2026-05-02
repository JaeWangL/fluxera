from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional

from .brokers.redis import RedisBroker
from .dead_letters import DeadLetterRecord


@dataclass(slots=True)
class ServingRevisionStatus:
    namespace: str
    queue_name: str
    serving_revision: Optional[str]


@dataclass(slots=True)
class ServingRevisionPromotion:
    namespace: str
    queue_name: str
    requested_revision: str
    previous_revision: Optional[str]
    serving_revision: Optional[str]
    updated: bool
    expected_revision: Optional[str] = None


@dataclass(slots=True)
class ServingRevisionSafePromotion:
    namespace: str
    queue_name: str
    requested_revision: str
    previous_revision: Optional[str]
    serving_revision: Optional[str]
    updated: bool
    expected_revision: Optional[str]
    pre_ready: bool
    post_accepting: bool
    timed_out: bool
    ready_workers: int
    accepting_workers: int
    waited_seconds: float
    worker_ids: list[str]


@dataclass(slots=True)
class ServingRevisionRecovery:
    namespace: str
    queue_name: str
    requested_revision: str
    previous_revision: Optional[str]
    serving_revision: Optional[str]
    updated: bool
    reason: str
    waiting_not_running: int
    ready_workers: int
    accepting_workers: int
    previous_accepting_workers: int
    worker_ids: list[str]
    waited_seconds: float


@dataclass(slots=True)
class DeadLetterStatus:
    namespace: str
    queue_name: str
    record: Optional[DeadLetterRecord]


@dataclass(slots=True)
class DeadLetterList:
    namespace: str
    queue_name: str
    records: list[DeadLetterRecord]


@dataclass(slots=True)
class DeadLetterResolution:
    namespace: str
    queue_name: str
    dead_letter_id: str
    action: str
    updated: bool
    record: Optional[DeadLetterRecord]


@dataclass(slots=True)
class WorkerRuntimeStatus:
    namespace: str
    worker_id: str
    worker_revision: Optional[str]
    status: str
    last_seen_ms: Optional[int]
    age_ms: Optional[int]
    hostname: str
    pid: Optional[int]
    queues: list[str]
    accepting_queues: list[str]
    runtime: dict[str, Any]


@dataclass(slots=True)
class QueueRuntimeStatus:
    namespace: str
    queue_name: str
    serving_revision: Optional[str]
    status: str
    stream_ready: int
    delayed: int
    pending: int
    pending_stale: int
    waiting_not_running: int
    workers_total: int
    workers_accepting: int
    workers_draining: int


@dataclass(slots=True)
class RuntimeStatus:
    namespace: str
    generated_at_ms: int
    overall_status: str
    workers: list[WorkerRuntimeStatus]
    queues: list[QueueRuntimeStatus]
    totals: dict[str, int]


def _split_csv(raw: Optional[str]) -> list[str]:
    if raw is None:
        return []
    return [part for part in raw.split(",") if part]


def _to_int(raw: Optional[str]) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _parse_runtime_value(raw: str) -> Any:
    lower = raw.lower()
    if lower in {"true", "false"}:
        return lower == "true"
    try:
        return int(raw)
    except (TypeError, ValueError):
        return raw


def _runtime_payload(status: RuntimeStatus) -> dict[str, Any]:
    return {
        "namespace": status.namespace,
        "generated_at_ms": status.generated_at_ms,
        "overall_status": status.overall_status,
        "totals": status.totals,
        "workers": [
            {
                "namespace": worker.namespace,
                "worker_id": worker.worker_id,
                "worker_revision": worker.worker_revision,
                "status": worker.status,
                "last_seen_ms": worker.last_seen_ms,
                "age_ms": worker.age_ms,
                "hostname": worker.hostname,
                "pid": worker.pid,
                "queues": worker.queues,
                "accepting_queues": worker.accepting_queues,
                "runtime": worker.runtime,
            }
            for worker in status.workers
        ],
        "queues": [
            {
                "namespace": queue.namespace,
                "queue_name": queue.queue_name,
                "serving_revision": queue.serving_revision,
                "status": queue.status,
                "stream_ready": queue.stream_ready,
                "delayed": queue.delayed,
                "pending": queue.pending,
                "pending_stale": queue.pending_stale,
                "waiting_not_running": queue.waiting_not_running,
                "workers_total": queue.workers_total,
                "workers_accepting": queue.workers_accepting,
                "workers_draining": queue.workers_draining,
            }
            for queue in status.queues
        ],
    }


async def get_serving_revision(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
) -> ServingRevisionStatus:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        return ServingRevisionStatus(
            namespace=namespace,
            queue_name=queue_name,
            serving_revision=await broker.get_serving_revision(queue_name),
        )
    finally:
        await broker.close()


async def ensure_serving_revision(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    revision: str,
) -> ServingRevisionStatus:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        serving_revision = await broker.ensure_serving_revision(queue_name, revision)
        return ServingRevisionStatus(
            namespace=namespace,
            queue_name=queue_name,
            serving_revision=serving_revision,
        )
    finally:
        await broker.close()


async def promote_serving_revision(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    revision: str,
    expected_revision: Optional[str] = None,
) -> ServingRevisionPromotion:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        previous_revision = await broker.get_serving_revision(queue_name)
        updated = await broker.promote_serving_revision(
            queue_name,
            revision,
            expected_revision=expected_revision,
        )
        serving_revision = await broker.get_serving_revision(queue_name)
        return ServingRevisionPromotion(
            namespace=namespace,
            queue_name=queue_name,
            requested_revision=revision,
            previous_revision=previous_revision,
            serving_revision=serving_revision,
            updated=updated,
            expected_revision=expected_revision,
        )
    finally:
        await broker.close()


async def promote_serving_revision_safe(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    revision: str,
    expected_revision: Optional[str] = None,
    min_ready_workers: int = 1,
    min_accepting_workers: int = 1,
    timeout_seconds: float = 180.0,
    poll_interval_seconds: float = 1.0,
    worker_stale_after_ms: Optional[int] = None,
) -> ServingRevisionSafePromotion:
    broker = RedisBroker(redis_url, namespace=namespace)
    start = time.monotonic()
    normalized_min_ready_workers = max(int(min_ready_workers), 1)
    normalized_min_accepting_workers = max(int(min_accepting_workers), 1)
    normalized_timeout_seconds = max(float(timeout_seconds), 1.0)
    normalized_poll_interval_seconds = max(float(poll_interval_seconds), 0.1)
    effective_worker_stale_after_ms = (
        max(int(worker_stale_after_ms), 1)
        if worker_stale_after_ms is not None
        else broker.worker_presence_ttl_ms * 2
    )
    deadline = start + normalized_timeout_seconds

    async def _snapshot_workers() -> tuple[int, int, list[str]]:
        rows = await broker.list_worker_runtime_rows(queue_names={queue_name})
        now_ms = int(time.time() * 1000)
        ready_workers = 0
        accepting_workers = 0
        worker_ids: list[str] = []
        for row in rows:
            if row.get("worker_revision") != revision:
                continue

            last_seen_ms = _to_int(row.get("last_seen_ms"))
            if last_seen_ms is None or (now_ms - last_seen_ms) > effective_worker_stale_after_ms:
                continue

            worker_queues = set(_split_csv(row.get("queues")))
            if queue_name not in worker_queues:
                continue

            ready_workers += 1
            worker_id = row.get("worker_id")
            if worker_id:
                worker_ids.append(worker_id)

            accepting_queues = set(_split_csv(row.get("accepting_queues")))
            if queue_name in accepting_queues:
                accepting_workers += 1

        worker_ids.sort()
        return ready_workers, accepting_workers, worker_ids

    try:
        previous_revision = await broker.get_serving_revision(queue_name)
        ready_workers = 0
        accepting_workers = 0
        worker_ids: list[str] = []
        pre_ready = False
        post_accepting = False
        timed_out = False

        while True:
            ready_workers, accepting_workers, worker_ids = await _snapshot_workers()
            if ready_workers >= normalized_min_ready_workers:
                pre_ready = True
                break

            if time.monotonic() >= deadline:
                timed_out = True
                serving_revision = await broker.get_serving_revision(queue_name)
                return ServingRevisionSafePromotion(
                    namespace=namespace,
                    queue_name=queue_name,
                    requested_revision=revision,
                    previous_revision=previous_revision,
                    serving_revision=serving_revision,
                    updated=False,
                    expected_revision=expected_revision,
                    pre_ready=pre_ready,
                    post_accepting=post_accepting,
                    timed_out=timed_out,
                    ready_workers=ready_workers,
                    accepting_workers=accepting_workers,
                    waited_seconds=max(time.monotonic() - start, 0.0),
                    worker_ids=worker_ids,
                )
            await asyncio.sleep(normalized_poll_interval_seconds)

        updated = await broker.promote_serving_revision(
            queue_name,
            revision,
            expected_revision=expected_revision,
        )
        serving_revision = await broker.get_serving_revision(queue_name)
        if not updated:
            return ServingRevisionSafePromotion(
                namespace=namespace,
                queue_name=queue_name,
                requested_revision=revision,
                previous_revision=previous_revision,
                serving_revision=serving_revision,
                updated=False,
                expected_revision=expected_revision,
                pre_ready=pre_ready,
                post_accepting=post_accepting,
                timed_out=timed_out,
                ready_workers=ready_workers,
                accepting_workers=accepting_workers,
                waited_seconds=max(time.monotonic() - start, 0.0),
                worker_ids=worker_ids,
            )

        while True:
            ready_workers, accepting_workers, worker_ids = await _snapshot_workers()
            serving_revision = await broker.get_serving_revision(queue_name)
            if (
                serving_revision == revision
                and accepting_workers >= normalized_min_accepting_workers
            ):
                post_accepting = True
                break

            if time.monotonic() >= deadline:
                timed_out = True
                break
            await asyncio.sleep(normalized_poll_interval_seconds)

        return ServingRevisionSafePromotion(
            namespace=namespace,
            queue_name=queue_name,
            requested_revision=revision,
            previous_revision=previous_revision,
            serving_revision=serving_revision,
            updated=updated,
            expected_revision=expected_revision,
            pre_ready=pre_ready,
            post_accepting=post_accepting,
            timed_out=timed_out,
            ready_workers=ready_workers,
            accepting_workers=accepting_workers,
            waited_seconds=max(time.monotonic() - start, 0.0),
            worker_ids=worker_ids,
        )
    finally:
        await broker.close()


async def recover_blocked_serving_revisions(
    redis_url: str,
    *,
    namespace: str,
    revision: str,
    queue_names: Optional[list[str]] = None,
    min_ready_workers: int = 1,
    min_accepting_workers: int = 1,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 1.0,
    worker_stale_after_ms: Optional[int] = None,
) -> list[ServingRevisionRecovery]:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        return await recover_blocked_serving_revisions_for_broker(
            broker,
            revision=revision,
            queue_names=queue_names,
            min_ready_workers=min_ready_workers,
            min_accepting_workers=min_accepting_workers,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            worker_stale_after_ms=worker_stale_after_ms,
        )
    finally:
        await broker.close()


async def recover_blocked_serving_revisions_for_broker(
    broker: Any,
    *,
    revision: str,
    queue_names: Optional[list[str]] = None,
    min_ready_workers: int = 1,
    min_accepting_workers: int = 1,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 1.0,
    worker_stale_after_ms: Optional[int] = None,
) -> list[ServingRevisionRecovery]:
    if not hasattr(broker, "list_worker_runtime_rows"):
        raise RuntimeError("Blocked recovery requires worker runtime rows.")
    if not hasattr(broker, "get_queue_runtime_row"):
        raise RuntimeError("Blocked recovery requires queue runtime rows.")

    normalized_min_ready_workers = max(int(min_ready_workers), 1)
    normalized_min_accepting_workers = max(int(min_accepting_workers), 1)
    normalized_timeout_seconds = max(float(timeout_seconds), 1.0)
    normalized_poll_interval_seconds = max(float(poll_interval_seconds), 0.1)
    effective_worker_stale_after_ms = (
        max(int(worker_stale_after_ms), 1)
        if worker_stale_after_ms is not None
        else getattr(broker, "worker_presence_ttl_ms", 45_000) * 2
    )
    namespace_value = getattr(broker, "namespace", "")
    start = time.monotonic()

    if queue_names is None:
        if not hasattr(broker, "list_runtime_queues"):
            raise RuntimeError("queue_names are required when broker cannot list runtime queues.")
        queue_names = await broker.list_runtime_queues()

    target_queue_names = sorted(set(queue_names))

    def _row_is_online(row: dict[str, Any], *, now_ms: int) -> bool:
        last_seen_ms = _to_int(row.get("last_seen_ms"))
        if last_seen_ms is None:
            return True
        return (now_ms - last_seen_ms) <= effective_worker_stale_after_ms

    def _queue_set(row: dict[str, Any], field: str) -> set[str]:
        return set(_split_csv(row.get(field)))

    async def _snapshot(queue_name: str) -> tuple[dict[str, Any], int, int, int, list[str]]:
        now_ms = int(time.time() * 1000)
        runtime_row = await broker.get_queue_runtime_row(queue_name)
        worker_rows = await broker.list_worker_runtime_rows(queue_names={queue_name})
        previous_revision = runtime_row.get("serving_revision")
        ready_worker_ids: list[str] = []
        ready_workers = 0
        accepting_workers = 0
        previous_accepting_workers = 0
        for row in worker_rows:
            if not _row_is_online(row, now_ms=now_ms):
                continue
            worker_queues = _queue_set(row, "queues")
            accepting_queues = _queue_set(row, "accepting_queues")
            if queue_name not in worker_queues:
                continue
            if row.get("worker_revision") == revision:
                ready_workers += 1
                worker_id = str(row.get("worker_id") or "").strip()
                if worker_id:
                    ready_worker_ids.append(worker_id)
                if queue_name in accepting_queues:
                    accepting_workers += 1
            if previous_revision and row.get("worker_revision") == previous_revision:
                if queue_name in accepting_queues:
                    previous_accepting_workers += 1

        ready_worker_ids.sort()
        return (
            runtime_row,
            ready_workers,
            accepting_workers,
            previous_accepting_workers,
            ready_worker_ids,
        )

    async def _wait_for_accepting(queue_name: str) -> tuple[int, list[str]]:
        deadline = time.monotonic() + normalized_timeout_seconds
        accepting_workers = 0
        worker_ids: list[str] = []
        while True:
            _row, ready_workers, accepting_workers, _previous_accepting, worker_ids = (
                await _snapshot(queue_name)
            )
            if accepting_workers >= normalized_min_accepting_workers:
                return accepting_workers, worker_ids
            if ready_workers < normalized_min_ready_workers:
                return accepting_workers, worker_ids
            if time.monotonic() >= deadline:
                return accepting_workers, worker_ids
            await asyncio.sleep(normalized_poll_interval_seconds)

    results: list[ServingRevisionRecovery] = []
    for queue_name in target_queue_names:
        (
            runtime_row,
            ready_workers,
            accepting_workers,
            previous_accepting_workers,
            worker_ids,
        ) = await _snapshot(queue_name)
        previous_revision = runtime_row.get("serving_revision")
        stream_ready = int(runtime_row.get("stream_length", 0))
        delayed = int(runtime_row.get("delayed_count", 0))
        waiting_not_running = stream_ready + delayed
        reason = "not_blocked"
        updated = False

        if previous_revision == revision:
            reason = "already_serving_revision"
        elif waiting_not_running <= 0:
            reason = "no_waiting_backlog"
        elif previous_accepting_workers > 0:
            reason = "previous_revision_still_accepting"
        elif ready_workers < normalized_min_ready_workers:
            reason = "target_revision_not_ready"
        else:
            if previous_revision is None:
                serving_after_ensure = await broker.ensure_serving_revision(queue_name, revision)
                updated = serving_after_ensure == revision
            else:
                updated = await broker.promote_serving_revision(
                    queue_name,
                    revision,
                    expected_revision=previous_revision,
                )
            if updated:
                accepting_workers, worker_ids = await _wait_for_accepting(queue_name)
                reason = (
                    "recovered"
                    if accepting_workers >= normalized_min_accepting_workers
                    else "promoted_but_accepting_timeout"
                )
            else:
                reason = "serving_revision_changed"

        serving_revision = await broker.get_serving_revision(queue_name)
        results.append(
            ServingRevisionRecovery(
                namespace=namespace_value,
                queue_name=queue_name,
                requested_revision=revision,
                previous_revision=previous_revision,
                serving_revision=serving_revision,
                updated=updated,
                reason=reason,
                waiting_not_running=waiting_not_running,
                ready_workers=ready_workers,
                accepting_workers=accepting_workers,
                previous_accepting_workers=previous_accepting_workers,
                worker_ids=worker_ids,
                waited_seconds=max(time.monotonic() - start, 0.0),
            )
        )

    return results


async def list_dead_letters(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
) -> DeadLetterList:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        return DeadLetterList(
            namespace=namespace,
            queue_name=queue_name,
            records=await broker.get_dead_letter_records(queue_name),
        )
    finally:
        await broker.close()


async def get_dead_letter(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    dead_letter_id: str,
) -> DeadLetterStatus:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        return DeadLetterStatus(
            namespace=namespace,
            queue_name=queue_name,
            record=await broker.get_dead_letter_record(queue_name, dead_letter_id),
        )
    finally:
        await broker.close()


async def requeue_dead_letter(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    dead_letter_id: str,
    note: Optional[str] = None,
) -> DeadLetterResolution:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        record = await broker.requeue_dead_letter(queue_name, dead_letter_id, note=note)
        return DeadLetterResolution(
            namespace=namespace,
            queue_name=queue_name,
            dead_letter_id=dead_letter_id,
            action="requeue",
            updated=record is not None and record.resolution_state == "requeued",
            record=record,
        )
    finally:
        await broker.close()


async def purge_dead_letter(
    redis_url: str,
    *,
    namespace: str,
    queue_name: str,
    dead_letter_id: str,
    note: Optional[str] = None,
) -> DeadLetterResolution:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        record = await broker.purge_dead_letter(queue_name, dead_letter_id, note=note)
        return DeadLetterResolution(
            namespace=namespace,
            queue_name=queue_name,
            dead_letter_id=dead_letter_id,
            action="purge",
            updated=record is not None and record.resolution_state == "purged",
            record=record,
        )
    finally:
        await broker.close()


async def get_runtime_status(
    redis_url: str,
    *,
    namespace: str,
    queues: Optional[list[str]] = None,
    worker_stale_after_ms: Optional[int] = None,
    pending_idle_threshold_ms: Optional[int] = None,
) -> RuntimeStatus:
    broker = RedisBroker(redis_url, namespace=namespace)
    try:
        now_ms = int(time.time() * 1000)
        effective_worker_stale_after_ms = (
            max(int(worker_stale_after_ms), 1)
            if worker_stale_after_ms is not None
            else broker.worker_presence_ttl_ms * 2
        )
        effective_pending_idle_threshold_ms = (
            max(int(pending_idle_threshold_ms), 1)
            if pending_idle_threshold_ms is not None
            else max(int(broker.lease_seconds * 1000 * 2), 1)
        )

        requested_queues = {queue for queue in (queues or []) if queue}
        known_queues = set(await broker.list_runtime_queues())
        if requested_queues:
            known_queues.update(requested_queues)

        worker_rows = await broker.list_worker_runtime_rows(
            queue_names=requested_queues if requested_queues else None,
        )
        workers: list[WorkerRuntimeStatus] = []
        queue_names_from_workers: set[str] = set()
        base_runtime_fields = {
            "worker_id",
            "worker_revision",
            "last_seen_ms",
            "hostname",
            "pid",
            "queues",
            "accepting_queues",
        }

        for row in worker_rows:
            worker_id = row.get("worker_id")
            if not worker_id:
                continue
            worker_queues = _split_csv(row.get("queues"))
            accepting_queues = _split_csv(row.get("accepting_queues"))
            queue_names_from_workers.update(worker_queues)
            if requested_queues and not requested_queues.intersection(worker_queues):
                continue

            last_seen_ms = _to_int(row.get("last_seen_ms"))
            age_ms = None if last_seen_ms is None else max(now_ms - last_seen_ms, 0)
            status = "online"
            if age_ms is not None and age_ms > effective_worker_stale_after_ms:
                status = "stale"

            runtime_payload: dict[str, Any] = {}
            for key, value in row.items():
                if key in base_runtime_fields:
                    continue
                runtime_payload[key] = _parse_runtime_value(value)

            workers.append(
                WorkerRuntimeStatus(
                    namespace=namespace,
                    worker_id=worker_id,
                    worker_revision=row.get("worker_revision"),
                    status=status,
                    last_seen_ms=last_seen_ms,
                    age_ms=age_ms,
                    hostname=row.get("hostname", ""),
                    pid=_to_int(row.get("pid")),
                    queues=worker_queues,
                    accepting_queues=accepting_queues,
                    runtime=runtime_payload,
                )
            )

        known_queues.update(queue_names_from_workers)
        queue_list = sorted(requested_queues if requested_queues else known_queues)
        queue_statuses: list[QueueRuntimeStatus] = []
        warning_statuses: set[str] = set()
        totals = {
            "workers_total": len(workers),
            "workers_online": sum(1 for worker in workers if worker.status == "online"),
            "workers_stale": sum(1 for worker in workers if worker.status == "stale"),
            "queues_total": len(queue_list),
            "stream_ready": 0,
            "delayed": 0,
            "pending": 0,
            "pending_stale": 0,
            "waiting_not_running": 0,
        }

        for queue_name in queue_list:
            row = await broker.get_queue_runtime_row(
                queue_name,
                pending_idle_threshold_ms=effective_pending_idle_threshold_ms,
            )

            stream_ready = int(row["stream_length"])
            delayed = int(row["delayed_count"])
            pending = int(row["pending_count"])
            pending_stale = int(row["pending_stale_count"])
            waiting_not_running = stream_ready + delayed

            workers_for_queue = [worker for worker in workers if queue_name in worker.queues]
            workers_total = len(workers_for_queue)
            workers_accepting = sum(
                1
                for worker in workers_for_queue
                if worker.status == "online" and queue_name in worker.accepting_queues
            )
            workers_online = sum(1 for worker in workers_for_queue if worker.status == "online")
            workers_draining = max(workers_online - workers_accepting, 0)

            queue_status = "ok"
            if waiting_not_running > 0 and workers_accepting == 0:
                queue_status = "blocked"
            elif pending_stale > 0:
                queue_status = "stalled"
            elif pending > 0 and workers_online == 0:
                queue_status = "orphaned_pending"

            if queue_status != "ok":
                warning_statuses.add(queue_status)

            queue_statuses.append(
                QueueRuntimeStatus(
                    namespace=namespace,
                    queue_name=queue_name,
                    serving_revision=row["serving_revision"],
                    status=queue_status,
                    stream_ready=stream_ready,
                    delayed=delayed,
                    pending=pending,
                    pending_stale=pending_stale,
                    waiting_not_running=waiting_not_running,
                    workers_total=workers_total,
                    workers_accepting=workers_accepting,
                    workers_draining=workers_draining,
                )
            )

            totals["stream_ready"] += stream_ready
            totals["delayed"] += delayed
            totals["pending"] += pending
            totals["pending_stale"] += pending_stale
            totals["waiting_not_running"] += waiting_not_running

        overall_status = "ok"
        if totals["workers_online"] == 0 and totals["waiting_not_running"] > 0:
            overall_status = "critical"
        elif warning_statuses.intersection({"blocked", "orphaned_pending"}):
            overall_status = "critical"
        elif warning_statuses or totals["workers_stale"] > 0:
            overall_status = "degraded"

        return RuntimeStatus(
            namespace=namespace,
            generated_at_ms=now_ms,
            overall_status=overall_status,
            workers=workers,
            queues=queue_statuses,
            totals=totals,
        )
    finally:
        await broker.close()


async def get_runtime_health(
    redis_url: str,
    *,
    namespace: str,
    queues: Optional[list[str]] = None,
    worker_stale_after_ms: Optional[int] = None,
    pending_idle_threshold_ms: Optional[int] = None,
) -> dict[str, Any]:
    status = await get_runtime_status(
        redis_url,
        namespace=namespace,
        queues=queues,
        worker_stale_after_ms=worker_stale_after_ms,
        pending_idle_threshold_ms=pending_idle_threshold_ms,
    )
    payload = _runtime_payload(status)
    payload["healthy"] = status.overall_status == "ok"
    return payload


def runtime_status_to_dict(status: RuntimeStatus) -> dict[str, Any]:
    return _runtime_payload(status)
