from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
import subprocess
import sys
from collections.abc import Iterable
from typing import Any

import orjson
import redis

from .admin import (
    ensure_serving_revision,
    get_dead_letter,
    get_serving_revision,
    list_dead_letters,
    promote_serving_revision,
    purge_dead_letter,
    requeue_dead_letter,
)
from .broker import Broker, get_broker
from .dead_letters import DeadLetterRecord
from .rate_limits import ConcurrentRateLimiter
from .runtime.worker import Worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fluxera", description="Fluxera admin CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    revision_parser = subparsers.add_parser("revision", help="Manage queue serving revisions.")
    revision_subparsers = revision_parser.add_subparsers(dest="revision_command", required=True)

    get_parser = revision_subparsers.add_parser("get", help="Read the current serving revision.")
    _add_revision_target_arguments(get_parser)
    get_parser.set_defaults(handler=_handle_revision_get)

    ensure_parser = revision_subparsers.add_parser("ensure", help="Bootstrap a serving revision if it is missing.")
    _add_revision_target_arguments(ensure_parser)
    ensure_parser.add_argument("--revision", required=True, help="Revision to set when the queue is uninitialized.")
    ensure_parser.set_defaults(handler=_handle_revision_ensure)

    promote_parser = revision_subparsers.add_parser("promote", help="Promote a queue to a new serving revision.")
    _add_revision_target_arguments(promote_parser)
    promote_parser.add_argument("--revision", required=True, help="Revision that should become serving.")
    promote_parser.add_argument(
        "--expected-revision",
        help="Require the current serving revision to match before promoting.",
    )
    promote_parser.set_defaults(handler=_handle_revision_promote)

    dlq_parser = subparsers.add_parser("dlq", help="Inspect and manage dead letters.")
    dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_command", required=True)

    dlq_list_parser = dlq_subparsers.add_parser("list", help="List active dead letters for a queue.")
    _add_dlq_target_arguments(dlq_list_parser)
    dlq_list_parser.set_defaults(handler=_handle_dlq_list)

    dlq_get_parser = dlq_subparsers.add_parser("get", help="Get a specific dead letter record.")
    _add_dlq_target_arguments(dlq_get_parser)
    dlq_get_parser.add_argument("--dead-letter-id", required=True, help="Dead letter record id.")
    dlq_get_parser.set_defaults(handler=_handle_dlq_get)

    dlq_requeue_parser = dlq_subparsers.add_parser("requeue", help="Requeue a dead letter back onto its queue.")
    _add_dlq_target_arguments(dlq_requeue_parser)
    dlq_requeue_parser.add_argument("--dead-letter-id", required=True, help="Dead letter record id.")
    dlq_requeue_parser.add_argument("--note", help="Optional resolution note.")
    dlq_requeue_parser.set_defaults(handler=_handle_dlq_requeue)

    dlq_purge_parser = dlq_subparsers.add_parser("purge", help="Mark a dead letter as purged and remove it from the active DLQ.")
    _add_dlq_target_arguments(dlq_purge_parser)
    dlq_purge_parser.add_argument("--dead-letter-id", required=True, help="Dead letter record id.")
    dlq_purge_parser.add_argument("--note", help="Optional resolution note.")
    dlq_purge_parser.set_defaults(handler=_handle_dlq_purge)

    worker_parser = subparsers.add_parser("worker", help="Run a Fluxera worker from imported modules.")
    worker_parser.add_argument(
        "modules",
        nargs="*",
        help="Modules to import before starting the worker. Import your setup module here.",
    )
    worker_parser.add_argument(
        "--module-registry",
        action="append",
        default=[],
        help="Module attribute containing an iterable of worker module names, for example package.worker_registry:WORKER_MODULES.",
    )
    worker_parser.add_argument(
        "--broker",
        help="Optional broker object reference in module:attr form. Defaults to the global broker after imports.",
    )
    worker_parser.add_argument(
        "--queue",
        dest="queues",
        action="append",
        default=[],
        help="Restrict the worker to specific queues. Can be passed more than once.",
    )
    worker_parser.add_argument(
        "--concurrency",
        type=int,
        default=_int_env("FLUXERA_CONCURRENCY"),
        help="Default async concurrency. Defaults to FLUXERA_CONCURRENCY or Worker default.",
    )
    worker_parser.add_argument(
        "--async-concurrency",
        type=int,
        default=_int_env("FLUXERA_ASYNC_CONCURRENCY"),
        help="Explicit async lane concurrency.",
    )
    worker_parser.add_argument(
        "--thread-concurrency",
        type=int,
        default=_int_env("FLUXERA_THREAD_CONCURRENCY"),
        help="Thread lane concurrency.",
    )
    worker_parser.add_argument(
        "--process-concurrency",
        type=int,
        default=_int_env("FLUXERA_PROCESS_CONCURRENCY"),
        help="Process lane concurrency.",
    )
    worker_parser.add_argument(
        "--prefetch",
        type=int,
        default=_int_env("FLUXERA_PREFETCH"),
        help="Consumer prefetch size.",
    )
    worker_parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=_int_env("FLUXERA_POLL_TIMEOUT_MS"),
        help="Poll timeout in milliseconds.",
    )
    worker_parser.add_argument(
        "--ready-queue-size",
        type=int,
        default=_int_env("FLUXERA_READY_QUEUE_SIZE"),
        help="Per-lane ready queue size.",
    )
    worker_parser.add_argument(
        "--max-in-flight",
        type=int,
        default=_int_env("FLUXERA_MAX_IN_FLIGHT"),
        help="Maximum total admitted deliveries.",
    )
    worker_parser.add_argument(
        "--process-start-method",
        default=os.environ.get("FLUXERA_PROCESS_START_METHOD"),
        help="Process start method override.",
    )
    worker_parser.add_argument(
        "--worker-revision",
        default=os.environ.get("FLUXERA_WORKER_REVISION"),
        help="Worker revision to register for rollout control.",
    )
    worker_parser.add_argument(
        "--worker-id",
        default=os.environ.get("FLUXERA_WORKER_ID"),
        help="Explicit worker id.",
    )
    worker_parser.add_argument(
        "--revision-poll-interval",
        type=float,
        default=_float_env("FLUXERA_REVISION_POLL_INTERVAL"),
        help="Revision heartbeat interval in seconds.",
    )
    worker_parser.add_argument(
        "--uvloop",
        action="store_true",
        default=_bool_env("FLUXERA_USE_UVLOOP"),
        help="Install uvloop before running the worker when available.",
    )
    worker_parser.add_argument(
        "--exit-when-idle",
        action="store_true",
        help="Exit once the currently queued work has drained. Useful for smoke tests.",
    )
    worker_parser.set_defaults(handler=_handle_worker)

    rate_limit_parser = subparsers.add_parser("rate-limit", help="Use the distributed concurrency limiter from the CLI.")
    rate_limit_subparsers = rate_limit_parser.add_subparsers(dest="rate_limit_command", required=True)

    probe_parser = rate_limit_subparsers.add_parser(
        "probe",
        help="Try to acquire a limiter key and release it immediately.",
    )
    _add_rate_limit_arguments(probe_parser, include_format=True)
    probe_parser.set_defaults(handler=_handle_rate_limit_probe)

    run_parser = rate_limit_subparsers.add_parser(
        "run",
        help="Run a command while holding a distributed limiter slot.",
    )
    _add_rate_limit_arguments(run_parser, include_format=False)
    run_parser.add_argument(
        "run_command",
        nargs=argparse.REMAINDER,
        help="Command to execute after '--'.",
    )
    run_parser.set_defaults(handler=_handle_rate_limit_run)

    return parser


def _add_revision_target_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("FLUXERA_REDIS_URL"),
        help="Redis connection URL. Defaults to FLUXERA_REDIS_URL.",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("FLUXERA_NAMESPACE", "fluxera"),
        help="Fluxera namespace. Defaults to FLUXERA_NAMESPACE or 'fluxera'.",
    )
    parser.add_argument("--queue", required=True, help="Queue name to inspect or update.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format.",
        )


def _add_dlq_target_arguments(parser: argparse.ArgumentParser) -> None:
    _add_revision_target_arguments(parser)


def _int_env(name: str) -> int | None:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return None
    return int(raw_value)


def _float_env(name: str) -> float | None:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return None
    return float(raw_value)


def _bool_env(name: str) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return False
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _require_redis_url(args: argparse.Namespace) -> str:
    if args.redis_url:
        return args.redis_url
    raise SystemExit("error: --redis-url is required when FLUXERA_REDIS_URL is not set")


def _emit(args: argparse.Namespace, payload: dict[str, object]) -> None:
    if getattr(args, "format", "text") == "json":
        print(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS).decode("utf-8"))
        return

    for key, value in payload.items():
        print(f"{key}={value}")


def _dead_letter_payload(record: DeadLetterRecord | None) -> dict[str, object] | None:
    if record is None:
        return None
    payload = record.to_dict()
    message_snapshot = payload.get("message_snapshot")
    if isinstance(message_snapshot, dict):
        payload["message_snapshot_present"] = True
        payload["message_snapshot"] = {
            "queue_name": message_snapshot.get("queue_name"),
            "actor_name": message_snapshot.get("actor_name"),
            "message_id": message_snapshot.get("message_id"),
            "message_timestamp": message_snapshot.get("message_timestamp"),
        }
    else:
        payload["message_snapshot_present"] = False
    return payload


def _add_rate_limit_arguments(parser: argparse.ArgumentParser, *, include_format: bool) -> None:
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("FLUXERA_REDIS_URL"),
        help="Redis connection URL. Defaults to FLUXERA_REDIS_URL.",
    )
    parser.add_argument("--key", required=True, help="Limiter key.")
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum concurrent holders. Defaults to 1.",
    )
    parser.add_argument(
        "--ttl-ms",
        type=int,
        help="Override the limiter TTL in milliseconds.",
    )
    parser.add_argument(
        "--prefix",
        default="fluxera:concurrency",
        help="Redis key prefix. Use an empty string to disable the prefix.",
    )
    if include_format:
        parser.add_argument(
            "--format",
            choices=("text", "json"),
            default="text",
            help="Output format.",
        )


def _build_rate_limiter(args: argparse.Namespace) -> tuple[redis.Redis, ConcurrentRateLimiter]:
    client = redis.Redis.from_url(_require_redis_url(args))
    limiter = ConcurrentRateLimiter(
        client,
        args.key,
        limit=args.limit,
        ttl_ms=args.ttl_ms,
        prefix=args.prefix,
    )
    return client, limiter


def _load_object_ref(reference: str) -> Any:
    module_name, separator, attr_name = reference.partition(":")
    if not module_name or not separator or not attr_name:
        raise SystemExit(f"error: expected module:attr reference, got {reference!r}")

    module = importlib.import_module(module_name)
    target = getattr(module, attr_name, None)
    if target is None:
        raise SystemExit(f"error: {reference!r} could not be resolved")
    if callable(target):
        return target()
    return target


def _iter_module_names(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if not isinstance(value, Iterable):
        raise SystemExit("error: module registry must resolve to a string or iterable of strings")

    module_names: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise SystemExit("error: module registry entries must be strings")
        module_names.append(item)
    return module_names


def _install_uvloop() -> None:
    try:
        import uvloop
    except Exception:
        return

    uvloop.install()


def _resolve_worker_broker(args: argparse.Namespace) -> Broker:
    if args.broker:
        broker = _load_object_ref(args.broker)
    else:
        broker = get_broker()

    if not isinstance(broker, Broker):
        raise SystemExit("error: resolved broker is not a Fluxera Broker instance")
    return broker


def _import_worker_modules(args: argparse.Namespace) -> list[str]:
    imported_names: list[str] = []
    seen: set[str] = set()

    for module_name in args.modules:
        importlib.import_module(module_name)
        if module_name not in seen:
            imported_names.append(module_name)
            seen.add(module_name)

    for registry_ref in args.module_registry:
        for module_name in _iter_module_names(_load_object_ref(registry_ref)):
            importlib.import_module(module_name)
            if module_name not in seen:
                imported_names.append(module_name)
                seen.add(module_name)

    return imported_names


async def _wait_for_stop_signal() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for signame in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signame, _request_stop)
        except (NotImplementedError, RuntimeError):
            continue

    await stop_event.wait()


def _build_worker(args: argparse.Namespace, broker: Broker) -> Worker:
    worker_kwargs: dict[str, Any] = {}
    if args.queues:
        worker_kwargs["queues"] = set(args.queues)
    if args.concurrency is not None:
        worker_kwargs["concurrency"] = args.concurrency
    if args.async_concurrency is not None:
        worker_kwargs["async_concurrency"] = args.async_concurrency
    if args.thread_concurrency is not None:
        worker_kwargs["thread_concurrency"] = args.thread_concurrency
    if args.process_concurrency is not None:
        worker_kwargs["process_concurrency"] = args.process_concurrency
    if args.prefetch is not None:
        worker_kwargs["prefetch"] = args.prefetch
    if args.poll_timeout_ms is not None:
        worker_kwargs["poll_timeout"] = args.poll_timeout_ms / 1000
    if args.ready_queue_size is not None:
        worker_kwargs["ready_queue_size"] = args.ready_queue_size
    if args.max_in_flight is not None:
        worker_kwargs["max_in_flight"] = args.max_in_flight
    if args.process_start_method is not None:
        worker_kwargs["process_start_method"] = args.process_start_method
    if args.worker_revision is not None:
        worker_kwargs["worker_revision"] = args.worker_revision
    if args.worker_id is not None:
        worker_kwargs["worker_id"] = args.worker_id
    if args.revision_poll_interval is not None:
        worker_kwargs["revision_poll_interval"] = args.revision_poll_interval
    return Worker(broker, **worker_kwargs)


async def _handle_revision_get(args: argparse.Namespace) -> int:
    status = await get_serving_revision(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
    )
    _emit(
        args,
        {
            "namespace": status.namespace,
            "queue": status.queue_name,
            "serving_revision": status.serving_revision,
            "exists": status.serving_revision is not None,
        },
    )
    return 0


async def _handle_revision_ensure(args: argparse.Namespace) -> int:
    status = await ensure_serving_revision(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
        revision=args.revision,
    )
    _emit(
        args,
        {
            "namespace": status.namespace,
            "queue": status.queue_name,
            "serving_revision": status.serving_revision,
        },
    )
    return 0


async def _handle_revision_promote(args: argparse.Namespace) -> int:
    result = await promote_serving_revision(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
        revision=args.revision,
        expected_revision=args.expected_revision,
    )
    _emit(
        args,
        {
            "namespace": result.namespace,
            "queue": result.queue_name,
            "requested_revision": result.requested_revision,
            "previous_revision": result.previous_revision,
            "serving_revision": result.serving_revision,
            "expected_revision": result.expected_revision,
            "updated": result.updated,
        },
    )
    return 0 if result.updated else 2


async def _handle_dlq_list(args: argparse.Namespace) -> int:
    listing = await list_dead_letters(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
    )
    _emit(
        args,
        {
            "namespace": listing.namespace,
            "queue": listing.queue_name,
            "count": len(listing.records),
            "items": [_dead_letter_payload(record) for record in listing.records],
        },
    )
    return 0


async def _handle_dlq_get(args: argparse.Namespace) -> int:
    status = await get_dead_letter(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
        dead_letter_id=args.dead_letter_id,
    )
    _emit(
        args,
        {
            "namespace": status.namespace,
            "queue": status.queue_name,
            "dead_letter_id": args.dead_letter_id,
            "exists": status.record is not None,
            "record": _dead_letter_payload(status.record),
        },
    )
    return 0 if status.record is not None else 4


async def _handle_dlq_requeue(args: argparse.Namespace) -> int:
    result = await requeue_dead_letter(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
        dead_letter_id=args.dead_letter_id,
        note=args.note,
    )
    _emit(
        args,
        {
            "namespace": result.namespace,
            "queue": result.queue_name,
            "dead_letter_id": result.dead_letter_id,
            "action": result.action,
            "updated": result.updated,
            "record": _dead_letter_payload(result.record),
        },
    )
    return 0 if result.updated else 4


async def _handle_dlq_purge(args: argparse.Namespace) -> int:
    result = await purge_dead_letter(
        _require_redis_url(args),
        namespace=args.namespace,
        queue_name=args.queue,
        dead_letter_id=args.dead_letter_id,
        note=args.note,
    )
    _emit(
        args,
        {
            "namespace": result.namespace,
            "queue": result.queue_name,
            "dead_letter_id": result.dead_letter_id,
            "action": result.action,
            "updated": result.updated,
            "record": _dead_letter_payload(result.record),
        },
    )
    return 0 if result.updated else 4


async def _handle_rate_limit_probe(args: argparse.Namespace) -> int:
    client, limiter = _build_rate_limiter(args)
    try:
        with limiter.acquire(raise_on_failure=False) as acquired:
            _emit(
                args,
                {
                    "key": limiter.key,
                    "requested_key": args.key,
                    "limit": limiter.limit,
                    "ttl_ms": limiter.ttl_ms,
                    "acquired": acquired,
                },
            )
            return 0 if acquired else 3
    finally:
        client.close()


async def _handle_rate_limit_run(args: argparse.Namespace) -> int:
    command = list(args.run_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("error: rate-limit run requires a command after '--'")

    client, limiter = _build_rate_limiter(args)
    try:
        with limiter.acquire(raise_on_failure=False) as acquired:
            if not acquired:
                print(
                    f"busy: key={limiter.key} limit={limiter.limit} ttl_ms={limiter.ttl_ms}",
                    file=sys.stderr,
                )
                return 3

            completed = await asyncio.to_thread(
                subprocess.run,
                command,
                check=False,
            )
            return int(completed.returncode)
    finally:
        client.close()


async def _handle_worker(args: argparse.Namespace) -> int:
    _import_worker_modules(args)
    broker = _resolve_worker_broker(args)
    if not broker.get_declared_queues():
        raise SystemExit("error: no queues were declared. Import actor modules or pass --module-registry.")

    worker = _build_worker(args, broker)
    await worker.start()
    try:
        if args.exit_when_idle:
            await worker.join()
            return 0

        await _wait_for_stop_signal()
        return 0
    finally:
        await worker.stop()


async def _run(args: argparse.Namespace) -> int:
    return await args.handler(args)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "command", None) == "worker" and getattr(args, "uvloop", False):
        _install_uvloop()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130
    except SystemExit:
        raise
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
