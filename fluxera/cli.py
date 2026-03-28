from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from .admin import ensure_serving_revision, get_serving_revision, promote_serving_revision


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


def _require_redis_url(args: argparse.Namespace) -> str:
    if args.redis_url:
        return args.redis_url
    raise SystemExit("error: --redis-url is required when FLUXERA_REDIS_URL is not set")


def _emit(args: argparse.Namespace, payload: dict[str, object]) -> None:
    if args.format == "json":
        print(json.dumps(payload, sort_keys=True))
        return

    for key, value in payload.items():
        print(f"{key}={value}")


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


async def _run(args: argparse.Namespace) -> int:
    return await args.handler(args)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
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
