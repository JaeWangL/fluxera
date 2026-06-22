from __future__ import annotations

import asyncio
import unittest
from urllib.parse import urlsplit

import orjson

import fluxera


async def _asgi_get(app, path: str, *, root_path: str = "") -> tuple[int, dict[str, str], bytes]:
    parsed = urlsplit(path)
    sent_messages: list[dict] = []
    receive_events = [{"type": "http.request", "body": b"", "more_body": False}]

    async def receive():
        if receive_events:
            return receive_events.pop(0)
        return {"type": "http.disconnect"}

    async def send(message):
        sent_messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": parsed.path,
        "root_path": root_path,
        "query_string": parsed.query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8000),
    }
    await app(scope, receive, send)

    start = next(message for message in sent_messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in sent_messages if message["type"] == "http.response.body")
    headers = {key.decode("utf-8"): value.decode("utf-8") for key, value in start.get("headers", [])}
    return int(start["status"]), headers, body


class FluxeraAdminASGITests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_and_dashboard_and_health_endpoints(self) -> None:
        payload = {
            "namespace": "unit",
            "generated_at_ms": 1,
            "overall_status": "ok",
            "healthy": True,
            "totals": {"workers_total": 1, "workers_online": 1, "workers_stale": 0, "queues_total": 1},
            "workers": [],
            "queues": [],
        }
        readiness_payload = {
            "namespace": "unit",
            "generated_at_ms": 2,
            "status": "ok",
            "healthy": True,
            "redis": {"ping": True, "latency_ms": 1.0},
        }
        snapshot_calls = 0

        async def snapshot_loader(_query_params):
            nonlocal snapshot_calls
            snapshot_calls += 1
            return payload

        async def readiness_loader():
            return readiness_payload

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
            readiness_loader=readiness_loader,
        )

        status, headers, body = await _asgi_get(app, "/")
        self.assertEqual(status, 200)
        self.assertTrue(headers["content-type"].startswith("text/html"))
        self.assertIn(b"Fluxera Runtime Admin", body)

        status, headers, body = await _asgi_get(app, "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "application/json")
        decoded = orjson.loads(body)
        self.assertEqual(decoded["namespace"], "unit")
        self.assertTrue(decoded["healthy"])
        self.assertEqual(decoded["snapshot_cache"]["state"], "refresh")
        self.assertEqual(snapshot_calls, 1)

        status, _headers, body = await _asgi_get(app, "/healthz")
        self.assertEqual(status, 200)
        decoded = orjson.loads(body)
        self.assertEqual(decoded["status"], "ok")
        self.assertTrue(decoded["healthy"])
        self.assertEqual(decoded["redis"]["ping"], True)
        self.assertEqual(snapshot_calls, 1)

    async def test_health_returns_503_when_unhealthy(self) -> None:
        async def readiness_loader():
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "status": "redis_unreachable",
                "healthy": False,
                "redis": {"ping": False, "latency_ms": 1.0, "error_type": "TimeoutError"},
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            readiness_loader=readiness_loader,
        )
        status, _headers, _body = await _asgi_get(app, "/healthz")
        self.assertEqual(status, 503)

    async def test_health_loader_exception_returns_503_payload(self) -> None:
        async def readiness_loader():
            raise TimeoutError("redis ping timed out")

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            readiness_loader=readiness_loader,
        )

        status, _headers, body = await _asgi_get(app, "/healthz")
        decoded = orjson.loads(body)
        self.assertEqual(status, 503)
        self.assertFalse(decoded["healthy"])
        self.assertEqual(decoded["status"], "redis_unreachable")
        self.assertEqual(decoded["redis"]["error_type"], "TimeoutError")

    async def test_snapshot_uses_recent_success_cache(self) -> None:
        calls = 0

        async def snapshot_loader(_query_params):
            nonlocal calls
            calls += 1
            return {
                "namespace": "unit",
                "generated_at_ms": calls,
                "overall_status": "ok",
                "healthy": True,
                "totals": {"workers_total": calls},
                "workers": [],
                "queues": [],
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
            snapshot_cache_seconds=25,
        )

        status, _headers, body = await _asgi_get(app, "/snapshot")
        self.assertEqual(status, 200)
        first = orjson.loads(body)
        self.assertEqual(first["generated_at_ms"], 1)
        self.assertEqual(first["snapshot_cache"]["state"], "refresh")

        status, _headers, body = await _asgi_get(app, "/snapshot")
        self.assertEqual(status, 200)
        second = orjson.loads(body)
        self.assertEqual(second["generated_at_ms"], 1)
        self.assertEqual(second["snapshot_cache"]["state"], "hit")
        self.assertEqual(calls, 1)

    async def test_snapshot_timeout_returns_degraded_payload(self) -> None:
        async def snapshot_loader(_query_params):
            await asyncio.sleep(1)
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "overall_status": "ok",
                "healthy": True,
                "totals": {},
                "workers": [],
                "queues": [],
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
            snapshot_timeout_seconds=0.01,
        )

        status, _headers, body = await _asgi_get(app, "/snapshot")
        decoded = orjson.loads(body)
        self.assertEqual(status, 503)
        self.assertFalse(decoded["healthy"])
        self.assertEqual(decoded["overall_status"], "degraded")
        self.assertEqual(decoded["diagnostics"]["error_type"], "TimeoutError")

    async def test_not_found_endpoint_returns_404(self) -> None:
        async def snapshot_loader(_query_params):
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "overall_status": "ok",
                "healthy": True,
                "totals": {"workers_total": 0, "workers_online": 0, "workers_stale": 0, "queues_total": 0},
                "workers": [],
                "queues": [],
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
        )
        status, _headers, body = await _asgi_get(app, "/unknown")
        self.assertEqual(status, 404)
        self.assertEqual(orjson.loads(body)["error"], "not_found")

    async def test_mounted_path_scope_is_normalized(self) -> None:
        async def snapshot_loader(_query_params):
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "overall_status": "ok",
                "healthy": True,
                "totals": {"workers_total": 0, "workers_online": 0, "workers_stale": 0, "queues_total": 0},
                "workers": [],
                "queues": [],
            }

        async def readiness_loader():
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "status": "ok",
                "healthy": True,
                "redis": {"ping": True, "latency_ms": 1.0},
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            mount_path="/admin/fluxera",
            snapshot_loader=snapshot_loader,
            readiness_loader=readiness_loader,
        )

        status, _headers, body = await _asgi_get(
            app,
            "/admin/fluxera",
            root_path="/admin/fluxera",
        )
        self.assertEqual(status, 200)
        self.assertIn(b"Fluxera Runtime Admin", body)

        status, _headers, body = await _asgi_get(
            app,
            "/admin/fluxera/snapshot",
            root_path="/admin/fluxera",
        )
        self.assertEqual(status, 200)
        self.assertEqual(orjson.loads(body)["namespace"], "unit")

        status, _headers, body = await _asgi_get(
            app,
            "/admin/fluxera/healthz",
            root_path="/admin/fluxera",
        )
        self.assertEqual(status, 200)
        self.assertTrue(orjson.loads(body)["healthy"])

    async def test_mount_path_fallback_when_root_path_missing(self) -> None:
        async def snapshot_loader(_query_params):
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "overall_status": "ok",
                "healthy": True,
                "totals": {"workers_total": 0, "workers_online": 0, "workers_stale": 0, "queues_total": 0},
                "workers": [],
                "queues": [],
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            mount_path="/admin/fluxera",
            snapshot_loader=snapshot_loader,
        )

        status, _headers, body = await _asgi_get(app, "/admin/fluxera/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(orjson.loads(body)["namespace"], "unit")


class FluxeraRuntimeDiagnosticsTests(unittest.TestCase):
    def test_runtime_payload_includes_queue_and_worker_diagnostics(self) -> None:
        status = fluxera.RuntimeStatus(
            namespace="unit",
            generated_at_ms=123,
            overall_status="degraded",
            totals={
                "workers_total": 1,
                "workers_online": 0,
                "workers_stale": 1,
                "queues_total": 1,
                "stream_ready": 1,
                "delayed": 0,
                "pending": 0,
                "pending_stale": 0,
                "waiting_not_running": 1,
            },
            workers=[
                fluxera.WorkerRuntimeStatus(
                    namespace="unit",
                    worker_id="worker-a",
                    worker_revision="local",
                    status="stale",
                    last_seen_ms=1,
                    age_ms=90_000,
                    hostname="host-a",
                    pid=10,
                    queues=["default"],
                    accepting_queues=[],
                    runtime={
                        "revision_heartbeat_failures": 3,
                        "failed_total": 2,
                        "retried_total": 1,
                        "dead_lettered_total": 0,
                    },
                )
            ],
            queues=[
                fluxera.QueueRuntimeStatus(
                    namespace="unit",
                    queue_name="default",
                    serving_revision="rev-a",
                    status="blocked",
                    stream_ready=1,
                    delayed=0,
                    pending=0,
                    pending_stale=0,
                    waiting_not_running=1,
                    workers_total=1,
                    workers_accepting=0,
                    workers_draining=0,
                )
            ],
        )

        diagnostics = fluxera.runtime_status_to_dict(status)["diagnostics"]

        self.assertIn("worker_stale", diagnostics["summary"])
        self.assertIn("zero_accepting_backlog", diagnostics["summary"])
        self.assertIn("blocked_or_orphaned_queue", diagnostics["summary"])
        self.assertIn("revision_heartbeat_failures", diagnostics["summary"])
        self.assertIn("worker_task_failures", diagnostics["summary"])
        self.assertEqual(diagnostics["queue_issues"][0]["queue_name"], "default")
        self.assertEqual(diagnostics["stale_workers"][0]["worker_id"], "worker-a")
        self.assertEqual(
            diagnostics["revision_heartbeat_failures"][0]["revision_heartbeat_failures"],
            3,
        )


class FluxeraAdminMountTests(unittest.TestCase):
    def test_mount_admin_asgi_mounts_app(self) -> None:
        class DummyApp:
            def __init__(self) -> None:
                self.calls: list[tuple[str, object]] = []

            def mount(self, path, app):
                self.calls.append((path, app))

        dummy = DummyApp()
        admin_app = fluxera.mount_admin_asgi(
            dummy,
            mount_path="/admin",
            redis_url="redis://unused",
            namespace="unit",
        )
        self.assertEqual(len(dummy.calls), 1)
        self.assertEqual(dummy.calls[0][0], "/admin")
        self.assertIs(dummy.calls[0][1], admin_app)
