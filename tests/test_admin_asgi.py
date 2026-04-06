from __future__ import annotations

import unittest

import orjson

import fluxera


async def _asgi_get(app, path: str, *, root_path: str = "") -> tuple[int, dict[str, str], bytes]:
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
        "path": path,
        "root_path": root_path,
        "query_string": b"",
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

        async def snapshot_loader(_query_params):
            return payload

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
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

        status, _headers, body = await _asgi_get(app, "/healthz")
        self.assertEqual(status, 200)
        decoded = orjson.loads(body)
        self.assertEqual(decoded["status"], "ok")
        self.assertTrue(decoded["healthy"])

    async def test_health_returns_503_when_unhealthy(self) -> None:
        async def snapshot_loader(_query_params):
            return {
                "namespace": "unit",
                "generated_at_ms": 1,
                "overall_status": "critical",
                "healthy": False,
                "totals": {"workers_total": 0, "workers_online": 0, "workers_stale": 0, "queues_total": 1},
                "workers": [],
                "queues": [],
            }

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            snapshot_loader=snapshot_loader,
        )
        status, _headers, _body = await _asgi_get(app, "/healthz")
        self.assertEqual(status, 503)

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

        app = fluxera.FluxeraAdminASGI(
            redis_url="redis://unused",
            namespace="unit",
            mount_path="/admin/fluxera",
            snapshot_loader=snapshot_loader,
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
