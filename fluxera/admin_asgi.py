from __future__ import annotations

import html
from http import HTTPStatus
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import parse_qs

import orjson

from .admin import get_runtime_status, runtime_status_to_dict

Headers = list[tuple[bytes, bytes]]
SnapshotLoader = Callable[[dict[str, list[str]]], Awaitable[dict[str, Any]]]


def _normalize_mount_path(path: Optional[str]) -> Optional[str]:
    if path is None:
        return None
    normalized = path.strip()
    if not normalized:
        return None
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    normalized = normalized.rstrip("/")
    return normalized or "/"


def _normalize_request_path(path: str, *, root_path: str, mount_path: Optional[str]) -> str:
    normalized_path = path or "/"
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"

    prefixes: list[str] = []
    normalized_root = _normalize_mount_path(root_path)
    if normalized_root is not None:
        prefixes.append(normalized_root)
    normalized_mount = _normalize_mount_path(mount_path)
    if normalized_mount is not None and normalized_mount not in prefixes:
        prefixes.append(normalized_mount)

    for prefix in prefixes:
        if prefix == "/":
            continue
        if normalized_path == prefix:
            return "/"
        prefixed = f"{prefix}/"
        if normalized_path.startswith(prefixed):
            relative = normalized_path[len(prefix):]
            if not relative:
                return "/"
            if not relative.startswith("/"):
                relative = f"/{relative}"
            return relative

    return normalized_path


def _dashboard_html(refresh_seconds: float) -> str:
    escaped_refresh = html.escape(str(max(refresh_seconds, 1.0)))
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Fluxera Admin</title>
    <style>
      body {{
        margin: 0;
        padding: 16px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        background: #f7f8fb;
        color: #1b1f2a;
      }}
      .card {{
        background: white;
        border: 1px solid #dce1ea;
        border-radius: 10px;
        padding: 12px;
        margin-bottom: 12px;
        overflow-x: auto;
      }}
      table {{
        border-collapse: collapse;
        min-width: 720px;
        width: 100%;
      }}
      th, td {{
        border-bottom: 1px solid #eef2f7;
        text-align: left;
        padding: 6px 8px;
        white-space: nowrap;
      }}
      th {{
        background: #fbfcfe;
      }}
      .status-ok {{ color: #1b7f3a; font-weight: 600; }}
      .status-degraded {{ color: #a56a00; font-weight: 600; }}
      .status-critical {{ color: #b42318; font-weight: 600; }}
      .muted {{ color: #5f6b7a; }}
    </style>
  </head>
  <body>
    <h2>Fluxera Runtime Admin</h2>
    <div class="muted">Auto-refresh: every {escaped_refresh}s</div>
    <div id="summary" class="card">Loading...</div>
    <div id="queues" class="card"></div>
    <div id="workers" class="card"></div>
    <script>
      const refreshSeconds = Number({escaped_refresh});

      function statusClass(status) {{
        if (status === "ok") return "status-ok";
        if (status === "degraded") return "status-degraded";
        return "status-critical";
      }}

      function renderSummary(data) {{
        const totals = data.totals || {{}};
        return `
          <div><strong>Namespace:</strong> ${{data.namespace}}</div>
          <div><strong>Overall:</strong> <span class="${{statusClass(data.overall_status)}}">${{data.overall_status}}</span></div>
          <div><strong>Workers:</strong> total=${{totals.workers_total || 0}}, online=${{totals.workers_online || 0}}, stale=${{totals.workers_stale || 0}}</div>
          <div><strong>Backlog:</strong> waiting_not_running=${{totals.waiting_not_running || 0}}, pending=${{totals.pending || 0}}, pending_stale=${{totals.pending_stale || 0}}</div>
        `;
      }}

      function renderQueues(data) {{
        const rows = (data.queues || []).map((queue) => `
          <tr>
            <td>${{queue.queue_name}}</td>
            <td>${{queue.serving_revision || "-"}}</td>
            <td class="${{statusClass(queue.status)}}">${{queue.status}}</td>
            <td>${{queue.workers_total}}</td>
            <td>${{queue.workers_accepting}}</td>
            <td>${{queue.workers_draining}}</td>
            <td>${{queue.stream_ready}}</td>
            <td>${{queue.delayed}}</td>
            <td>${{queue.pending}}</td>
            <td>${{queue.pending_stale}}</td>
            <td>${{queue.waiting_not_running}}</td>
          </tr>
        `).join("");
        return `
          <h3>Queues</h3>
          <table>
            <thead>
              <tr>
                <th>Queue</th><th>Serving Revision</th><th>Status</th>
                <th>Workers</th><th>Accepting</th><th>Draining</th>
                <th>Ready</th><th>Delayed</th><th>Pending</th><th>Pending Stale</th><th>Waiting(Not Running)</th>
              </tr>
            </thead>
            <tbody>${{rows}}</tbody>
          </table>
        `;
      }}

      function renderWorkers(data) {{
        const rows = (data.workers || []).map((worker) => `
          <tr>
            <td>${{worker.worker_id}}</td>
            <td>${{worker.worker_revision || "-"}}</td>
            <td>${{worker.hostname || "-"}}</td>
            <td>${{worker.pid || "-"}}</td>
            <td class="${{worker.status === "online" ? "status-ok" : "status-degraded"}}">${{worker.status}}</td>
            <td>${{worker.age_ms ?? "-"}}</td>
            <td>${{(worker.accepting_queues || []).join(",") || "-"}}</td>
            <td>${{(worker.queues || []).join(",") || "-"}}</td>
            <td>${{worker.runtime?.in_flight ?? "-"}}</td>
            <td>${{worker.runtime?.ready_backlog ?? "-"}}</td>
            <td>${{worker.runtime?.completed_total ?? "-"}}</td>
            <td>${{worker.runtime?.retried_total ?? "-"}}</td>
          </tr>
        `).join("");
        return `
          <h3>Workers</h3>
          <table>
            <thead>
              <tr>
                <th>Worker ID</th><th>Revision</th><th>Host</th><th>PID</th><th>Status</th><th>Age(ms)</th>
                <th>Accepting Queues</th><th>Queues</th><th>In Flight</th><th>Ready Backlog</th><th>Completed</th><th>Retried</th>
              </tr>
            </thead>
            <tbody>${{rows}}</tbody>
          </table>
        `;
      }}

      async function refresh() {{
        try {{
          const snapshotUrl = new URL("snapshot", window.location.href);
          const response = await fetch(snapshotUrl.toString());
          const data = await response.json();
          document.getElementById("summary").innerHTML = renderSummary(data);
          document.getElementById("queues").innerHTML = renderQueues(data);
          document.getElementById("workers").innerHTML = renderWorkers(data);
        }} catch (error) {{
          document.getElementById("summary").textContent = "Failed to load admin snapshot: " + String(error);
        }}
      }}

      refresh();
      setInterval(refresh, Math.max(refreshSeconds, 1) * 1000);
    </script>
  </body>
</html>
"""


def _response_headers(content_type: str, payload: bytes) -> Headers:
    return [
        (b"content-type", content_type.encode("utf-8")),
        (b"cache-control", b"no-store"),
        (b"content-length", str(len(payload)).encode("utf-8")),
    ]


class FluxeraAdminASGI:
    def __init__(
        self,
        *,
        redis_url: str,
        namespace: str,
        queue_filter: Optional[list[str]] = None,
        worker_stale_after_ms: Optional[int] = None,
        pending_idle_threshold_ms: Optional[int] = None,
        refresh_seconds: float = 3.0,
        snapshot_loader: Optional[SnapshotLoader] = None,
        mount_path: Optional[str] = None,
    ) -> None:
        self.redis_url = redis_url
        self.namespace = namespace
        self.queue_filter = queue_filter
        self.worker_stale_after_ms = worker_stale_after_ms
        self.pending_idle_threshold_ms = pending_idle_threshold_ms
        self.refresh_seconds = refresh_seconds
        self.snapshot_loader = snapshot_loader
        self.mount_path = _normalize_mount_path(mount_path)

    async def _runtime_payload(self, query_params: dict[str, list[str]]) -> dict[str, Any]:
        if self.snapshot_loader is not None:
            return await self.snapshot_loader(query_params)

        query_queues = [queue for queue in query_params.get("queue", []) if queue]
        status = await get_runtime_status(
            self.redis_url,
            namespace=self.namespace,
            queues=query_queues or self.queue_filter,
            worker_stale_after_ms=self.worker_stale_after_ms,
            pending_idle_threshold_ms=self.pending_idle_threshold_ms,
        )
        payload = runtime_status_to_dict(status)
        payload["healthy"] = status.overall_status == "ok"
        return payload

    async def _send(
        self,
        send,
        *,
        status_code: int,
        content_type: str,
        body: bytes,
    ) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": _response_headers(content_type, body),
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": body,
                "more_body": False,
            }
        )

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self._send(
                send,
                status_code=HTTPStatus.NOT_IMPLEMENTED,
                content_type="text/plain; charset=utf-8",
                body=b"Fluxera admin only supports HTTP scopes.",
            )
            return

        # Drain request body for compatibility with some ASGI servers.
        while True:
            event = await receive()
            if event["type"] != "http.request" or not event.get("more_body", False):
                break

        path = _normalize_request_path(
            scope.get("path", ""),
            root_path=scope.get("root_path", ""),
            mount_path=self.mount_path,
        )
        query_string = scope.get("query_string", b"")
        query_params = parse_qs(query_string.decode("utf-8", errors="ignore"))

        if path in {"", "/"}:
            body = _dashboard_html(self.refresh_seconds).encode("utf-8")
            await self._send(
                send,
                status_code=HTTPStatus.OK,
                content_type="text/html; charset=utf-8",
                body=body,
            )
            return

        if path == "/snapshot":
            payload = await self._runtime_payload(query_params)
            body = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
            await self._send(
                send,
                status_code=HTTPStatus.OK,
                content_type="application/json",
                body=body,
            )
            return

        if path in {"/health", "/healthz"}:
            payload = await self._runtime_payload(query_params)
            health_payload = {
                "namespace": payload.get("namespace"),
                "status": payload.get("overall_status", "unknown"),
                "healthy": bool(payload.get("healthy")),
                "generated_at_ms": payload.get("generated_at_ms"),
                "totals": payload.get("totals", {}),
            }
            body = orjson.dumps(health_payload, option=orjson.OPT_SORT_KEYS)
            await self._send(
                send,
                status_code=HTTPStatus.OK if health_payload["healthy"] else HTTPStatus.SERVICE_UNAVAILABLE,
                content_type="application/json",
                body=body,
            )
            return

        await self._send(
            send,
            status_code=HTTPStatus.NOT_FOUND,
            content_type="application/json",
            body=orjson.dumps({"error": "not_found"}),
        )


def create_admin_asgi_app(
    *,
    redis_url: str,
    namespace: str,
    queue_filter: Optional[list[str]] = None,
    worker_stale_after_ms: Optional[int] = None,
    pending_idle_threshold_ms: Optional[int] = None,
    refresh_seconds: float = 3.0,
    mount_path: Optional[str] = None,
) -> FluxeraAdminASGI:
    return FluxeraAdminASGI(
        redis_url=redis_url,
        namespace=namespace,
        queue_filter=queue_filter,
        worker_stale_after_ms=worker_stale_after_ms,
        pending_idle_threshold_ms=pending_idle_threshold_ms,
        refresh_seconds=refresh_seconds,
        mount_path=mount_path,
    )


def mount_admin_asgi(
    app: Any,
    *,
    mount_path: str = "/admin/fluxera",
    redis_url: str,
    namespace: str,
    queue_filter: Optional[list[str]] = None,
    worker_stale_after_ms: Optional[int] = None,
    pending_idle_threshold_ms: Optional[int] = None,
    refresh_seconds: float = 3.0,
) -> FluxeraAdminASGI:
    mount = getattr(app, "mount", None)
    if not callable(mount):
        raise TypeError("app must provide a mount(path, app) method.")

    admin_app = create_admin_asgi_app(
        redis_url=redis_url,
        namespace=namespace,
        queue_filter=queue_filter,
        worker_stale_after_ms=worker_stale_after_ms,
        pending_idle_threshold_ms=pending_idle_threshold_ms,
        refresh_seconds=refresh_seconds,
        mount_path=mount_path,
    )
    mount(mount_path, admin_app)
    return admin_app
