from __future__ import annotations

import asyncio
import html
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

import orjson

from .admin import get_runtime_status, runtime_status_to_dict

logger = logging.getLogger(__name__)


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
          const response = await fetch("/admin/snapshot");
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


class _AdminDashboardHandler(BaseHTTPRequestHandler):
    redis_url: str = ""
    namespace: str = "fluxera"
    queue_filter: list[str] | None = None
    worker_stale_after_ms: Optional[int] = None
    pending_idle_threshold_ms: Optional[int] = None
    refresh_seconds: float = 3.0

    def log_message(self, format: str, *args) -> None:  # noqa: A003 - stdlib method name
        logger.info("fluxera-admin %s - %s", self.address_string(), format % args)

    def _json_response(self, payload: dict, *, status: int = HTTPStatus.OK) -> None:
        encoded = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _text_response(self, payload: str, *, content_type: str = "text/plain; charset=utf-8") -> None:
        encoded = payload.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _resolve_queues(self, query_params: dict[str, list[str]]) -> Optional[list[str]]:
        query_queues = query_params.get("queue", [])
        if query_queues:
            return [queue for queue in query_queues if queue]
        return self.queue_filter

    def _load_runtime_payload(self, *, queues: Optional[list[str]]) -> dict:
        status = asyncio.run(
            get_runtime_status(
                self.redis_url,
                namespace=self.namespace,
                queues=queues,
                worker_stale_after_ms=self.worker_stale_after_ms,
                pending_idle_threshold_ms=self.pending_idle_threshold_ms,
            )
        )
        payload = runtime_status_to_dict(status)
        payload["healthy"] = status.overall_status == "ok"
        return payload

    def do_GET(self) -> None:  # noqa: N802 - stdlib method name
        parsed = urlparse(self.path)
        query_params = parse_qs(parsed.query)
        queues = self._resolve_queues(query_params)

        if parsed.path in {"/health", "/healthz"}:
            payload = self._load_runtime_payload(queues=queues)
            self._json_response(
                {
                    "status": payload["overall_status"],
                    "healthy": payload["healthy"],
                    "namespace": payload["namespace"],
                    "totals": payload["totals"],
                    "generated_at_ms": payload["generated_at_ms"],
                },
                status=HTTPStatus.OK if payload["healthy"] else HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return

        if parsed.path == "/admin/snapshot":
            payload = self._load_runtime_payload(queues=queues)
            self._json_response(payload)
            return

        if parsed.path in {"/admin", "/admin/"}:
            self._text_response(
                _dashboard_html(self.refresh_seconds),
                content_type="text/html; charset=utf-8",
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")


class AdminDashboardServer:
    def __init__(
        self,
        *,
        redis_url: str,
        namespace: str,
        host: str,
        port: int,
        queue_filter: Optional[list[str]] = None,
        worker_stale_after_ms: Optional[int] = None,
        pending_idle_threshold_ms: Optional[int] = None,
        refresh_seconds: float = 3.0,
    ) -> None:
        handler = type(
            "FluxeraAdminHandler",
            (_AdminDashboardHandler,),
            {
                "redis_url": redis_url,
                "namespace": namespace,
                "queue_filter": queue_filter,
                "worker_stale_after_ms": worker_stale_after_ms,
                "pending_idle_threshold_ms": pending_idle_threshold_ms,
                "refresh_seconds": refresh_seconds,
            },
        )
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread: Optional[threading.Thread] = None

    @property
    def host(self) -> str:
        return str(self._server.server_address[0])

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="fluxera-admin-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
