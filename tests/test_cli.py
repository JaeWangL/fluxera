from __future__ import annotations

import io
import importlib
import sys
import tempfile
import unittest
import asyncio
from contextlib import contextmanager
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from uuid import uuid4

import orjson
import redis

import fluxera
from fluxera.cli import main


class FluxeraCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.redis_url = "redis://127.0.0.1:6379/15"
        self.client = redis.Redis.from_url(self.redis_url)
        self.test_keys: list[str] = []
        self.test_namespaces: list[str] = []

        try:
            self.client.ping()
        except Exception as exc:
            self.client.close()
            self.skipTest(f"Redis is not available for CLI tests: {exc}")

    def tearDown(self) -> None:
        if self.test_keys:
            self.client.delete(*self.test_keys)
        for namespace in self.test_namespaces:
            keys = list(self.client.scan_iter(match=f"{namespace}:*"))
            if keys:
                self.client.delete(*keys)
        self.client.close()

    def unique_key(self) -> str:
        key = f"fluxera:cli:test:{uuid4().hex}"
        self.test_keys.append(key)
        return key

    def unique_namespace(self) -> str:
        namespace = f"fluxera-cli-test-{uuid4().hex}"
        self.test_namespaces.append(namespace)
        return namespace

    @contextmanager
    def temporary_import_path(self, path: Path):
        inserted = False
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
            inserted = True
        try:
            yield
        finally:
            if inserted:
                sys.path.remove(path_str)

    def test_rate_limit_probe_reports_acquired_and_busy(self) -> None:
        key = self.unique_key()
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "rate-limit",
                    "probe",
                    "--redis-url",
                    self.redis_url,
                    "--key",
                    key,
                    "--prefix",
                    "",
                    "--format",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertTrue(payload["acquired"])
        self.assertEqual(payload["requested_key"], key)
        self.assertEqual(payload["key"], key)

        limiter = fluxera.ConcurrentRateLimiter(self.client, key, prefix="", limit=1, ttl_ms=1000)
        stdout = io.StringIO()
        with limiter.acquire():
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "rate-limit",
                        "probe",
                        "--redis-url",
                        self.redis_url,
                        "--key",
                        key,
                        "--prefix",
                        "",
                        "--format",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 3)
        payload = orjson.loads(stdout.getvalue())
        self.assertFalse(payload["acquired"])

    def test_rate_limit_run_executes_command_under_lock(self) -> None:
        key = self.unique_key()
        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "marker.txt"
            exit_code = main(
                [
                    "rate-limit",
                    "run",
                    "--redis-url",
                    self.redis_url,
                    "--key",
                    key,
                    "--prefix",
                    "",
                    "--",
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(marker_path)!r}).write_text('ok', encoding='utf-8')",
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(marker_path.read_text(encoding="utf-8"), "ok")

    def test_rate_limit_run_returns_busy_exit_code_without_running_command(self) -> None:
        key = self.unique_key()
        limiter = fluxera.ConcurrentRateLimiter(self.client, key, prefix="", limit=1, ttl_ms=1000)

        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "marker.txt"
            stderr = io.StringIO()
            with limiter.acquire():
                with redirect_stderr(stderr):
                    exit_code = main(
                        [
                            "rate-limit",
                            "run",
                            "--redis-url",
                            self.redis_url,
                            "--key",
                            key,
                            "--prefix",
                            "",
                            "--",
                            sys.executable,
                            "-c",
                            f"from pathlib import Path; Path({str(marker_path)!r}).write_text('should-not-run', encoding='utf-8')",
                        ]
                    )

            self.assertEqual(exit_code, 3)
            self.assertFalse(marker_path.exists())
            self.assertIn("busy:", stderr.getvalue())

    def test_worker_command_runs_stub_broker_app_from_module_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir) / "cli_worker_app"
            package_dir.mkdir()
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            marker_path = package_dir / "marker.txt"

            (package_dir / "app_setup.py").write_text(
                "\n".join(
                    [
                        "import fluxera",
                        "",
                        "broker = fluxera.StubBroker()",
                        "fluxera.set_broker(broker)",
                    ]
                ),
                encoding="utf-8",
            )
            (package_dir / "workers.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "",
                        "import fluxera",
                        "from cli_worker_app.app_setup import broker",
                        "",
                        "@fluxera.actor(broker=broker, queue_name='default')",
                        "async def write_marker(path: str) -> None:",
                        "    Path(path).write_text('ok', encoding='utf-8')",
                    ]
                ),
                encoding="utf-8",
            )
            (package_dir / "worker_registry.py").write_text(
                "WORKER_MODULES = ['cli_worker_app.workers']\n",
                encoding="utf-8",
            )

            with self.temporary_import_path(Path(temp_dir)):
                app_setup = importlib.import_module("cli_worker_app.app_setup")
                workers = importlib.import_module("cli_worker_app.workers")
                workers.write_marker.send_sync(str(marker_path))

                exit_code = main(
                    [
                        "worker",
                        "cli_worker_app.app_setup",
                        "--module-registry",
                        "cli_worker_app.worker_registry:WORKER_MODULES",
                        "--broker",
                        "cli_worker_app.app_setup:broker",
                        "--process-concurrency",
                        "0",
                        "--exit-when-idle",
                    ]
                )

                self.assertEqual(exit_code, 0)
                self.assertEqual(marker_path.read_text(encoding="utf-8"), "ok")

                # Avoid leaking the temporary package into later tests.
                for module_name in [
                    "cli_worker_app.workers",
                    "cli_worker_app.worker_registry",
                    "cli_worker_app.app_setup",
                    "cli_worker_app",
                ]:
                    sys.modules.pop(module_name, None)
                import fluxera.broker as broker_module

                broker_module.global_broker = None

    def test_dlq_commands_list_get_requeue_and_purge(self) -> None:
        namespace = self.unique_namespace()

        async def create_dead_letter(value: str) -> str:
            broker = fluxera.RedisBroker(self.redis_url, namespace=namespace)

            @fluxera.actor(broker=broker, actor_name="cli_dlq", queue_name="default")
            async def noop(arg: str) -> str:
                return arg

            await noop.send(value)
            consumer = await broker.open_consumer(noop.queue_name)
            delivery = (await consumer.receive(limit=1, timeout=1.0))[0]
            await consumer.reject(delivery, requeue=False)
            records = await broker.get_dead_letter_records(noop.queue_name)
            await consumer.close()
            await broker.close()
            return records[0].dead_letter_id

        first_dead_letter_id = asyncio.run(create_dead_letter("first"))

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "list",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["dead_letter_id"], first_dead_letter_id)

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "get",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--dead-letter-id",
                    first_dead_letter_id,
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertTrue(payload["exists"])
        self.assertEqual(payload["record"]["failure_kind"], "operator_reject")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "requeue",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--dead-letter-id",
                    first_dead_letter_id,
                    "--note",
                    "retry from cli",
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertTrue(payload["updated"])
        self.assertEqual(payload["record"]["resolution_state"], "requeued")

        async def drain_requeued_message() -> None:
            broker = fluxera.RedisBroker(self.redis_url, namespace=namespace)
            consumer = await broker.open_consumer("default")
            delivery = (await consumer.receive(limit=1, timeout=1.0))[0]
            await consumer.ack(delivery)
            await broker.join("default")
            await consumer.close()
            await broker.close()

        asyncio.run(drain_requeued_message())

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "list",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertEqual(payload["count"], 0)

        second_dead_letter_id = asyncio.run(create_dead_letter("second"))
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "purge",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--dead-letter-id",
                    second_dead_letter_id,
                    "--note",
                    "drop from cli",
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertTrue(payload["updated"])
        self.assertEqual(payload["record"]["resolution_state"], "purged")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "dlq",
                    "get",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--queue",
                    "default",
                    "--dead-letter-id",
                    second_dead_letter_id,
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertTrue(payload["exists"])
        self.assertEqual(payload["record"]["resolution_state"], "purged")

    def test_monitor_snapshot_reports_worker_and_queue_state(self) -> None:
        namespace = self.unique_namespace()

        async def seed_runtime_state() -> None:
            broker = fluxera.RedisBroker(self.redis_url, namespace=namespace)

            @fluxera.actor(broker=broker, actor_name="cli_monitor", queue_name="default")
            async def noop(value: str) -> None:
                del value

            await broker.ensure_serving_revision("default", "rev-monitor")
            await broker.register_worker_revision(
                worker_id="worker-monitor",
                worker_revision="rev-monitor",
                queue_states={"default": "accepting"},
                runtime_state={
                    "worker_status": "running",
                    "in_flight": 2,
                    "ready_backlog": 3,
                    "completed_total": 10,
                },
            )
            await noop.send("queued")
            await broker.close()

        asyncio.run(seed_runtime_state())

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(
                [
                    "monitor",
                    "snapshot",
                    "--redis-url",
                    self.redis_url,
                    "--namespace",
                    namespace,
                    "--format",
                    "json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = orjson.loads(stdout.getvalue())
        self.assertEqual(payload["namespace"], namespace)
        self.assertEqual(payload["totals"]["workers_total"], 1)
        self.assertEqual(payload["totals"]["queues_total"], 1)
        self.assertEqual(payload["workers"][0]["worker_id"], "worker-monitor")
        self.assertEqual(payload["workers"][0]["runtime"]["in_flight"], 2)
        self.assertEqual(payload["queues"][0]["queue_name"], "default")
