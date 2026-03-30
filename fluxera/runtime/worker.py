from __future__ import annotations

import asyncio
import inspect
import logging
import multiprocessing as mp
import os
import pickle
import random
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Optional
from uuid import uuid4

import redis.exceptions as redis_exceptions

from ..broker import Broker, Consumer, Delivery
from ..callbacks import DeadLetterContext, OutcomeContext
from ..dead_letters import DeadLetterRecord, FailureKind
from ..errors import ActorNotFound, RemoteExecutionError, WorkerError
from ..message import current_millis

if TYPE_CHECKING:
    from ..actor import Actor


DEFAULT_PROCESS_START_METHOD = "spawn"
DEFAULT_RETRY_JITTER = "full"

logger = logging.getLogger(__name__)


def _execute_call(fn, args, kwargs):
    return fn(*args, **kwargs)


def _process_worker_main(connection) -> None:
    try:
        while True:
            payload = connection.recv()
            if payload is None:
                return

            fn, args, kwargs = payload
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:  # pragma: no cover - exercised through parent integration tests
                traceback_text = traceback.format_exc()
                try:
                    pickle.dumps(exc)
                    connection.send(("error", exc, traceback_text))
                except BaseException:
                    connection.send(("remote_error", type(exc).__name__, repr(exc), traceback_text))
            else:
                connection.send(("ok", result))
    except EOFError:  # pragma: no cover - depends on shutdown timing
        return
    finally:
        connection.close()


@dataclass(slots=True)
class TaskRecord:
    """Runtime metadata for an in-flight message."""

    delivery: Delivery
    attempt: int = 0
    max_retries: int = 0
    execution_mode: str = "async"
    state: str = "delivered"
    final_action: Optional[str] = None
    cancel_reason: Optional[str] = None
    retry_delay_ms: Optional[int] = None
    timeout_ms: Optional[int] = None
    received_at: float = 0.0
    admitted_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    received_at_ms: int = 0
    admitted_at_ms: Optional[int] = None
    started_at_ms: Optional[int] = None
    finished_at_ms: Optional[int] = None
    failed_at_ms: Optional[int] = None
    lease_deadline: Optional[float] = None
    result: Any = None
    exception: Optional[BaseException] = None


@dataclass(slots=True)
class _ScheduledDelivery:
    consumer: Consumer
    delivery: Delivery
    actor: "Actor"


@dataclass(slots=True)
class RetryPolicy:
    max_retries: int = 0
    min_backoff_ms: int = 0
    max_backoff_ms: int = 60_000
    retry_for: Optional[tuple[type[BaseException], ...]] = None
    retry_when: Any = None
    throws: Optional[tuple[type[BaseException], ...]] = None
    retry_on_timeout: bool = True
    jitter: str = DEFAULT_RETRY_JITTER


class _ProcessSlot:
    def __init__(self, slot_id: int, ctx) -> None:
        self.slot_id = slot_id
        self.ctx = ctx
        self.process = None
        self.connection = None
        self._start()

    def _start(self) -> None:
        parent_conn, child_conn = self.ctx.Pipe()
        process = self.ctx.Process(
            target=_process_worker_main,
            args=(child_conn,),
            name=f"fluxera-process-{self.slot_id}",
        )
        process.start()
        child_conn.close()
        self.process = process
        self.connection = parent_conn

    def is_alive(self) -> bool:
        return self.process is not None and self.process.is_alive()

    def run_sync(self, fn, args, kwargs):
        if not self.is_alive() or self.connection is None:
            raise WorkerError("Process slot is not available.")

        try:
            self.connection.send((fn, args, kwargs))
            status, *payload = self.connection.recv()
        except (BrokenPipeError, EOFError, OSError) as exc:
            raise WorkerError("Process slot exited unexpectedly.") from exc

        if status == "ok":
            return payload[0]

        if status == "error":
            exc, traceback_text = payload
            try:
                setattr(exc, "remote_traceback", traceback_text)
            except BaseException:
                pass
            raise exc

        if status == "remote_error":
            exc_name, message, traceback_text = payload
            raise RemoteExecutionError(
                f"Process task raised {exc_name}: {message}",
                traceback_text=traceback_text,
            )

        raise WorkerError(f"Unknown process slot response {status!r}.")

    async def run(self, fn, args, kwargs):
        return await asyncio.to_thread(self.run_sync, fn, args, kwargs)

    async def restart(self) -> None:
        await self.close(force=True)
        self._start()

    async def close(self, *, force: bool) -> None:
        process = self.process
        connection = self.connection
        self.process = None
        self.connection = None

        if process is None:
            if connection is not None:
                connection.close()
            return

        try:
            if connection is not None and process.is_alive() and not force:
                try:
                    await asyncio.to_thread(connection.send, None)
                except (BrokenPipeError, EOFError, OSError):
                    force = True
        finally:
            if connection is not None:
                connection.close()

        if force and process.is_alive():
            process.terminate()

        await asyncio.to_thread(process.join, 1.0 if force else 0.2)
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join, 1.0)


class _ProcessLane:
    def __init__(self, concurrency: int, *, start_method: Optional[str]) -> None:
        self.concurrency = concurrency
        self.start_method = start_method or DEFAULT_PROCESS_START_METHOD
        self.ctx = self._get_context()
        self.available: asyncio.Queue[_ProcessSlot] = asyncio.Queue(maxsize=concurrency)
        self.slots: list[_ProcessSlot] = []
        self._closing = False

    def _get_context(self):
        return mp.get_context(self.start_method)

    async def start(self) -> None:
        if self.slots:
            return

        for slot_id in range(self.concurrency):
            slot = _ProcessSlot(slot_id, self.ctx)
            self.slots.append(slot)
            self.available.put_nowait(slot)

    async def run(self, fn, args, kwargs, *, restart_on_cancel: bool) -> Any:
        slot = await self.available.get()
        should_return_slot = True

        try:
            return await slot.run(fn, args, kwargs)
        except asyncio.CancelledError:
            await asyncio.shield(self._replace_slot(slot, restart=restart_on_cancel and not self._closing))
            should_return_slot = False
            raise
        except WorkerError:
            await asyncio.shield(self._replace_slot(slot, restart=not self._closing))
            should_return_slot = False
            raise
        finally:
            if should_return_slot and not self._closing:
                await asyncio.shield(self.available.put(slot))

    async def _replace_slot(self, slot: _ProcessSlot, *, restart: bool) -> None:
        if restart:
            await slot.restart()
            if not self._closing:
                await self.available.put(slot)
            return

        await slot.close(force=True)

    async def close(self) -> None:
        self._closing = True
        while not self.available.empty():
            try:
                self.available.get_nowait()
            except asyncio.QueueEmpty:
                break

        await asyncio.gather(*(slot.close(force=True) for slot in self.slots), return_exceptions=True)
        self.slots.clear()


class Worker:
    """Async-native worker runtime for Fluxera brokers."""

    EXECUTION_MODES = ("async", "thread", "process")

    def __init__(
        self,
        broker: Broker,
        *,
        queues: Optional[set[str]] = None,
        concurrency: int = 128,
        async_concurrency: Optional[int] = None,
        thread_concurrency: Optional[int] = None,
        process_concurrency: Optional[int] = None,
        max_in_flight: Optional[int] = None,
        prefetch: Optional[int] = None,
        ready_queue_size: Optional[int] = None,
        poll_timeout: float = 0.1,
        process_start_method: Optional[str] = None,
        worker_revision: Optional[str] = None,
        worker_id: Optional[str] = None,
        revision_poll_interval: float = 1.0,
    ) -> None:
        self.broker = broker
        self.queue_filter = queues
        self.async_concurrency = self._normalize_limit(async_concurrency if async_concurrency is not None else concurrency, "async_concurrency")
        self.thread_concurrency = self._normalize_limit(
            thread_concurrency if thread_concurrency is not None else self.async_concurrency,
            "thread_concurrency",
        )
        default_process = min(os.cpu_count() or 1, max(self.async_concurrency, 1))
        self.process_concurrency = self._normalize_limit(
            process_concurrency if process_concurrency is not None else default_process,
            "process_concurrency",
        )
        self.lane_limits = {
            "async": self.async_concurrency,
            "thread": self.thread_concurrency,
            "process": self.process_concurrency,
        }
        configured_limits = [limit for limit in self.lane_limits.values() if limit > 0]
        if not configured_limits:
            raise ValueError("At least one execution lane must have a positive concurrency.")

        if max_in_flight is None:
            self.max_in_flight = sum(configured_limits)
        else:
            self.max_in_flight = self._normalize_limit(max_in_flight, "max_in_flight")

        self.prefetch = prefetch or min(max(self.max_in_flight, 1), 256)
        self.ready_queue_size = ready_queue_size
        self.poll_timeout = poll_timeout
        self.process_start_method = (
            process_start_method
            or os.environ.get("FLUXERA_PROCESS_START_METHOD")
            or DEFAULT_PROCESS_START_METHOD
        )
        self.worker_revision = worker_revision or os.environ.get("FLUXERA_WORKER_REVISION") or "local"
        self.worker_id = worker_id or f"fluxera-worker-{uuid4().hex}"
        self.revision_poll_interval = max(float(revision_poll_interval), 0.05)

        self.ready_queues: dict[str, asyncio.Queue[_ScheduledDelivery]] = {
            execution: asyncio.Queue(maxsize=self._queue_size_for_lane(execution))
            for execution, limit in self.lane_limits.items()
            if limit > 0
        }
        self.admission_permits = asyncio.Semaphore(self.max_in_flight)
        self.lane_permits = {
            execution: asyncio.Semaphore(limit)
            for execution, limit in self.lane_limits.items()
            if limit > 0
        }
        self.actor_permits: dict[str, asyncio.Semaphore] = {}
        self.consumers: dict[str, Consumer] = {}
        self.records: dict[str, TaskRecord] = {}
        self.record_history: dict[str, list[TaskRecord]] = defaultdict(list)

        self.ingress_tasks: set[asyncio.Task[None]] = set()
        self.consumer_restart_tasks: dict[str, asyncio.Task[None]] = {}
        self.execution_tasks: set[asyncio.Task[None]] = set()
        self.scheduler_tasks: dict[str, asyncio.Task[None]] = {}
        self.revision_task: Optional[asyncio.Task[None]] = None

        self.thread_executor: Optional[ThreadPoolExecutor] = None
        self.process_lane: Optional[_ProcessLane] = None
        self.process_lane_lock = asyncio.Lock()
        self.managed_queues: set[str] = set()
        self.queue_states: dict[str, str] = {}

        self._running = False
        self._stopping = False

    async def __aenter__(self) -> "Worker":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    @property
    def in_flight(self) -> int:
        return sum(1 for task in self.execution_tasks if not task.done())

    def _normalize_limit(self, value: int, name: str) -> int:
        try:
            limit = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be an integer, got {value!r}.") from exc

        if limit < 0:
            raise ValueError(f"{name} must be greater than or equal to zero.")

        return limit

    def _queue_size_for_lane(self, execution: str) -> int:
        if self.ready_queue_size is not None:
            return max(int(self.ready_queue_size), 1)
        return max(self.lane_limits[execution] * 2, 1)

    def _total_ready_backlog(self) -> int:
        return sum(queue.qsize() for queue in self.ready_queues.values())

    def _total_ready_capacity(self) -> int:
        return sum(queue.maxsize - queue.qsize() for queue in self.ready_queues.values())

    def _accepts_queue(self, queue_name: str) -> bool:
        return self.queue_states.get(queue_name, "accepting") == "accepting"

    async def _refresh_revision_state(self, *, bootstrap: bool) -> None:
        if not self.managed_queues:
            return

        next_states: dict[str, str] = {}
        for queue_name in sorted(self.managed_queues):
            if bootstrap:
                serving_revision = await self.broker.ensure_serving_revision(queue_name, self.worker_revision)
            else:
                serving_revision = await self.broker.get_serving_revision(queue_name)
                if serving_revision is None:
                    serving_revision = await self.broker.ensure_serving_revision(queue_name, self.worker_revision)
            next_states[queue_name] = "accepting" if serving_revision == self.worker_revision else "draining"

        self.queue_states = next_states
        await self.broker.register_worker_revision(
            worker_id=self.worker_id,
            worker_revision=self.worker_revision,
            queue_states=next_states.copy(),
        )

    async def _run_revision_heartbeat(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self.revision_poll_interval)
            await self._refresh_revision_state(bootstrap=False)

    async def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._stopping = False

        if self.thread_concurrency > 0 and self.thread_executor is None:
            self.thread_executor = ThreadPoolExecutor(
                max_workers=self.thread_concurrency,
                thread_name_prefix="fluxera-thread",
            )

        queue_names = set(self.queue_filter or self.broker.get_declared_queues())
        self.managed_queues = queue_names
        await self._refresh_revision_state(bootstrap=True)
        self.revision_task = asyncio.create_task(
            self._run_revision_heartbeat(),
            name=f"fluxera-revision:{self.worker_id}",
        )

        for queue_name in sorted(queue_names):
            consumer = await self.broker.open_consumer(queue_name, prefetch=self.prefetch)
            self.consumers[queue_name] = consumer
            self._start_consumer_task(queue_name, consumer)

        for execution in sorted(self.ready_queues):
            task = asyncio.create_task(self._schedule_lane(execution), name=f"fluxera-scheduler:{execution}")
            self.scheduler_tasks[execution] = task

    async def join(self) -> None:
        while True:
            if self.ready_queues:
                await asyncio.gather(*(queue.join() for queue in self.ready_queues.values()))

            if self._total_ready_backlog() == 0 and self.in_flight == 0:
                return
            await asyncio.sleep(self.poll_timeout)

    def _start_consumer_task(self, queue_name: str, consumer: Consumer) -> None:
        task = asyncio.create_task(
            self._consume(queue_name, consumer),
            name=f"fluxera-consumer:{queue_name}",
        )
        self.ingress_tasks.add(task)
        task.add_done_callback(partial(self._on_consumer_task_done, queue_name))

    def _on_consumer_task_done(self, queue_name: str, task: asyncio.Task[None]) -> None:
        self.ingress_tasks.discard(task)

        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except BaseException:  # pragma: no cover - defensive logging path
            logger.exception("Failed to inspect consumer task completion for queue %r.", queue_name)
            if not self._stopping:
                self._schedule_consumer_restart(queue_name)
            return

        if exc is None:
            if not self._stopping:
                logger.warning(
                    "Consumer task for queue %r exited unexpectedly without an exception. Restarting.",
                    queue_name,
                )
                self._schedule_consumer_restart(queue_name)
            return

        logger.warning(
            "Consumer task for queue %r crashed; scheduling restart.",
            queue_name,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        if not self._stopping:
            self._schedule_consumer_restart(queue_name)

    def _schedule_consumer_restart(self, queue_name: str) -> None:
        existing = self.consumer_restart_tasks.get(queue_name)
        if existing is not None and not existing.done():
            return

        task = asyncio.create_task(
            self._restart_consumer(queue_name),
            name=f"fluxera-consumer-restart:{queue_name}",
        )
        self.consumer_restart_tasks[queue_name] = task
        task.add_done_callback(partial(self._on_consumer_restart_done, queue_name))

    def _on_consumer_restart_done(self, queue_name: str, task: asyncio.Task[None]) -> None:
        current = self.consumer_restart_tasks.get(queue_name)
        if current is task:
            self.consumer_restart_tasks.pop(queue_name, None)

        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except BaseException:  # pragma: no cover - defensive logging path
            logger.exception("Failed to inspect consumer restart task for queue %r.", queue_name)
            if not self._stopping:
                self._schedule_consumer_restart(queue_name)
            return

        if exc is not None:
            logger.warning(
                "Consumer restart task for queue %r failed; scheduling another retry.",
                queue_name,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            if not self._stopping:
                self._schedule_consumer_restart(queue_name)

    async def _restart_consumer(self, queue_name: str) -> None:
        delay = self.poll_timeout
        while not self._stopping and queue_name in self.managed_queues:
            old_consumer = self.consumers.get(queue_name)
            if old_consumer is not None:
                try:
                    await old_consumer.close()
                except BaseException:  # pragma: no cover - close failures are non-fatal
                    logger.warning(
                        "Failed to close previous consumer for queue %r during restart.",
                        queue_name,
                        exc_info=True,
                    )

            try:
                consumer = await self.broker.open_consumer(queue_name, prefetch=self.prefetch)
            except BaseException:
                logger.warning(
                    "Failed to reopen consumer for queue %r; retrying in %.3fs.",
                    queue_name,
                    delay,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
                delay = min(max(delay * 2.0, self.poll_timeout), 5.0)
                continue

            self.consumers[queue_name] = consumer
            self._start_consumer_task(queue_name, consumer)
            logger.info("Restarted consumer for queue %r.", queue_name)
            return

    async def _reopen_consumer(self, queue_name: str, consumer: Consumer) -> Consumer:
        try:
            await consumer.close()
        except BaseException:  # pragma: no cover - close failures are non-fatal
            logger.warning(
                "Failed to close consumer for queue %r before reopening.",
                queue_name,
                exc_info=True,
            )

        new_consumer = await self.broker.open_consumer(queue_name, prefetch=self.prefetch)
        self.consumers[queue_name] = new_consumer
        return new_consumer

    async def stop(self, *, timeout: float = 30.0) -> None:
        if not self._running:
            return

        self._stopping = True

        for task in list(self.consumer_restart_tasks.values()):
            task.cancel()
        if self.consumer_restart_tasks:
            await asyncio.gather(*self.consumer_restart_tasks.values(), return_exceptions=True)
            self.consumer_restart_tasks.clear()

        for task in list(self.ingress_tasks):
            task.cancel()
        if self.ingress_tasks:
            await asyncio.gather(*self.ingress_tasks, return_exceptions=True)

        try:
            await asyncio.wait_for(self.join(), timeout=timeout)
        except asyncio.TimeoutError:
            for task in list(self.execution_tasks):
                task.cancel()
            if self.execution_tasks:
                await asyncio.gather(*self.execution_tasks, return_exceptions=True)

        if self.revision_task is not None:
            self.revision_task.cancel()
            await asyncio.gather(self.revision_task, return_exceptions=True)
            self.revision_task = None

        for task in list(self.scheduler_tasks.values()):
            task.cancel()
        if self.scheduler_tasks:
            await asyncio.gather(*self.scheduler_tasks.values(), return_exceptions=True)
            self.scheduler_tasks.clear()

        for consumer in list(self.consumers.values()):
            await consumer.close()
        self.consumers.clear()

        await self.broker.unregister_worker_revision(worker_id=self.worker_id, queue_names=self.managed_queues.copy())
        self.managed_queues.clear()
        self.queue_states.clear()
        await self._shutdown_executors()
        await self.broker.close()
        self._running = False

    async def _shutdown_executors(self) -> None:
        if self.thread_executor is not None:
            executor = self.thread_executor
            self.thread_executor = None
            await asyncio.to_thread(executor.shutdown, True, cancel_futures=True)

        if self.process_lane is not None:
            lane = self.process_lane
            self.process_lane = None
            await lane.close()

    async def run_in_thread(self, fn, *args, **kwargs):
        if self.thread_executor is None:
            return await asyncio.to_thread(fn, *args, **kwargs)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_executor, _execute_call, fn, args, kwargs)

    async def run_in_process(self, fn, *args, **kwargs):
        if self.process_concurrency <= 0:
            raise RuntimeError("Process execution is disabled for this worker.")

        if self.process_lane is None:
            async with self.process_lane_lock:
                if self.process_lane is None:
                    self.process_lane = _ProcessLane(
                        self.process_concurrency,
                        start_method=self.process_start_method,
                    )
                    await self.process_lane.start()

        try:
            return await self.process_lane.run(
                fn,
                args,
                kwargs,
                restart_on_cancel=not self._stopping,
            )
        except (AttributeError, TypeError, pickle.PicklingError) as exc:
            raise RuntimeError(
                "Process execution requires top-level picklable callables and arguments."
            ) from exc

    async def _consume(self, queue_name: str, consumer: Consumer) -> None:
        while not self._stopping:
            if not self._accepts_queue(queue_name):
                await asyncio.sleep(self.poll_timeout)
                continue

            available_slots = self.max_in_flight - self.in_flight - self._total_ready_backlog()
            local_capacity = self._total_ready_capacity()
            batch_size = min(self.prefetch, max(available_slots, 0), max(local_capacity, 0))

            if batch_size <= 0:
                await asyncio.sleep(self.poll_timeout)
                continue

            try:
                deliveries = await consumer.receive(limit=batch_size, timeout=self.poll_timeout)
            except (
                redis_exceptions.TimeoutError,
                redis_exceptions.ConnectionError,
                ConnectionError,
                OSError,
            ) as exc:
                if self._stopping:
                    break

                logger.warning(
                    "Transient consumer error on queue %r; retrying.",
                    queue_name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                try:
                    consumer = await self._reopen_consumer(queue_name, consumer)
                except BaseException:
                    logger.warning(
                        "Failed to reopen consumer for queue %r after transient error.",
                        queue_name,
                        exc_info=True,
                    )
                await asyncio.sleep(self.poll_timeout)
                continue

            for delivery in deliveries:
                try:
                    actor = self.broker.get_actor(delivery.actor_name)
                except ActorNotFound:
                    self._attach_terminal_dead_letter(
                        delivery,
                        actor_name=delivery.actor_name,
                        failure_kind="invalid_configuration",
                        exception_message=f"Actor {delivery.actor_name!r} is not registered.",
                    )
                    await consumer.reject(delivery, requeue=False)
                    continue

                ready_queue = self.ready_queues.get(actor.execution)
                if ready_queue is None:
                    self._attach_terminal_dead_letter(
                        delivery,
                        actor_name=actor.actor_name,
                        failure_kind="invalid_configuration",
                        execution_mode=actor.execution,
                        exception_message=f"Execution lane {actor.execution!r} is not enabled for this worker.",
                    )
                    await consumer.reject(delivery, requeue=False)
                    continue

                if not self._accepts_queue(delivery.queue_name):
                    await consumer.reject(delivery, requeue=True)
                    continue

                await ready_queue.put(_ScheduledDelivery(consumer=consumer, delivery=delivery, actor=actor))

    async def _schedule_lane(self, execution: str) -> None:
        ready_queue = self.ready_queues[execution]
        while not self._stopping or not ready_queue.empty() or self.in_flight:
            try:
                item = await asyncio.wait_for(ready_queue.get(), timeout=self.poll_timeout)
            except asyncio.TimeoutError:
                continue

            if not self._accepts_queue(item.delivery.queue_name):
                await item.consumer.reject(item.delivery, requeue=True)
                ready_queue.task_done()
                continue

            admission_acquired = False
            actor_permit: Optional[asyncio.Semaphore] = None
            lane_permit: Optional[asyncio.Semaphore] = None

            try:
                await self.admission_permits.acquire()
                admission_acquired = True
                actor_permit = await self._acquire_actor_permit(item.actor)
                lane_permit = await self._acquire_lane_permit(execution)

                if not self._accepts_queue(item.delivery.queue_name):
                    if lane_permit is not None:
                        lane_permit.release()
                        lane_permit = None
                    if actor_permit is not None:
                        actor_permit.release()
                        actor_permit = None
                    if admission_acquired:
                        self.admission_permits.release()
                        admission_acquired = False
                    await item.consumer.reject(item.delivery, requeue=True)
                    ready_queue.task_done()
                    continue

                record = self._build_task_record(item.actor, item.delivery)
                record.admitted_at = time.perf_counter()
                record.admitted_at_ms = current_millis()
                record.state = "admitted"
                self.records[item.delivery.message_id] = record
                self.record_history[item.delivery.message_id].append(record)

                task = asyncio.create_task(
                    self._execute(item, record, actor_permit, lane_permit),
                    name=f"fluxera-exec:{item.actor.actor_name}:{item.delivery.message_id}",
                )
                self.execution_tasks.add(task)
                task.add_done_callback(self.execution_tasks.discard)
            except BaseException:
                if lane_permit is not None:
                    lane_permit.release()
                if actor_permit is not None:
                    actor_permit.release()
                if admission_acquired:
                    self.admission_permits.release()
                ready_queue.task_done()
                raise

    async def _execute(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
        actor_permit: Optional[asyncio.Semaphore],
        lane_permit: Optional[asyncio.Semaphore],
    ) -> None:
        lease_task = self._start_lease_heartbeat(item, record)
        try:
            record.state = "running"
            record.started_at = time.perf_counter()
            record.started_at_ms = current_millis()

            if record.timeout_ms is None:
                record.result = await item.actor.run(*item.delivery.args, worker=self, **item.delivery.kwargs)
            else:
                async with asyncio.timeout(record.timeout_ms / 1000):
                    record.result = await item.actor.run(*item.delivery.args, worker=self, **item.delivery.kwargs)

            record.state = "succeeded"
            record.final_action = "ack"
            await item.consumer.ack(item.delivery)
            await self._emit_outcome_callbacks(
                item,
                record,
                event="success",
                option_name="on_success",
                result=record.result,
            )
        except TimeoutError as exc:
            record.state = "timed_out"
            record.exception = exc
            record.failed_at_ms = current_millis()
            await self._handle_failure(item, item.actor, record, exc, failure_kind="timeout")
        except asyncio.CancelledError as exc:
            record.state = "cancelled"
            record.exception = exc
            record.cancel_reason = "worker_shutdown" if self._stopping else "external_cancel"
            record.failed_at_ms = current_millis()
            await self._handle_cancellation(item, item.actor, record)
            raise
        except BaseException as exc:
            record.state = "failed"
            record.exception = exc
            record.failed_at_ms = current_millis()
            await self._handle_failure(item, item.actor, record, exc, failure_kind="exception")
        finally:
            if lease_task is not None:
                lease_task.cancel()
                await asyncio.gather(lease_task, return_exceptions=True)
            record.finished_at = time.perf_counter()
            record.finished_at_ms = current_millis()
            if lane_permit is not None:
                lane_permit.release()
            if actor_permit is not None:
                actor_permit.release()
            self.admission_permits.release()
            self.ready_queues[item.actor.execution].task_done()

    def _start_lease_heartbeat(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
    ) -> Optional[asyncio.Task[None]]:
        lease_seconds = item.delivery.metadata.get("lease_seconds")
        if lease_seconds is None:
            return None

        try:
            lease_seconds = float(lease_seconds)
        except (TypeError, ValueError):
            return None

        if lease_seconds <= 0:
            return None

        interval = self._lease_heartbeat_interval(lease_seconds)
        return asyncio.create_task(
            self._renew_lease(item, record, lease_seconds=lease_seconds, interval=interval),
            name=f"fluxera-lease:{item.actor.actor_name}:{item.delivery.message_id}",
        )

    def _lease_heartbeat_interval(self, lease_seconds: float) -> float:
        interval = lease_seconds * 0.3
        if lease_seconds > 1.0:
            interval = min(interval, 5.0)
        return max(interval, 0.05)

    async def _renew_lease(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
        *,
        lease_seconds: float,
        interval: float,
    ) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await item.consumer.extend_lease(item.delivery, seconds=lease_seconds)
            except asyncio.CancelledError:
                raise
            except BaseException:
                return
            else:
                record.lease_deadline = item.delivery.lease_deadline

    async def _acquire_lane_permit(self, execution: str) -> Optional[asyncio.Semaphore]:
        permit = self.lane_permits.get(execution)
        if permit is None:
            return None

        await permit.acquire()
        return permit

    async def _acquire_actor_permit(self, actor) -> Optional[asyncio.Semaphore]:
        limit = actor.options.get("concurrency")
        if limit is None:
            return None

        try:
            concurrency = int(limit)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Actor concurrency must be an integer, got {limit!r}.") from exc

        if concurrency <= 0:
            raise ValueError(f"Actor concurrency must be positive, got {concurrency!r}.")

        permit = self.actor_permits.get(actor.actor_name)
        if permit is None:
            permit = self.actor_permits[actor.actor_name] = asyncio.Semaphore(concurrency)

        await permit.acquire()
        return permit

    def _build_task_record(self, actor, delivery: Delivery) -> TaskRecord:
        retry_policy = self._resolve_retry_policy(actor, delivery)
        timeout_ms = self._resolve_timeout_ms(actor, delivery)

        return TaskRecord(
            delivery=delivery,
            attempt=self._resolve_attempt(delivery),
            max_retries=retry_policy.max_retries,
            execution_mode=actor.execution,
            timeout_ms=timeout_ms,
            received_at=time.perf_counter(),
            received_at_ms=current_millis(),
            lease_deadline=delivery.lease_deadline,
        )

    def _resolve_attempt(self, delivery: Delivery) -> int:
        raw_attempt = delivery.options.get("attempt", 0)
        try:
            return int(raw_attempt)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Message attempt must be an integer, got {raw_attempt!r}.") from exc

    def _resolve_timeout_ms(self, actor, delivery: Delivery) -> Optional[int]:
        raw_timeout = delivery.options.get("timeout", actor.options.get("timeout"))
        if raw_timeout is None:
            return None

        try:
            timeout_ms = int(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Actor timeout must be an integer number of milliseconds, got {raw_timeout!r}.") from exc

        if timeout_ms <= 0:
            raise ValueError(f"Actor timeout must be positive, got {timeout_ms!r}.")

        return timeout_ms

    def _resolve_retry_policy(self, actor, delivery: Delivery) -> RetryPolicy:
        raw_max_retries = delivery.options.get("max_retries", actor.options.get("max_retries", 0))
        raw_min_backoff = delivery.options.get("min_backoff", actor.options.get("min_backoff", 0))
        raw_max_backoff = delivery.options.get("max_backoff", actor.options.get("max_backoff", 60_000))
        retry_for = delivery.options.get("retry_for", actor.options.get("retry_for"))
        retry_when = delivery.options.get("retry_when", actor.options.get("retry_when"))
        throws = delivery.options.get("throws", actor.options.get("throws"))
        retry_on_timeout = delivery.options.get("retry_on_timeout", actor.options.get("retry_on_timeout", True))
        raw_jitter = delivery.options.get("jitter", actor.options.get("jitter", DEFAULT_RETRY_JITTER))

        try:
            max_retries = int(raw_max_retries)
            min_backoff_ms = int(raw_min_backoff)
            max_backoff_ms = int(raw_max_backoff)
        except (TypeError, ValueError) as exc:
            raise ValueError("Retry policy values must be integers.") from exc

        if max_retries < 0:
            raise ValueError("max_retries must be greater than or equal to zero.")
        if min_backoff_ms < 0 or max_backoff_ms < 0:
            raise ValueError("Retry backoff values must be greater than or equal to zero.")
        if max_backoff_ms < min_backoff_ms:
            raise ValueError("max_backoff must be greater than or equal to min_backoff.")

        retry_for = self._coerce_exception_types(retry_for, option_name="retry_for")
        throws = self._coerce_exception_types(throws, option_name="throws")

        if retry_when is not None and not callable(retry_when):
            raise ValueError("retry_when must be callable.")

        jitter = self._coerce_retry_jitter(raw_jitter)

        return RetryPolicy(
            max_retries=max_retries,
            min_backoff_ms=min_backoff_ms,
            max_backoff_ms=max_backoff_ms,
            retry_for=retry_for,
            retry_when=retry_when,
            throws=throws,
            retry_on_timeout=bool(retry_on_timeout),
            jitter=jitter,
        )

    def _coerce_exception_types(self, raw_value: Any, *, option_name: str) -> Optional[tuple[type[BaseException], ...]]:
        if raw_value is None:
            return None

        if isinstance(raw_value, type):
            raw_value = (raw_value,)
        elif not isinstance(raw_value, tuple):
            raise ValueError(f"{option_name} must be an exception type or a tuple of exception types.")

        for exc_type in raw_value:
            if not isinstance(exc_type, type) or not issubclass(exc_type, BaseException):
                raise ValueError(f"{option_name} entries must be exception types.")

        return raw_value

    def _coerce_retry_jitter(self, raw_value: Any) -> str:
        if raw_value in {None, True}:
            return "full"
        if raw_value is False:
            return "none"
        if isinstance(raw_value, str):
            normalized = raw_value.strip().lower()
            if normalized in {"none", "full"}:
                return normalized
        raise ValueError("jitter must be one of: True, False, 'full', 'none'.")

    def _callback_targets(self, actor, delivery: Delivery, option_name: str) -> list[Any]:
        target = delivery.options.get(option_name, actor.options.get(option_name))
        if target is None:
            return []
        if isinstance(target, (list, tuple, set)):
            return [item for item in target if item is not None]
        return [target]

    def _build_outcome_context(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
        *,
        event: str,
        result: Any = None,
        failure_kind: Optional[FailureKind] = None,
        exc: Optional[BaseException] = None,
        dead_letter_id: Optional[str] = None,
    ) -> OutcomeContext:
        traceback_text = None
        if exc is not None and exc.__traceback__ is not None:
            traceback_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        return OutcomeContext(
            event=event,
            message=item.delivery.message,
            queue_name=item.delivery.queue_name,
            actor_name=item.actor.actor_name,
            attempt=record.attempt,
            max_retries=record.max_retries,
            execution_mode=record.execution_mode,
            worker_id=self.worker_id,
            worker_revision=self.worker_revision,
            result=result,
            failure_kind=failure_kind,
            exception_type=None if exc is None else type(exc).__name__,
            exception_message=None if exc is None else str(exc),
            traceback_text=traceback_text,
            retry_delay_ms=record.retry_delay_ms,
            dead_letter_id=dead_letter_id,
        )

    async def _dispatch_actor_callback(self, target: Any, payload: dict[str, Any]) -> None:
        if isinstance(target, str):
            target_actor = self.broker.get_actor(target)
            await target_actor.send(payload)
            return

        if hasattr(target, "send") and callable(target.send):
            await target.send(payload)
            return

        raise TypeError(f"Callback actor target must be an actor name or Actor instance, got {type(target)!r}.")

    async def _dispatch_inprocess_callback(self, target: Any, context: Any) -> None:
        result = target(context)
        if inspect.isawaitable(result):
            await result

    async def _invoke_callbacks(self, targets: list[Any], *, context: Any, actor_payload: Optional[dict[str, Any]]) -> None:
        for target in targets:
            try:
                if callable(target) and not isinstance(target, str):
                    await self._dispatch_inprocess_callback(target, context)
                else:
                    if actor_payload is None:
                        raise TypeError("Actor callback payload is not available for this callback.")
                    await self._dispatch_actor_callback(target, actor_payload)
            except asyncio.CancelledError:
                raise
            except BaseException:
                logger.exception("Fluxera callback %r failed.", target)

    async def _emit_outcome_callbacks(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
        *,
        event: str,
        option_name: str,
        result: Any = None,
        failure_kind: Optional[FailureKind] = None,
        exc: Optional[BaseException] = None,
        dead_letter_id: Optional[str] = None,
    ) -> None:
        targets = self._callback_targets(item.actor, item.delivery, option_name)
        if not targets:
            return
        context = self._build_outcome_context(
            item,
            record,
            event=event,
            result=result,
            failure_kind=failure_kind,
            exc=exc,
            dead_letter_id=dead_letter_id,
        )
        await self._invoke_callbacks(targets, context=context, actor_payload=context.to_dict())

    async def _emit_dead_letter_callbacks(self, item: _ScheduledDelivery, record: DeadLetterRecord) -> None:
        targets = self._callback_targets(item.actor, item.delivery, "on_dead_lettered")
        if not targets:
            return
        context = DeadLetterContext(record=record)
        await self._invoke_callbacks(targets, context=context, actor_payload=context.to_dict())

    async def _emit_dead_letter_related_callbacks(
        self,
        item: _ScheduledDelivery,
        record: TaskRecord,
        dead_letter_record: Optional[DeadLetterRecord],
    ) -> None:
        if dead_letter_record is None:
            return
        await self._emit_dead_letter_callbacks(item, dead_letter_record)

    async def _handle_failure(
        self,
        item,
        actor,
        record: TaskRecord,
        exc: BaseException,
        *,
        failure_kind: FailureKind,
    ) -> None:
        try:
            retry_policy = self._resolve_retry_policy(actor, item.delivery)
            should_retry = self._should_retry(record, retry_policy, exc, failure_kind=failure_kind)
        except Exception as policy_exc:
            record.final_action = "reject"
            self._attach_dead_letter_record(
                item,
                actor,
                record,
                exc=policy_exc,
                failure_kind="invalid_configuration",
            )
            await item.consumer.reject(item.delivery, requeue=False)
            dead_letter_record = item.delivery.metadata.get("dead_letter_record")
            await self._emit_outcome_callbacks(
                item,
                record,
                event="failure",
                option_name="on_failure",
                failure_kind="invalid_configuration",
                exc=policy_exc,
                dead_letter_id=None if dead_letter_record is None else dead_letter_record.dead_letter_id,
            )
            await self._emit_dead_letter_related_callbacks(item, record, dead_letter_record)
            return

        if should_retry:
            retry_delay_ms = self._compute_retry_delay_ms(record, retry_policy)
            await self.broker.send(
                item.delivery.message.copy(
                    options={
                        "attempt": record.attempt + 1,
                        "retries": record.attempt + 1,
                        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                        "requeue_timestamp": current_millis(),
                    }
                ),
                delay=retry_delay_ms / 1000 if retry_delay_ms > 0 else None,
            )
            record.state = "retry_scheduled"
            record.retry_delay_ms = retry_delay_ms
            record.final_action = "retry"
            await item.consumer.ack(item.delivery)
            await self._emit_outcome_callbacks(
                item,
                record,
                event="failure",
                option_name="on_failure",
                failure_kind=failure_kind,
                exc=exc,
            )
            await self._emit_outcome_callbacks(
                item,
                record,
                event="retry_scheduled",
                option_name="on_retry_scheduled",
                failure_kind=failure_kind,
                exc=exc,
            )
            return

        record.final_action = "reject"
        self._attach_dead_letter_record(item, actor, record, exc=exc, failure_kind=failure_kind)
        await item.consumer.reject(item.delivery, requeue=False)
        dead_letter_record = item.delivery.metadata.get("dead_letter_record")
        dead_letter_id = None if dead_letter_record is None else dead_letter_record.dead_letter_id
        await self._emit_outcome_callbacks(
            item,
            record,
            event="failure",
            option_name="on_failure",
            failure_kind=failure_kind,
            exc=exc,
            dead_letter_id=dead_letter_id,
        )
        await self._emit_outcome_callbacks(
            item,
            record,
            event="retry_exhausted",
            option_name="on_retry_exhausted",
            failure_kind=failure_kind,
            exc=exc,
            dead_letter_id=dead_letter_id,
        )
        await self._emit_dead_letter_related_callbacks(item, record, dead_letter_record)

    async def _handle_cancellation(self, item, actor, record: TaskRecord) -> None:
        on_cancel = item.delivery.options.get("on_cancel", actor.options.get("on_cancel", "requeue"))
        if on_cancel == "ack":
            record.final_action = "ack"
            await item.consumer.ack(item.delivery)
            return

        if on_cancel == "reject":
            record.final_action = "reject"
            self._attach_dead_letter_record(
                item,
                actor,
                record,
                exc=record.exception,
                failure_kind="cancel_reject",
            )
            await item.consumer.reject(item.delivery, requeue=False)
            dead_letter_record = item.delivery.metadata.get("dead_letter_record")
            dead_letter_id = None if dead_letter_record is None else dead_letter_record.dead_letter_id
            await self._emit_outcome_callbacks(
                item,
                record,
                event="failure",
                option_name="on_failure",
                failure_kind="cancel_reject",
                exc=record.exception,
                dead_letter_id=dead_letter_id,
            )
            await self._emit_dead_letter_related_callbacks(item, record, dead_letter_record)
            return

        if on_cancel == "requeue":
            record.final_action = "requeue"
            await item.consumer.reject(item.delivery, requeue=True)
            return

        raise ValueError(f"Unknown cancellation policy {on_cancel!r}.")

    def _should_retry(
        self,
        record: TaskRecord,
        retry_policy: RetryPolicy,
        exc: BaseException,
        *,
        failure_kind: FailureKind,
    ) -> bool:
        if retry_policy.throws is not None and isinstance(exc, retry_policy.throws):
            return False

        if retry_policy.retry_when is not None:
            should_retry = retry_policy.retry_when(record.attempt, exc, record)
            if not isinstance(should_retry, bool):
                raise ValueError("retry_when must return a boolean value.")
            return should_retry

        if record.attempt >= retry_policy.max_retries:
            return False

        if failure_kind == "timeout":
            return retry_policy.retry_on_timeout

        if retry_policy.retry_for is None:
            return True

        return isinstance(exc, retry_policy.retry_for)

    def _compute_retry_delay_ms(self, record: TaskRecord, retry_policy: RetryPolicy) -> int:
        if retry_policy.min_backoff_ms == 0:
            return 0

        base_delay_ms = retry_policy.min_backoff_ms * (2**record.attempt)
        if retry_policy.jitter == "none":
            return min(base_delay_ms, retry_policy.max_backoff_ms)

        delay_ms = int(base_delay_ms * random.uniform(1.0, 2.0))
        if delay_ms > retry_policy.max_backoff_ms:
            return int(retry_policy.max_backoff_ms * random.uniform(0.5, 1.0))
        return delay_ms

    def _attach_terminal_dead_letter(
        self,
        delivery: Delivery,
        *,
        actor_name: str,
        failure_kind: FailureKind,
        execution_mode: str = "async",
        exception_message: Optional[str] = None,
    ) -> None:
        namespace = getattr(self.broker, "namespace", self.broker.__class__.__name__.lower())
        now_ms = current_millis()
        retention_deadline_ms = self._dead_letter_retention_deadline(now_ms)
        record = DeadLetterRecord.from_message(
            delivery.message,
            namespace=namespace,
            queue_name=delivery.queue_name,
            actor_name=actor_name,
            delivery_id=delivery.transport_id,
            failure_kind=failure_kind,
            execution_mode=execution_mode,
            exception_message=exception_message,
            dead_lettered_at_ms=now_ms,
            worker_id=self.worker_id,
            worker_revision=self.worker_revision,
            consumer_name=delivery.metadata.get("consumer_name"),
            retention_deadline_ms=retention_deadline_ms,
        )
        delivery.metadata["dead_letter_record"] = record
        delivery.metadata["execution_mode"] = execution_mode

    def _attach_dead_letter_record(
        self,
        item: _ScheduledDelivery,
        actor,
        record: TaskRecord,
        *,
        exc: Optional[BaseException],
        failure_kind: FailureKind,
    ) -> None:
        now_ms = current_millis()
        retention_deadline_ms = self._dead_letter_retention_deadline(now_ms)
        exception_type = None if exc is None else type(exc).__name__
        exception_message = None if exc is None else str(exc)
        traceback_text = None
        if exc is not None and exc.__traceback__ is not None:
            traceback_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        deduplication_id = None
        raw_deduplication = item.delivery.options.get("deduplication")
        if isinstance(raw_deduplication, dict):
            raw_id = raw_deduplication.get("id")
            if raw_id not in {None, ""}:
                deduplication_id = str(raw_id)
        if deduplication_id is None:
            raw_job_id = item.delivery.options.get("job_id")
            if raw_job_id not in {None, ""}:
                deduplication_id = str(raw_job_id)

        namespace = getattr(self.broker, "namespace", self.broker.__class__.__name__.lower())
        dead_letter_record = DeadLetterRecord.from_message(
            item.delivery.message,
            namespace=namespace,
            queue_name=item.delivery.queue_name,
            actor_name=actor.actor_name,
            delivery_id=item.delivery.transport_id,
            failure_kind=failure_kind,
            attempt=record.attempt,
            max_retries=record.max_retries,
            timeout_ms=record.timeout_ms,
            execution_mode=record.execution_mode,
            exception_type=exception_type,
            exception_message=exception_message,
            traceback_text=traceback_text,
            cancel_reason=record.cancel_reason,
            retry_delay_ms=record.retry_delay_ms,
            dead_lettered_at_ms=now_ms,
            received_at_ms=record.received_at_ms,
            started_at_ms=record.started_at_ms,
            failed_at_ms=record.failed_at_ms or now_ms,
            worker_id=self.worker_id,
            worker_revision=self.worker_revision,
            consumer_name=item.delivery.metadata.get("consumer_name"),
            deduplication_id=deduplication_id,
            idempotency_key=item.delivery.options.get("idempotency_key"),
            retention_deadline_ms=retention_deadline_ms,
        )
        item.delivery.metadata["dead_letter_record"] = dead_letter_record
        item.delivery.metadata["execution_mode"] = record.execution_mode

    def _dead_letter_retention_deadline(self, now_ms: int) -> Optional[int]:
        ttl_ms = getattr(self.broker, "dead_letter_ttl_ms", None)
        if ttl_ms is None:
            return None
        return now_ms + int(ttl_ms)
