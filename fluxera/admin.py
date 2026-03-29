from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

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
