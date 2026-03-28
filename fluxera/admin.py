from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .brokers.redis import RedisBroker


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
