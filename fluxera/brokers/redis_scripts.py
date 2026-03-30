from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


_LUA_DIR = Path(__file__).with_name("redis_lua")


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _decode_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return _decode_text(value)


def _decode_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(_decode_text(value))


@dataclass(slots=True)
class EnqueueDecision:
    status: str
    message_id: str
    replaced_message_id: Optional[str] = None

    @classmethod
    def from_response(cls, response: Any) -> "EnqueueDecision":
        status = _decode_text(response[0])
        message_id = _decode_text(response[1])
        replaced_message_id = None
        if len(response) > 2 and response[2] is not None:
            replaced_message_id = _decode_text(response[2])
        return cls(status=status, message_id=message_id, replaced_message_id=replaced_message_id)


@dataclass(slots=True)
class IdempotencyBeginResult:
    status: str
    fence_token: Optional[int] = None
    lease_deadline_ms: Optional[int] = None
    owner: Optional[str] = None
    completed_at_ms: Optional[int] = None
    result_ref: Optional[str] = None
    result_digest: Optional[str] = None

    @classmethod
    def from_response(cls, response: Any) -> "IdempotencyBeginResult":
        status = _decode_text(response[0])
        if status in {"acquired", "stolen"}:
            owner = None
            if len(response) > 3:
                owner = _decode_optional_text(response[3])
            return cls(
                status=status,
                fence_token=_decode_int(response[1]),
                lease_deadline_ms=_decode_int(response[2]),
                owner=owner,
            )
        if status == "completed":
            return cls(
                status=status,
                fence_token=_decode_int(response[1]),
                completed_at_ms=_decode_int(response[2]),
                result_ref=_decode_optional_text(response[3]),
                result_digest=_decode_optional_text(response[4]),
            )
        if status == "busy":
            return cls(
                status=status,
                fence_token=_decode_int(response[1]),
                lease_deadline_ms=_decode_int(response[2]),
                owner=_decode_optional_text(response[3]),
            )
        return cls(status=status)


@dataclass(slots=True)
class IdempotencyHeartbeatResult:
    status: str
    lease_deadline_ms: Optional[int] = None
    owner: Optional[str] = None
    fence_token: Optional[int] = None
    record_status: Optional[str] = None

    @classmethod
    def from_response(cls, response: Any) -> "IdempotencyHeartbeatResult":
        status = _decode_text(response[0])
        if status == "ok":
            return cls(status=status, lease_deadline_ms=_decode_int(response[1]))
        if status == "owner_mismatch":
            return cls(
                status=status,
                owner=_decode_optional_text(response[1]),
                fence_token=_decode_int(response[2]),
            )
        if status == "not_running":
            return cls(status=status, record_status=_decode_optional_text(response[1]))
        return cls(status=status)


@dataclass(slots=True)
class IdempotencyCommitResult:
    status: str
    completed_at_ms: Optional[int] = None
    owner: Optional[str] = None
    fence_token: Optional[int] = None
    record_status: Optional[str] = None

    @classmethod
    def from_response(cls, response: Any) -> "IdempotencyCommitResult":
        status = _decode_text(response[0])
        if status == "ok":
            return cls(status=status, completed_at_ms=_decode_int(response[1]))
        if status == "owner_mismatch":
            return cls(
                status=status,
                owner=_decode_optional_text(response[1]),
                fence_token=_decode_int(response[2]),
            )
        if status == "not_running":
            return cls(status=status, record_status=_decode_optional_text(response[1]))
        return cls(status=status)


@dataclass(slots=True)
class IdempotencyReleaseResult:
    status: str
    owner: Optional[str] = None
    fence_token: Optional[int] = None
    record_status: Optional[str] = None

    @classmethod
    def from_response(cls, response: Any) -> "IdempotencyReleaseResult":
        status = _decode_text(response[0])
        if status == "owner_mismatch":
            return cls(
                status=status,
                owner=_decode_optional_text(response[1]),
                fence_token=_decode_int(response[2]),
            )
        if status == "not_running":
            return cls(status=status, record_status=_decode_optional_text(response[1]))
        return cls(status=status)


class RedisLuaScripts:
    def __init__(self, client) -> None:
        self.client = client
        self._scripts = {
            "enqueue_or_deduplicate": self.client.register_script(self._load("enqueue_or_deduplicate.lua")),
            "promote_due": self.client.register_script(self._load("promote_due.lua")),
            "remove_dedupe_key_if_owner": self.client.register_script(self._load("remove_dedupe_key_if_owner.lua")),
            "idem_begin": self.client.register_script(self._load("idem_begin.lua")),
            "idem_heartbeat": self.client.register_script(self._load("idem_heartbeat.lua")),
            "idem_commit": self.client.register_script(self._load("idem_commit.lua")),
            "idem_release": self.client.register_script(self._load("idem_release.lua")),
        }

    def _load(self, filename: str) -> str:
        return (_LUA_DIR / filename).read_text(encoding="utf-8")

    async def enqueue_or_deduplicate(
        self,
        *,
        message_key_prefix: str,
        stream_key: str,
        delayed_key: str,
        dedupe_key: str,
        message_id: str,
        encoded_message: bytes,
        now_ms: int,
        deliver_at_ms: int,
        mode: str,
        ttl_ms: int,
        extend: bool,
        replace: bool,
        message_ttl_ms: int,
    ) -> EnqueueDecision:
        response = await self._scripts["enqueue_or_deduplicate"](
            keys=[message_key_prefix, stream_key, delayed_key, dedupe_key],
            args=[
                message_id,
                encoded_message,
                now_ms,
                deliver_at_ms,
                mode,
                ttl_ms,
                1 if extend else 0,
                1 if replace else 0,
                message_ttl_ms,
            ],
        )
        return EnqueueDecision.from_response(response)

    def enqueue_or_deduplicate_sync(
        self,
        *,
        message_key_prefix: str,
        stream_key: str,
        delayed_key: str,
        dedupe_key: str,
        message_id: str,
        encoded_message: bytes,
        now_ms: int,
        deliver_at_ms: int,
        mode: str,
        ttl_ms: int,
        extend: bool,
        replace: bool,
        message_ttl_ms: int,
    ) -> EnqueueDecision:
        response = self._scripts["enqueue_or_deduplicate"](
            keys=[message_key_prefix, stream_key, delayed_key, dedupe_key],
            args=[
                message_id,
                encoded_message,
                now_ms,
                deliver_at_ms,
                mode,
                ttl_ms,
                1 if extend else 0,
                1 if replace else 0,
                message_ttl_ms,
            ],
        )
        return EnqueueDecision.from_response(response)

    async def promote_due(
        self,
        *,
        delayed_key: str,
        stream_key: str,
        now_ms: int,
        limit: int,
    ) -> int:
        response = await self._scripts["promote_due"](
            keys=[delayed_key, stream_key],
            args=[now_ms, limit],
        )
        return _decode_int(response)

    async def remove_dedupe_key_if_owner(self, *, dedupe_key: str, message_id: str) -> bool:
        response = await self._scripts["remove_dedupe_key_if_owner"](
            keys=[dedupe_key],
            args=[message_id],
        )
        return bool(_decode_int(response))

    async def idem_begin(
        self,
        *,
        idempotency_key: str,
        owner: str,
        message_id: str,
        attempt: int,
        now_ms: int,
        lease_ms: int,
    ) -> IdempotencyBeginResult:
        response = await self._scripts["idem_begin"](
            keys=[idempotency_key],
            args=[owner, message_id, attempt, now_ms, lease_ms],
        )
        return IdempotencyBeginResult.from_response(response)

    async def idem_heartbeat(
        self,
        *,
        idempotency_key: str,
        owner: str,
        fence_token: int,
        now_ms: int,
        lease_ms: int,
    ) -> IdempotencyHeartbeatResult:
        response = await self._scripts["idem_heartbeat"](
            keys=[idempotency_key],
            args=[owner, fence_token, now_ms, lease_ms],
        )
        return IdempotencyHeartbeatResult.from_response(response)

    async def idem_commit(
        self,
        *,
        idempotency_key: str,
        owner: str,
        fence_token: int,
        now_ms: int,
        ttl_ms: int,
        result_ref: Optional[str],
        result_digest: Optional[str],
    ) -> IdempotencyCommitResult:
        response = await self._scripts["idem_commit"](
            keys=[idempotency_key],
            args=[
                owner,
                fence_token,
                now_ms,
                ttl_ms,
                result_ref or "",
                result_digest or "",
            ],
        )
        return IdempotencyCommitResult.from_response(response)

    async def idem_release(
        self,
        *,
        idempotency_key: str,
        owner: str,
        fence_token: int,
    ) -> IdempotencyReleaseResult:
        response = await self._scripts["idem_release"](
            keys=[idempotency_key],
            args=[owner, fence_token],
        )
        return IdempotencyReleaseResult.from_response(response)
