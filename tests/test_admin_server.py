from __future__ import annotations

import unittest

from fluxera.admin_server import _RuntimeSnapshotCache


class RuntimeSnapshotCacheTests(unittest.TestCase):
    def test_reuses_payload_for_matching_queue_filter(self) -> None:
        cache = _RuntimeSnapshotCache(25.0)
        calls = 0

        def load() -> dict:
            nonlocal calls
            calls += 1
            return {"generated_at_ms": calls}

        first = cache.get_or_load(("alpha",), load)
        second = cache.get_or_load(("alpha",), load)
        other = cache.get_or_load(("beta",), load)

        self.assertEqual(first["snapshot_cache"]["state"], "refresh")
        self.assertEqual(second["snapshot_cache"]["state"], "hit")
        self.assertEqual(second["generated_at_ms"], 1)
        self.assertEqual(other["snapshot_cache"]["state"], "refresh")
        self.assertEqual(calls, 2)

    def test_zero_ttl_disables_cache_hits(self) -> None:
        cache = _RuntimeSnapshotCache(0.0)
        calls = 0

        def load() -> dict:
            nonlocal calls
            calls += 1
            return {"generated_at_ms": calls}

        first = cache.get_or_load(None, load)
        second = cache.get_or_load(None, load)

        self.assertEqual(first["snapshot_cache"]["state"], "refresh")
        self.assertEqual(second["snapshot_cache"]["state"], "refresh")
        self.assertEqual(calls, 2)
