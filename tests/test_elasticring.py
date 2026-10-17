"""Baseline behavior of the elastic ring and snapshot store."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

import elasticring
from elasticring import ElasticRing, SnapshotStore

NODES = ["edge-pop-0", "edge-pop-1", "edge-pop-2", "edge-pop-3"]


class TestElasticRing:
    def test_init_rejects_zero_vnodes(self):
        with pytest.raises(ValueError):
            ElasticRing(NODES, vnodes=0)

    def test_owner_of_empty_ring_is_none(self):
        assert ElasticRing([]).owner_of("sensor:000001") is None

    def test_owner_is_a_member(self):
        ring = ElasticRing(NODES)
        for key in ["sensor:000001", "device:7", "session:42", "a", "zzz"]:
            assert ring.owner_of(key) in NODES

    def test_owner_deterministic_in_process(self):
        ring = ElasticRing(NODES)
        assert ring.owner_of("sensor:000001") == ring.owner_of("sensor:000001")

    def test_placement_counts_cover_all_keys(self):
        ring = ElasticRing(NODES)
        keys = ["sensor:{:06d}".format(i) for i in range(100)]
        counts = ring.placement(keys)
        assert set(counts) == set(NODES)
        assert sum(counts.values()) == 100

    def test_peak_load_ratio_empty_is_one(self):
        assert ElasticRing(NODES).peak_load_ratio([]) == 1.0

    def test_leave_unknown_raises(self):
        ring = ElasticRing(NODES)
        with pytest.raises(KeyError):
            ring.leave_node("edge-pop-9")

    def test_join_then_leave_roundtrip(self):
        ring = ElasticRing(NODES)
        ring.join_node("edge-pop-4")
        assert "edge-pop-4" in ring.nodes
        assert ring.owner_of("sensor:000001") in ring.nodes
        ring.leave_node("edge-pop-4")
        assert "edge-pop-4" not in ring.nodes

    def test_routing_stable_across_processes(self):
        keys = ["sensor:000001", "device:000002", "session:000003"]
        ring = ElasticRing(["edge-pop-0", "edge-pop-1"])
        expected = [ring.owner_of(key) for key in keys]
        pkgdir = os.path.dirname(
            os.path.dirname(os.path.abspath(elasticring.__file__))
        )
        code = (
            "import json; from elasticring import ElasticRing; "
            "ring = ElasticRing(['edge-pop-0', 'edge-pop-1']); "
            "print(json.dumps([ring.owner_of(k) for k in {!r}]))"
        ).format(keys)
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=pkgdir,
            timeout=120,
        )
        assert proc.returncode == 0, proc.stderr
        assert json.loads(proc.stdout) == expected


class TestSnapshotStore:
    def test_put_and_get_roundtrip(self):
        store = SnapshotStore(NODES, dim=3)
        store.put("sensor:1", [0.1, 0.2, 0.3])
        assert store.get("sensor:1") == [0.1, 0.2, 0.3]

    def test_get_missing_is_none(self):
        assert SnapshotStore(NODES, dim=2).get("nope") is None

    def test_wrong_dim_raises(self):
        store = SnapshotStore(NODES, dim=3)
        with pytest.raises(ValueError):
            store.put("k", [1.0, 2.0])

    def test_delete_roundtrip(self):
        store = SnapshotStore(NODES, dim=2)
        store.put("k", [1.0, 2.0])
        assert store.delete("k") is True
        assert store.get("k") is None

    def test_delete_missing_is_false(self):
        assert SnapshotStore(NODES, dim=2).delete("k") is False

    def test_total_size_and_node_sizes_sum(self):
        store = SnapshotStore(NODES, dim=2)
        store.put("a", [1.0, 2.0])
        store.put("b", [3.0, 4.0])
        assert store.total_size() == 2
        assert sum(store.node_sizes().values()) == 2

    def test_put_batch_count_and_get_batch(self):
        store = SnapshotStore(NODES, dim=2)
        count = store.put_batch({"x": [1.0, 2.0], "y": [3.0, 4.0]})
        assert count == 2
        batch = store.get_batch(["x", "y", "z"])
        assert batch["x"] == [1.0, 2.0]
        assert batch["y"] == [3.0, 4.0]
        assert batch["z"] is None

    def test_restore_unknown_snapshot_raises(self):
        store = SnapshotStore(NODES, dim=2)
        with pytest.raises(KeyError):
            store.restore(999)

    def test_restore_drops_later_puts(self):
        store = SnapshotStore(NODES, dim=2)
        store.put("a", [1.0, 2.0])
        snap = store.snapshot()
        store.put("b", [3.0, 4.0])
        store.restore(snap)
        assert store.get("a") == [1.0, 2.0]
        assert store.get("b") is None

    def test_restore_after_join_keeps_records(self):
        store = SnapshotStore(NODES, dim=2)
        store.put("sensor:1", [1.0, 2.0])
        store.put("device:2", [3.0, 4.0])
        snap = store.snapshot()
        store.join_node("edge-pop-4")
        store.restore(snap)
        assert store.get("sensor:1") == [1.0, 2.0]
        assert store.get("device:2") == [3.0, 4.0]

    def test_leave_node_keeps_records(self):
        store = SnapshotStore(["solo"], dim=2)
        store.put("a", [1.0, 2.0])
        store.join_node("edge-pop-1")
        store.leave_node("solo")
        assert store.get("a") == [1.0, 2.0]
        assert store.total_size() == 1
