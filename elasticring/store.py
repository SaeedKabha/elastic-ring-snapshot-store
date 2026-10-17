"""A record store sharded over an elastic ring, with snapshots."""

from __future__ import annotations

from typing import Dict, List, Optional

from elasticring.ring import ElasticRing


class SnapshotStore:
    """Fixed-width float records sharded across ring nodes.

    ``snapshot()`` freezes the current records under an integer id and
    ``restore(id)`` rolls the live records back to that frozen state,
    re-placing each record through the ring as it stands at restore time
    so snapshots keep working as nodes join and leave. Records added
    after the snapshot are gone after the restore.
    """

    def __init__(
        self, nodes: List[str], dim: int = 64, vnodes: int = 64
    ) -> None:
        if dim < 1:
            raise ValueError("dim must be >= 1")
        self._ring = ElasticRing(nodes, vnodes)
        self._dim = dim
        self._shards: Dict[str, Dict[str, List[float]]] = {
            name: {} for name in self._ring.nodes
        }
        self._snapshots: Dict[int, Dict[str, Dict[str, List[float]]]] = {}
        self._next_snapshot_id = 1

    @property
    def dim(self) -> int:
        return self._dim

    @property
    def nodes(self) -> List[str]:
        return self._ring.nodes

    def put(self, key: str, values: List[float]) -> None:
        if len(values) != self._dim:
            raise ValueError(
                "expected {} values, got {}".format(self._dim, len(values))
            )
        node = self._ring.owner_of(key)
        self._shards[node][key] = values

    def get(self, key: str) -> Optional[List[float]]:
        node = self._ring.owner_of(key)
        if node is None:
            return None
        return self._shards[node].get(key)

    def delete(self, key: str) -> bool:
        node = self._ring.owner_of(key)
        if node is None or key not in self._shards[node]:
            return False
        del self._shards[node][key]
        return True

    def put_batch(self, entries: Dict[str, List[float]]) -> int:
        count = 0
        for key, values in entries.items():
            self.put(key, values)
            count += 1
        return count

    def get_batch(self, keys: List[str]) -> Dict[str, Optional[List[float]]]:
        return {key: self.get(key) for key in keys}

    def total_size(self) -> int:
        return sum(len(shard) for shard in self._shards.values())

    def node_sizes(self) -> Dict[str, int]:
        return {node: len(self._shards[node]) for node in self._ring.nodes}

    def join_node(self, name: str) -> None:
        self._ring.join_node(name)
        if name not in self._shards:
            self._shards[name] = {}

    def leave_node(self, name: str) -> None:
        self._ring.leave_node(name)
        orphaned = self._shards.pop(name)
        for key, values in orphaned.items():
            self._shards[self._ring.owner_of(key)][key] = values

    def snapshot(self) -> int:
        snap_id = self._next_snapshot_id
        self._next_snapshot_id += 1
        self._snapshots[snap_id] = {
            node: dict(shard) for node, shard in self._shards.items()
        }
        return snap_id

    def restore(self, snap_id: int) -> None:
        try:
            saved = self._snapshots[snap_id]
        except KeyError:
            raise KeyError("unknown snapshot: {!r}".format(snap_id))
        fresh: Dict[str, Dict[str, List[float]]] = {
            name: {} for name in self._ring.nodes
        }
        for shard in saved.values():
            for key, values in shard.items():
                fresh[self._ring.owner_of(key)][key] = values
        self._shards = fresh
