"""Elastic consistent-hash ring with virtual nodes."""

from __future__ import annotations

from bisect import bisect
from typing import Dict, List, Optional

_RING_MOD = 1 << 32


def _token(text: str) -> int:
    """Map a string to a point on the ring.

    Fast inline string hash: multiply-add over the characters of the text.
    """
    h = 5381
    for ch in text:
        h = ((h * 33) + ord(ch)) & 0xFFFFFFFF
    return h


class ElasticRing:
    """Routes keys to nodes over a token ring.

    Each node owns ``vnodes`` virtual points on the ring. A key belongs to
    the first node point at or after the key's own point, wrapping around
    the end of the ring. Nodes may join or leave at any time; only keys
    next to the membership change move.
    """

    def __init__(self, nodes: List[str], vnodes: int = 64) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self._default_vnodes = vnodes
        self._vnodes: Dict[str, int] = {}
        self._tokens: List[int] = []
        self._owners: Dict[int, str] = {}
        for name in nodes:
            self._vnodes[name] = vnodes
        self._rebuild()

    @property
    def nodes(self) -> List[str]:
        return list(self._vnodes)

    def owner_of(self, key: str) -> Optional[str]:
        """Return the node owning ``key``, or None when the ring is empty."""
        if not self._tokens:
            return None
        pos = bisect(self._tokens, _token(key)) % len(self._tokens)
        return self._owners[self._tokens[pos]]

    def placement(self, keys: List[str]) -> Dict[str, int]:
        """Return how many of ``keys`` each node owns."""
        counts: Dict[str, int] = {name: 0 for name in self._vnodes}
        for key in keys:
            owner = self.owner_of(key)
            if owner is not None:
                counts[owner] += 1
        return counts

    def peak_load_ratio(self, keys: List[str]) -> float:
        """Return the busiest node's count over the perfectly even share.

        A perfectly even placement returns 1.0.
        """
        if not keys:
            return 1.0
        counts = self.placement(keys)
        ideal = len(keys) / len(counts)
        return max(counts.values()) / ideal

    def join_node(self, name: str, vnodes: Optional[int] = None) -> None:
        """Add ``name`` to the ring, or update its virtual-node count."""
        count = self._default_vnodes if vnodes is None else vnodes
        if count < 1:
            raise ValueError("vnodes must be >= 1")
        self._vnodes[name] = count
        self._rebuild()

    def leave_node(self, name: str) -> None:
        """Remove ``name`` from the ring."""
        if name not in self._vnodes:
            raise KeyError("unknown node: {!r}".format(name))
        del self._vnodes[name]
        self._rebuild()

    def _rebuild(self) -> None:
        tokens: List[int] = []
        owners: Dict[int, str] = {}
        for name, count in self._vnodes.items():
            for i in range(count):
                point = _token("{}#{}".format(name, i))
                tokens.append(point)
                owners[point] = name
        tokens.sort()
        self._tokens = tokens
        self._owners = owners
