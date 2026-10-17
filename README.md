# elastic-ring-snapshot-store

A small Python library for sharding fixed-width float records over an
elastic consistent-hash ring, with point-in-time snapshots.

- `ElasticRing` maps keys to nodes over a 32-bit token ring. Each node
  owns a number of virtual points; a key belongs to the first point at
  or after the key's own point, wrapping around the end of the ring.
  Nodes can join or leave at any time.
- `SnapshotStore` shards records across the ring's nodes. `snapshot()`
  freezes the current records under an integer id; `restore(id)` rolls
  the live records back, re-placing each record through the ring as it
  stands at restore time so snapshots keep working as nodes join and
  leave.

```python
from elasticring import ElasticRing, SnapshotStore

ring = ElasticRing(["edge-pop-0", "edge-pop-1", "edge-pop-2"])
ring.owner_of("sensor:000001")

store = SnapshotStore(["edge-pop-0", "edge-pop-1"], dim=4)
store.put("sensor:000001", [0.1, 0.2, 0.3, 0.4])
snap = store.snapshot()
store.restore(snap)
```

Run the tests with `python -m pytest tests/ -v`.
