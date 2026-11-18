# Exact QuickMRC lookup observations, V1 staging

This adds a **default-off native observation**, not a controller, reporter,
or cache policy. The existing QuickMRC histogram, sampling, ghost accounting
and reset are unchanged. Apply the patches in order to a fresh checkout of the
FRocksDB gitlink at `d439c0fbf80e87340c728ab3fad1664ac9f72098`:

```sh
native=sources/libs/frocksdb
git -C "$native" apply "$PWD/sources/libs/patches/frocksdb-a4s-block-latency.patch"
git -C "$native" apply "$PWD/sources/libs/patches/frocksdb-a4s-cache-lookup-v1.patch"
images/frocksdb/build.sh
```

As with the latency patch, use the matching Java and native library from that
one build. Set `FLINK_A4S_COUNT_CACHE_LOOKUPS=1` before starting the process;
otherwise counting is off. The cache must also be created with QuickMRC
enabled and **one actual native LRU shard**. For a controlled experiment use
`state.backend.rocksdb.memory.lru-cache-num-shard-bits: 0` where the A4S runtime
passes it through, and still check the reported native shard count; Flink
parallelism 1 does not in itself mean one native shard or one cache owner. No
default shard, backend, job, or QuickMRC options change.

## Boundary and units

`org.rocksdb.LRUCache.getA4SCacheLookupSnapshot()` returns one atomic observation
of the **physical cache**, not a column family, Flink task, state, or RocksDB
Statistics object. Java serializes the call against Java handle closure.
Disabled, closed, and unsupported caches return null rather than a partial or
inconsistent observation. V1 does not try to merge independently timed
shard snapshots. The 7-element array is:

| Index | Field | Unit or interpretation |
|---:|---|---|
| 0 | schema version | integer `1` |
| 1 | cache ID | positive identity for the native lifetime, scoped to the JNI process |
| 2 | capacity | configured native cache charge budget, bytes |
| 3 | capacity generation | increases on each actual capacity change, including a resize round trip |
| 4 | shard count | actual native shards, exactly `1` in V1 |
| 5 | total lookups | count of generic uncompressed or other lookups targeting this physical LRU instance |
| 6 | total resident misses | number of those lookups that found no entry in the resident table |

The exact increment is at the same LRU `Lookup` hook used by QuickMRC, after
the resident-table lookup and before any ghost probe. A ghost hit is a
resident miss. A complete miss is also a resident miss. The count includes
index and filter as well as data if those actually use this cache; it does
not count non-lookup MRC operations, logical events inferred from weighted
histogram mass, RocksDB GETs, native device reads, or processed records.
Each sampled histogram reset leaves both exact counters untouched. A
snapshot takes the same existing single shard lock as lookup or resize, so
capacity, capacity generation, lookups and misses all refer to the same native
boundary. Deduplicate by (TaskManager process generation, native cache ID), even
if two task attempts report it. Never reuse an ID across a process restart
merely because the numeric ID matches.

## What still blocks a controller estimate

A caller has to bracket the native observation with its monotonic poll times
and align the two boundaries with **the same subtask attempt's** processed
record counter. Subtract only same-ID, same-schema, same-capacity, same-epoch
observations; reject restart, resize, missing, stale, nonmonotonic and
insufficient-count windows, or a miss delta outside `[0, lookup delta]`.
Do not run a second sampler on another wrapper around the same physical
cache. A task-level reporter must prove, not assume, the P=1 trial has one
exclusive cache owner and no unrelated traffic (a shared RocksDB cache can
include lookups for other operators/backends). Even with clean alignment,
infer a workload marginal cost using held-out controlled fixed-parallelism
cache-capacity interventions, not these counters alone; current A4S constants
still apply and existing placement and rescaling behavior remains unchanged.

## Focused verification

Run the FRocksDB `lru_cache_test` cases filtered by
`LRUCacheTest.A4SLookup*` to check exact hit/ghost/cold classification,
histogram reset independence, disabled default, capacity epoch, and
poll-time coherence under concurrent lookups. Against
the matching patched Java/JNI package, compile the standalone
`scripts/autoscaling/smoke/A4SCacheLookupSmoke.java` and run it once with
`FLINK_A4S_COUNT_CACHE_LOOKUPS=0` and once with `=1`. It checks default gate,
identity, actual native geometry and close handling. Neither test qualifies a
per-record estimate or runtime use.
