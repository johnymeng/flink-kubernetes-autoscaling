# Measuring A4S's cache-miss penalty

A4S still uses the existing configured `job.autoscaler.a4s.hit-latency-sec` and
`job.autoscaler.a4s.miss-latency-sec` (default 5 µs and 5 ms). This opt-in
diagnostic measures local block access costs online, but **does not replace
either controller input**. The current inverse MRC equates seconds/record with
seconds/access. QuickMRC sees generic LRU lookups, including metadata, while this
sampler covers point-read data-block lookups. Treating these as interchangeable
would assume one equivalent lookup per record and ignore effects the new gauge
does not measure. No placement, scaling, MRC or Nexmark defaults are changed.

## Enable and smoke-test

The active Flink source is vendored, but FRocksDB is a git submodule. Build a
matching Java and JNI pair from the supplied patch against the existing pin; do
not change the submodule pointer or mix two jars/libraries. From the repository
root, using a fresh submodule workspace:

```sh
git submodule update --init sources/libs/frocksdb
test "$(git -C sources/libs/frocksdb rev-parse HEAD)" = d439c0fbf80e87340c728ab3fad1664ac9f72098
patch="$PWD/sources/libs/patches/frocksdb-a4s-block-latency.patch"
git -C sources/libs/frocksdb apply --check "$patch"
git -C sources/libs/frocksdb apply "$patch"
images/frocksdb/build.sh
images/flink-runtime-a4s/build.sh
```

The image build consumes `rocksdbjni-6.20.3-linux64.jar` copied by the first
build script. To validate the matching local JAR by itself:

```sh
jar="$PWD/sources/flink/flink-runtime-1-18-a4s/rocksdbjni-6.20.3-linux64.jar"
tmp="$(mktemp -d)"
javac -cp "$jar" -d "$tmp" scripts/autoscaling/smoke/A4SBlockLatencySmoke.java
FLINK_A4S_SAMPLE_BLOCK_LATENCY=0 java -cp "$jar:$tmp" A4SBlockLatencySmoke "$tmp/off"
FLINK_A4S_SAMPLE_BLOCK_LATENCY=1 java -cp "$jar:$tmp" A4SBlockLatencySmoke "$tmp/on"
# Only where the local filesystem supports direct reads:
FLINK_A4S_SAMPLE_BLOCK_LATENCY=1 java -cp "$jar:$tmp" A4SBlockLatencySmoke "$tmp/direct" direct
```

Set both the TaskManager process environment variable
`FLINK_A4S_SAMPLE_BLOCK_LATENCY=1` and the Flink option
`state.backend.rocksdb.metrics.a4s-block-latency: true`. The RocksDB patch
checks the environment once per process, then samples only if a RocksDB
statistics instance exists. The Flink option allocates statistics when
needed and exports the gauges. To guarantee no sampling, leave the environment
variable unset; if it is set, other DBs in the process that already have
statistics can also be sampled. With the stock Java/native pair the Flink
option logs unsupported telemetry and registers no gauges; it does not affect
controller behavior.

## What it measures

The native sampler picks about 1 in 128 eligible attempts, before knowing
whether they hit, using a private per-thread stream. It times the block
lookup through successful return from the cache or fetch/insert path. Failed
reads, pre-supplied blocks, cache-only reads, no-fill reads, compressed block
caches, memtable reads, metadata, ordinary iterators/seeks and other paths
without a `GetContext` aren't represented. It reports four exclusive classes:

| Class | Meaning |
|---|---|
| `hit` | Hit in the uncompressed RocksDB block cache |
| `miss_direct` | Miss served through a successful call to the direct SST file reader |
| `miss_non_direct_file` | Miss served through the non-direct SST file reader; it may be served by the OS page cache or mmap |
| `miss_other` | Miss served through the prefetch/persistent-cache path instead of a call from this fetcher to the file reader; prefetch may itself do I/O |

Direct read identifies a path, not physical device service: storage, filesystem,
queueing, decompression, checksum verification and cache insertion may all
contribute. The gauge is a difference between measured sample means rather
than a causal device penalty; e.g. hit and miss blocks may differ in size.

Per DB, the Flink view uses differences in cumulative histogram count and
sum (nanoseconds) over non-overlapping windows of at least 60 seconds. Each
`rocksdb.a4s_block_<class>_seconds` gauge is
`Δsum_ns / Δcount / 1e9`. The shadow gauge
`rocksdb.a4s_block_miss_penalty_seconds` pools all miss classes by count
before subtracting the hit mean:

```text
(sum_miss_ns / n_miss - sum_hit_ns / n_hit) / 1e9
```

Use the `rocksdb.a4s_block_<class>_samples` gauges to check coverage.
A mean needs at least 64 samples in its population (a penalty needs 64 hits
and 64 pooled misses). Cold starts, observed counter resets, inconsistent
snapshots, expired results (120 s), and nonpositive deltas do not publish an
estimate (NaN). Snapshots compare to the previous poll as well as the window
start; a reset that fully catches up between polls cannot be identified by
counts alone. Closing a DB discards its estimate. Polling reads count and
sum from one native snapshot per class, but these are approximate under
concurrent native updates. Histogram variance is not used. Don't average
penalty gauges across DBs as if they were additive or equally weighted.

No clock or RNG calls are made for the sampler when off. Even off, the patched
native jar allocates four extra histogram slots per statistics shard, roughly
3.5 KB/core/DB with this build; the stock default jar does not incur that cost.

## Qualification before controller use

Compare restored main, patched with sampling off, and patched with sampling
on on identical optimized builds, measuring throughput, busy time, CPU and
RSS. Characterize cold and warm windows separately, preserving fixed
parallelism, source rate, input seed and storage across matched trials. Check
coverage by read class and by subtask; qualified estimates should survive a
held-out cache-capacity intervention. Compare the change in busy
seconds/record against the change in actual misses/record in the same lookup
population, rather than plugging the DATA-only difference above into the
current MRC. Keep configured latency until those units and populations match
and an intervention shows the new model predicts better than the fixed one.
