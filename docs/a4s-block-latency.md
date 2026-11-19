# Measuring A4S's cache-miss penalty

A4S defaults to the configured `job.autoscaler.a4s.hit-latency-sec` and
`job.autoscaler.a4s.miss-latency-sec` (5 µs and 5 ms). Enable
`job.autoscaler.a4s.online-cache-costs.enabled: true` to fit the cache cost
from previously measured RocksDB hit and miss timings. With this switch on,
an unqualified vertex uses its configured legacy latencies until it has a
qualified window. The offline `job.autoscaler.a4s.calibrated-cache-costs`
map applies when the online estimator is off.

## Enable and smoke-test

The active Flink source is vendored, but FRocksDB is a git submodule. Build a
matching Java and JNI pair from the supplied patch against the existing pin; do
not change the submodule pointer or mix two jars/libraries. From the repository
root, using a fresh submodule workspace:

```sh
git submodule update --init sources/libs/frocksdb
test "$(git -C sources/libs/frocksdb rev-parse HEAD)" = d439c0fbf80e87340c728ab3fad1664ac9f72098
for name in frocksdb-a4s-block-latency frocksdb-a4s-cache-lookup-v1; do
    patch="$PWD/sources/libs/patches/$name.patch"
    git -C sources/libs/frocksdb apply --check "$patch"
    git -C sources/libs/frocksdb apply "$patch"
done
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
javac -cp "$jar" -d "$tmp" scripts/autoscaling/smoke/A4SBlockLatencyCoverageSmoke.java
FLINK_A4S_SAMPLE_BLOCK_LATENCY=1 FLINK_A4S_COUNT_CACHE_LOOKUPS=1 \
    java -cp "$jar:$tmp" A4SBlockLatencyCoverageSmoke "$tmp/coverage"
```

Set `FLINK_A4S_SAMPLE_BLOCK_LATENCY=1` and
`FLINK_A4S_COUNT_CACHE_LOOKUPS=1` in the TaskManager process environment.
Set `state.backend.rocksdb.metrics.a4s-block-latency: true` in Flink and
enable QuickMRC and its stack-distance-histogram metric. The RocksDB patch
checks the environment once per process, then samples only if a RocksDB
statistics instance exists. The Flink option allocates statistics when
needed and exports the gauges. Leave the sampling environment variable unset
to disable sampling. When it is set, other DBs in the process with statistics
can also be sampled. The stock Java/native pair logs unsupported telemetry
and the ABI 1 pair can export diagnostic gauges. Online evaluation requires
the exact lookup-counter patch and timing ABI 2 or 3. ABI 3 identifies
background compaction lookups and keeps them out of the foreground timer.

## What it measures

ABI 3 samples about 1 in 32 eligible foreground attempts before knowing
whether they hit, using a private per-thread stream. ABI 2 sampled 1 in 128.
ABI 3 counts exact background compaction hits and misses at the resident LRU
lookup, including no-fill compaction reads. It times the block
lookup through successful return from the cache or fetch/insert path. Failed
reads, pre-supplied blocks, cache-only reads, no-fill reads, compressed block
caches, memtable reads, and batched MultiGet outside this path are excluded.
ABI 2 includes ordinary iterators/seeks and metadata when they use the same
cache-backed read path. The sampler reports four exclusive classes:

| Class | Meaning |
|---|---|
| `hit` | Hit in the uncompressed RocksDB block cache |
| `miss_direct` | Miss served through a successful call to the direct SST file reader |
| `miss_non_direct_file` | Miss served through the non-direct SST file reader; it may be served by the OS page cache or mmap |
| `miss_other` | Miss served through the prefetch/persistent-cache path instead of a call from this fetcher to the file reader; prefetch may itself do I/O |

Direct read identifies a path, not physical device service: storage, filesystem,
queueing, decompression, checksum verification and cache insertion may all
contribute. The difference in means describes the observed workload. Hit and
miss blocks can differ in size, and iterator prefetch can perform work before
the measured lookup starts.

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

## Online cost and scope

An opted-in request to
`/jobs/<job>/vertices/<vertex>/a4s-metrics?get=cacheCostSamples,cacheCostCompaction`
exports cumulative input records, accumulated task busy time, exact resident
cache lookup and miss counters, eligible timing-path counters, and sampled
hit/miss counts and nanoseconds. The additional selector returns the timing
ABI and, with ABI 3, the exact background compaction counters. The runtime publishes samples for exclusive
single-shard cache owners, identifies the active execution attempt, and omits
ambiguous or stale samples. Flink keeps the existing response shape for calls
without the query parameter.

The autoscaler uses the most recent qualified 60–120 second window in the
existing metric history. Each subtask must have the same execution attempt,
cache identity, capacity and epoch at both ends. The elapsed time comes
from the TaskManager observations. It needs at least 64 timed
hits and 64 timed misses and at least 90% eligible coverage of both exact
foreground populations. ABI 3 subtracts the classified compaction hits and
misses from the exact LRU totals before checking coverage. Unclassified
lookups still count against coverage. ABI 2 checks coverage against all exact
lookups. The up-to-2% allowance above 100% accounts for counters read
separately while the task is running. A changed cache or restarted task warms
up again. Configure `job.autoscaler.metrics.window` to retain at least 60
seconds of samples.

For one subtask, the model is:

```text
extra_seconds_per_foreground_miss = mean_sampled_miss_seconds - mean_sampled_hit_seconds
foreground_miss_count = total_miss_count - compaction_miss_count
foreground_miss_share = foreground_miss_count / total_miss_count
slope = extra_seconds_per_foreground_miss × foreground_miss_share × lookup_count / input_records
zero_miss_cost = (busy_seconds - extra_seconds_per_foreground_miss × foreground_miss_count) / input_records
busy_seconds_per_record(total_miss_fraction) = zero_miss_cost + slope × total_miss_fraction
```

Across subtasks, A4S weights the coefficients by input record count and
inverts its existing miss-rate curve with this per-record cost. QuickMRC's
miss fraction includes compaction. The model uses the observed foreground
share of misses to translate that fraction into task busy time. When
projecting to a different cache capacity, the estimate assumes a similar
foreground share and lookup density. Different subtasks can also have
different access patterns or read costs. A vertex falls back to the fixed
cost when any subtask is unqualified.

The QuickMRC gauge encodes large histogram strings before Flink's
modified-UTF metric transport. The JobManager decodes them to the original
histogram before merging and building the curve. Deploy the matching metric
publisher and reader together. An empty histogram is treated as a missing
sample so the REST response can still carry cache timing counters.

Fixed-allocation replay can test timing prediction and processing-cost error.
A run with scaling actuation enabled measures effects on memory and slot
decisions.
