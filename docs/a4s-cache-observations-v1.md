# A4S cache observations v1

The versioned Java transport uses the existing single-shard QuickMRC resident-lookup ABI.
Its deterministic fixtures cover the same population and byte units.

## Trace a cache to the controller

1. The process-scoped registry has one publisher per native physical cache ID. Wrapper aliases
   can supply the same publisher with a read source. Each backend and monitored DB has its own
   lease. Ownership changes advance the ownership generation and invalidate the old publication.
   Prior use and concurrent backend or DB leases prevent exclusive attribution.
2. Each cache publishes at most one interval on metric queries spaced at least ten seconds apart.
   The first publication establishes the baseline. Every observer of the same allocation and
   interval gets the same sequence and native interval. Shared intervals carry a diagnostic
   status and produce no independent vertex curve. Closing the last RocksDB shared resource
   prevents further native reads before closing its JNI handle.
3. `rocksdb.a4s-cache-observation` identifies the sender by job, vertex, subtask and execution
   attempt. The window adds a process UUID, native cache ID, native capacity epoch, sequence,
   backend lease IDs, population, charge and eviction policies, sampler rate, actual native capacity
   in bytes, process-local monotonic interval, wall time, and exact native interval counts.
   The histogram provides sampled reference counts, cold misses and rounded charge quanta.
   Its nominal coordinate range runs from one bin through the last emitted bin, inclusive.
   An aligned v2 observation also carries a cumulative histogram and exact counts at the
   native snapshot, its ownership generation and the paired ABI 3 task/DB timing read.
4. Query `GET /jobs/<job>/vertices/<vertex>/a4s-metrics` using
   `get=cacheObservations,cacheCostSamples,cacheCostCompaction`. The current execution graph
   provides every expected running attempt. Match each response's `cacheObservations.subtasks`
   against job, vertex, subtask, attempt and `window.process/cacheId/epoch/sequence`. Inspect
   `status`, `expectedSubtasks`, `oldestAgeMillis` and `wireBytes` before reading `scaledMrc`.
   The sibling `scaledMrcCapacityAxis` labels versioned aggregate coordinates as
   `NOMINAL_ROUNDED_4K`. Missing or unknown axes are unverified. Roster handling finishes
   before the corresponding metric dumps update the store, even when roster discovery fails.
5. The autoscaler stores the most recent batch, constituent observations, and independent timing
   samples in `CollectedMetrics`. Under Justin+A4S every unfinished vertex requires a complete
   observation unless explicitly exempted by its vertex ID in
   `job.autoscaler.a4s.cache-metrics.exempt-vertices`. Exemptions are for verified non-RocksDB or
   stateless vertices. With required invalid data the autoscaler logs the reason and holds the
   current allocation, including when old overrides remain in the state store.

The positive two-subtask example is
`sources/flink/flink-runtime-1-18-a4s/flink-runtime/src/test/resources/a4s/cache-observations-v1.json`.
Both entries in `observations` are complete synthetic decoded gauge examples. A successful REST
response has these entries under `cacheObservations.subtasks` with status `VALID` and carries
the fixture's `curve` as `scaledMrc`. The archived Q8 example in
`outputs/q8-ryan-online-autoscaling/controller/transport-preflight/repaired-live-receipt.txt`
belongs to the earlier raw QuickMRC and ABI-3 timing transport. Use it for provenance of the
failure and baseline only.

## Aggregation and quality

For cache *i*, let *Lᵢ* be exact interval references, *Dᵢ* the monotonic window duration, *Hᵢ* the
total sampled references including complete misses, and *hᵢ(Cᵢ)* the sampled references with reuse
distance up through byte capacity *Cᵢ*. The local miss rate is *1 − hᵢ(Cᵢ)/Hᵢ*. The vertex miss rate
is *Σᵢ rᵢ(1 − hᵢ(Cᵢ)/Hᵢ) / Σᵢ rᵢ*, where *rᵢ = Lᵢ/Dᵢ* is the cache's lookup rate. Raw reference
counts give equivalent weights when the observation durations match.
This projection uses equal per-cache capacity *Cᵢ = C/P* for
*P* distinct, exclusive caches with matching actual capacities, sampling rate and bin geometry.
It uses common support only and requires max/min lookup rate at most 1.25. The cache budgets
in the observations are native block-cache bytes. Allocation and managed-memory translation
belong to the planner.

Snapshot schema 1 is additive to the existing native lookup API and timing ABI 3. Under the
physical shard mutex, the native snapshot copies the capacity generation, increasing sequence,
exact lookups and misses, and cumulative complete misses and finite bins. Accesses and capacity
changes use the same mutex. Each interval differences two snapshots of one compatible generation
and ownership. Adjacent compatible snapshot pairs partition the observed access stream.
A delayed publisher poll still covers the full interval between its endpoints. Consumers need
evidence of coverage if they miss a published interval. The publisher rebaselines on a new
generation or decreasing counter.
Routine polling does not reset native counters, resident recency, or ghost history.

The legacy histogram getters still return a resettable interval. Their separate reads and
legacy reset cannot certify a coherent interval. Resetting that interval leaves the cumulative
snapshot and reuse state intact. Snapshot schema mismatch or unavailable JNI capability is
explicitly ineligible. Only one physical shard is supported. Timing sample and busy-time reads
have separate consistency boundaries.

Native counters describe resident-cache lookups. The estimator rounds each entry's payload charge
up independently to 4,096-byte quanta and tracks those quanta in grouped distance buckets.
A bin scales that distance by the configured number of quanta. The numeric curve coordinates
express multiples of those quanta. The physical cache's capacity is independently observed in
bytes, and the managed-memory budget also covers non-cache resources. Metadata charge, pinned
entries and high-priority LRU pools are additional limits of the nominal counterfactual.

The new `quickmrc-rounded-4k-charge-v1` policy token declares this approximate domain. The historical
`fixed-4096-byte-item` token describes the same nominal axis. The fixed policy can retain numeric
diagnostics, but its inversion provides no verified native byte allocation. The sampled timing fit
can remain valid independently. Exact learned allocation requires a separate byte-charge and
eviction-policy attestation, which this QuickMRC producer cannot supply. The consumer also
rejects unrecognized capacity-axis claims for published aggregate curves.

Monotonic duration is local to each process. REST accepts 9–30 s windows with wall start and end
skew at most 5 s and end age at most 30 s. The skew check assumes synchronized worker wall clocks.
`IDLE`, `ABSENT`, `WARMUP`, `SHARED_CACHE`, `INCOMPLETE`, `COUNTER_RESET` and
`CACHE_CHANGED` all withhold a curve. Unknown schema versions, unknown populations, mismatched
attempts and incompatible bins are explicit invalid states. ABI 3 keeps its existing sample
classification and counter units. The aligned endpoint brackets the native, task and per-DB
reads on the publisher's monotonic clock with a 100 ms acquisition limit. The REST receiver
binds the timing to the selected attempt and checks it against the same endpoint's native
identity and cumulative counters. Process-local monotonic clocks are never compared across
TaskManagers.

The operator differences compatible cumulative endpoints over the actual timing fit and validation
intervals. A missing poll is acceptable when compatible endpoints bound the full interval. Fit and
holdout are adjacent and share exactly one endpoint. A short, latest histogram and a longer
timing fit do not establish coverage. Legacy timing and oversized observations that only carry
a v1 interval remain readable as diagnostics. The 16 MiB per-job endpoint budget keeps or
evicts a vertex population together. Endpoints are excluded from persisted history, so a
restart needs fresh observations.

The codec preserves small JSON and sends larger values as `a4s-gzip-v1:<base64>`, with an
explicit omission sentinel. Encoded gauges fit the 65,535 modified-UTF byte ceiling. Decoding
bounds each expanded gauge to 4 MiB. REST also bounds cumulative wire and decoded size per vertex.

## Compatibility matrix

| TaskManager publisher | JobManager receiver | Operator | Expected behavior |
| --- | --- | --- | --- |
| Legacy histogram/ABI-3 timing | Legacy | Legacy | Original behavior, including its attribution limitations |
| Legacy | Versioned | Versioned | Legacy histogram recognized as unsupported. Required vertices hold |
| Versioned | Legacy | Versioned | No batch in response. Required vertices hold |
| Versioned | Versioned | Legacy | Legacy route has no versioned histogram. Mixed deploy does not provide the new controller safety gate |
| Versioned | Versioned | Versioned | Valid slices reach the collector. Incompatible, missing and expired slices cause hold |

Use the matched native library and Java binding in each TaskManager. Upgrade TaskManagers,
JobManager and operator together before enabling this reconciliation path.
The remaining live check is two concurrent RocksDB backends on the same physical cache followed
by an isolated exclusive placement. Match cache identity and owner scope across metrics and verify
the controller logs an explicit shared-cache hold before accepting the new exclusive window.
