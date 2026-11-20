# A4S cache observations v1

The versioned Java transport uses the existing single-shard QuickMRC resident-lookup ABI.
Its deterministic fixtures cover the same population and byte units.

## Trace a cache to the controller

1. Each Flink-managed RocksDB allocation owns its registry entry. The owning Java cache wrapper
   lives as long as that physical allocation. Each backend resource container acquires a local
   owner lease before opening its DB and releases it during close. An allocation seen by a second
   backend stays ineligible for exclusive attribution until the physical cache is recreated.
2. Each cache publishes at most one interval on metric queries spaced at least ten seconds apart.
   The first publication establishes the baseline. Every observer of the same allocation and
   interval gets the same sequence and native interval. Shared intervals carry a diagnostic
   status and produce no independent vertex curve. Closing the last RocksDB shared resource
   prevents further native reads before closing its JNI handle.
3. `rocksdb.a4s-cache-observation` identifies the sender by job, vertex, subtask and execution
   attempt. The window adds a process UUID, native cache ID, native capacity epoch, sequence,
   backend lease IDs, population, charge and eviction policies, sampler rate, live capacity
   in bytes, process-local monotonic interval, wall time, and exact native interval counts.
   The histogram provides sampled reference counts, cold misses and capacity-bin units. The
   supported region runs from one bin through the last emitted bin, inclusive.
4. Query `GET /jobs/<job>/vertices/<vertex>/a4s-metrics` using
   `get=cacheObservations,cacheCostSamples,cacheCostCompaction`. The current execution graph
   provides every expected running attempt. Match each response's `cacheObservations.subtasks`
   against job, vertex, subtask, attempt and `window.process/cacheId/epoch/sequence`. Inspect
   `status`, `expectedSubtasks`, `oldestAgeMillis` and `wireBytes` before reading `scaledMrc`.
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

For cache *i*, let *Lᵢ* be exact interval references, *Hᵢ* the total sampled references including
cold misses, and *hᵢ(Cᵢ)* the sampled references with reuse distance up through byte capacity *Cᵢ*.
The local miss rate is *1 − hᵢ(Cᵢ)/Hᵢ*. The vertex miss rate is the sum of local miss rates weighted
by *Lᵢ*, divided by total *Lᵢ*. This projection uses equal per-cache capacity *Cᵢ = C/P* for
*P* distinct, exclusive caches with matching actual capacities, sampling rate and bin geometry.
It uses common support only and requires max/min lookup rate at most 1.25. The cache budgets
in the observations are native block-cache bytes. Allocation and managed-memory translation
belong to the planner.

The publisher warms up after a physical allocation and detects generation changes and decreasing
native counters. The legacy QuickMRC JNI separates counter reads, histogram reads and resets.
The publisher brackets the drain and rejects intervals with over 2% of their references at the
boundary. Its counters describe resident-cache lookups while its nominal bin charges are 4 KiB
per item. Variable block sizes can still bias the nominal capacity estimate.

Monotonic duration is local to each process. REST accepts 9–30 s windows with wall start and end
skew at most 5 s and end age at most 30 s. The skew check assumes synchronized worker wall clocks.
`IDLE`, `ABSENT`, `WARMUP`, `SHARED_CACHE`, `INCOMPLETE`, `COUNTER_RESET` and
`CACHE_CHANGED` all withhold a curve. Unknown schema versions, unknown populations, mismatched
attempts and incompatible bins are explicit invalid states. ABI-3 timing fields are carried on
their existing separately selected path. Timing wire version 3 appends the process UUID and
monotonic capture time to the existing ABI-3 counter sequence. The versioned REST selector
exposes them as process and observedAtNanos while retaining timingAbi 3. They bind timing to the
QuickMRC cache and allow elapsed windows within one process. Older timing payloads remain
readable for diagnostics. A timing sample's presence alone does not establish a
compatible MRC window or population.

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

Upgrade TaskManagers, JobManager and operator together before enabling this reconciliation path.
The remaining live check is two concurrent RocksDB backends on the same physical cache followed
by an isolated exclusive placement. Match cache identity and owner scope across metrics and verify
the controller logs an explicit shared-cache hold before accepting the new exclusive window.
