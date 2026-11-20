# Online QuickMRC miss penalty

## Population and units

QuickMRC estimates misses over the physical cache's resident-lookup population. ABI 3 times a
sample of synchronous foreground accesses and counts compaction hits and misses separately.
Qualification binds both streams to compatible job, vertex, subtask attempt, TaskManager process,
cache ID, capacity epoch, geometry and sampling policy. [Observation contract](a4s-cache-observations-v1.md)

For one subtask, let *R* be input records, *L* exact lookups, *M* exact resident misses,
*Mᶠ* foreground misses, and *B* active task busy seconds in the fit interval.

| Quantity | Formula | Unit |
| --- | --- | --- |
| Actual incremental foreground miss time *a* | mean sampled miss time − mean sampled hit time | s / foreground miss |
| Foreground share *f* | *Mᶠ / M* | fraction of resident misses |
| Lookup density *d* | *L / R* | lookups / input record |
| Effective QuickMRC coefficient *k* | *a × f × d* | s / record / unit miss fraction |
| Zero-miss baseline *b* | *B / R − a × Mᶠ / R* | s / input record |

The residual *b* includes work outside the timed lookup path. The fit uses the full sampled sums
and event counts, including long observations. The fixed comparator assumes 5 µs total hit time
and 5 ms total miss time, for a 4.995 ms increment. The online slope uses the foreground share
once because the independent variable is the fraction of all lookups that miss. ABI 3 supplies
aggregate timing. Changes in the mix of operations, block sizes, and read paths can affect that
aggregate.

RocksDB PerfContext counts and timers are thread-local and describe filesystem operations.
Kernel cache residency, storage, and decompression can all influence application miss costs.
The samples describe foreground latency through the full path. Per-device attribution requires
independent measurements. [RocksDB timing documentation](https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context),
[Symbiosis, FAST 2024](https://www.usenix.org/system/files/fast24-dai.pdf),
[Boosting Cache Performance by Access Time Measurements](https://doi.org/10.1145/3572778)

The project plan cites Wang's operator-level compute and I/O model as the starting point.
The thesis repository returned HTTP 403 during validation.
[Wang, A4S thesis, §3.2.1](https://utoronto.scholaris.ca/bitstreams/e0a39a88-27af-4859-b1c9-6a13b87b8ec4/download)

## Fit and candidate selection

- The fit uses complete 60–120 s endpoint pairs and process-monotonic durations. It requires
  at least 64 sampled hits and 64 sampled misses per subtask and 90–102% coverage of the
  corresponding foreground denominators. Reset counters, incompatible identities, invalid
  timing, incomplete subtasks, and nonpositive costs withhold a fit.
- Coefficients frozen before a disjoint later window are checked against its observed busy
  time using its **actual** miss ratio. A worst-subtask relative error above 25%, a change in
  foreground miss share above ten percentage points, or a lookup-density change above 25%
  withholds selection. This is an empirical stability diagnostic. Event-level variance and
  confidence intervals require additional native timing statistics.
- **FitIdentity** records the attempt, process, cache ID, capacity, epoch, sampling rate, and
  endpoint sequences for each subtask. **Fit** records the wall interval, measured coefficients,
  counts, coverage, and mean hit and miss times. **Recommendation** records byte support, selected
  capacity, forecast busy seconds per record, the worst same-window MRC bias at current capacity,
  and whether unconstrained demand retained the live allocation.
- For a candidate capacity, evaluate each subtask as
  *B/R + k × (m(candidate) − m(current))* using its own QuickMRC. Anchor a live capacity between
  bins at the enclosing upper edge to avoid overestimating improvement. Every cache needs
  support for both the live and candidate capacities. Apply the measured record shares and
  require the predicted work at target throughput to fit within one busy second per second on
  every subtask. Use the existing throughput demand helper, including backlog, restart catch-up,
  and target utilization.
- Changes to attempts, process, cache capacity, or parallelism require fresh observations.
  Qualification needs two disjoint 60–120 s windows. Retain sufficient metric history for both
  and for startup warmup.

## Selection and actuation

| Mode | Meaning |
| --- | --- |
| **LEGACY_FIXED** | Configured total hit and miss costs |
| **OFFLINE_CALIBRATED** | Explicitly configured per-vertex offline coefficients |
| **ONLINE_SHADOW** | Report the fit and candidate while the configured fixed or offline policy selects decisions |
| **ONLINE_QUALIFIED** | A supported learned projection at the current parallelism |
| **INVALID** | Learned selection withheld with a reason such as **UNBOUND_TIMING**, **PHASE_DRIFT**, or **CAPACITY_UNSUPPORTED** |

Set **job.autoscaler.a4s.online-cache-costs.enabled** to true to evaluate the learned model.
Also set **job.autoscaler.a4s.online-cache-costs.shadow** to true to evaluate it while the
configured nonlearned policy continues to act. The **use-fixed-baseline** intervention applies
to shadow projections. An unavailable required observation or learned fit causes a hold.

The branch's executor currently creates requests only when parallelism changes. Qualified
learned projections apply at the measured parallelism. Learned mode holds with
**ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE** until the placement policy supports a cache-only
request at fixed parallelism.

## Validation evidence

| Check | Outcome | Evidence |
| --- | --- | --- |
| Foreground compaction mix | A 20 µs actual foreground penalty yields 16 µs per record per unit of QuickMRC miss fraction at two lookups per record and 40% foreground misses | Java fixture |
| Forecast vs actual-miss diagnostic | At 12,288 B the frozen curve forecasts 116 µs. A later fixture observes 117 µs at 30% misses. The actual-miss plug-in predicts 116.8 µs | Java fixture |
| Capacity support | Candidate support spans 4,096–12,288 B. Unconstrained demand retains 8,192 B. A 10,000 B live allocation uses the 12,288 B anchor | Java fixture |
| Same-allocation Q8 transfer | 41.47 µs predicted against 40.53 µs measured | Archived actual-miss replay |
| P1-to-P3 Q8 transfer | 45.24 µs predicted against 307.76 µs measured. The baseline accounts for about 257 µs of the gap | Archived actual-miss replay |
| Live causal controls and overhead | Paired timing-off/on and isolated cache-only, restart-only, and parallelism-only trials remain pending | Live validation |

The offline calibrator accepts optional trial IDs and forecast timestamps and records a model
fingerprint. Whole trials must stay in a single capacity split. It reports the actual-miss
diagnostic separately from the combined forecast-MRC error. Historical CSVs without timestamps
are labeled capacity-only and undated. The archived Q8 replay uses observed misses and does not
isolate the cause of the change after restore. Live scaling performance for this patch remains
unmeasured.
