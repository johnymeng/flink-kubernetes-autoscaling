# SHARDS port plan

Port upstream's online FRocksDB estimator and histogram contract into the
work repo's existing Flink 1.18 A4S runtime and operator. Keep QuickMRC as the
default during validation and make SHARDS opt-in. The first milestone is a
windowed byte-based miss-rate curve (MRC) from a stateful job, with scaling
decisions disabled. Reuse the upstream estimator with a correction to its
sample eviction rule. The local history identifies no prerequisite FRocksDB
version bump.

## Baseline and design

Both local `main` refs point to `f8ce7fbb`. Upstream
`a4s-shards-mrc` at `7d3211a5` is 59 commits ahead and includes a separate
checkpoint-aware stack, operator migration, and benchmark setup. The review
branch starts at the work repo's `main`. The proposed first port targets
`sources/flink/flink-runtime-1-18-a4s` and
`sources/operators/flink-kubernetes-operator-a4s`, which already contain the
QuickMRC telemetry and A4S consumer. Upstream's Java SHARDS changes target
`flink-1-18-src-from-justin` and `flink-kubernetes-operator-justin`.
If the team chooses the checkpoint-aware stack for deployment, review that
runtime and operator migration as its own change.

SHARDS samples by block identity and supports both online and offline MRC
generation. Wang's 2023 A4S thesis (§§3.4.1–3.4.2) describes how QuickMRC
already uses SHARDS-style spatial sampling alongside bucketed LRU and a ghost
cache. The ECE496 implementation plan (§2.3 and Appendix E) calls for an
online replacement that feeds the existing A4S loop. Upstream implements that
replacement with a separate sampled recency structure and a Fenwick tree over
byte charges. Its offline tools are useful for replay and validation. See
[Efficient MRC Construction with SHARDS, FAST 2015](https://www.usenix.org/conference/fast15/technical-sessions/presentation/waldspurger).

The intended flow is one estimator per physical managed RocksDB block cache,
with a versioned cumulative histogram exported through JNI to the TaskManager.
The JobManager merges eligible observations for the vertex and exposes them
through `/a4s-metrics`. The operator differences cumulative snapshots from
the same cache generation and converts the window to an MRC. Under the
one-stateful-task-per-slot model, point `i` represents per-slot cache capacity
`(i + 1) × binBytes`, multiplied by vertex parallelism on the operator's
cache axis. Keep the conversion to per-task managed memory explicit.

## Commit selection

| Upstream commits | Port decision |
| --- | --- |
| `f3b76f93` | Use the clean FRocksDB source as the native patch base. This commit records an import of RyanL123's QuickMRC at `d439c0fbf`, the exact gitlink in the work repo. The commit also converts the submodule into a tracked directory. |
| `7201ff63` → `91a86f72` → `00f12d66` → `ffff0b18` | Apply the FRocksDB changes in this order. They add the offline replay tool, cache-owned C++ estimator and reader hooks, `LRUCache` JNI API, and corrected byte-LRU hit-size convention. Carry their tests and add the cap regression below. |
| `a70e84a7`, `7cb4c336` | Port the estimator switch, settings, histogram v2 codec, metric publication, byte-axis MRC conversion, and optional merged histogram in the REST response into the existing A4S runtime. Reuse the current QuickMRC format and behavior. |
| `75f543f9`, `da16e171`, `a2d7523a` | Adapt applicable operator hunks for A4S-only collection, stabilization windows and optional SHARDS_adj, and transient MRC storage. Keep the current policy and execution path. |
| `88ef8856` | Use as an accounting reference. The work repo's existing A4S runtime already keeps index and filter blocks outside the shared cache. Include that memory in overhead measurements. |
| `b5c60875`, `55363c2e`, `f502f31c`, `6b3157d2` | Defer the A4S migration into the Justin runtime and operator, submodule switches and operator vendoring. Benchmark and checkpoint workflows also stay outside the first port. |
| `ab8b00f2`, `e17c83e2`, `602c9558`, `fea0f592`, `7d3211a5` | Review feasible parallelism, minimum parallelism, managed-memory sizing, profile retention and slot-sharing bounds before enabling SHARDS-driven decisions. These are controller and scheduler changes beyond the telemetry milestone. |

The work repo's FRocksDB gitlink is
`d439c0fbf80e87340c728ab3fad1664ac9f72098`. The submodule is uninitialized
locally and the pinned commit object is unavailable in the work repo. The
native patches applied cleanly to a scratch export of `f3b76f93` and
reconstructed the FRocksDB source tree at `7d3211a5` exactly. Prefer applying
the patches inside the FRocksDB submodule and recording an immutable pin when
that commit can be shared. Upstream's tracked source snapshot is a local
alternative if the team chooses to vendor the tree. Keep that layout decision
separate from the estimator. If a version bump proves necessary in the native
or image build, stage it on its own branch first and reproduce the QuickMRC
baseline there.

The existing `images/frocksdb/build.sh` hands the native JAR to
`images/flink-runtime-a4s/Dockerfile`, which installs it as
`org.rocksdb:frocksdbjni:6.20.3-custom`. Use a freshly built JAR with both
the Java and native SHARDS methods. Verify which artifact the build and
runtime load, since an older QuickMRC-only JAR can share those coordinates.
The online estimator uses standard C++ containers. The offline
`ShardsMRC` implementation uses GNU libstdc++'s policy-based tree.

## Corrections required for the first port

- **Native sample cap.** `ffff0b18` records a hit size of
  `sampledNewerBytes / R + ownCharge`. Eviction still compares
  `trackedBytes / R` against the cap, scaling the oldest block's own charge
  as well. A standalone build of the upstream estimator matched the expected
  full-sample byte-LRU bins for `A(5), B(2), A(5), A(5)`. With 1-byte bins,
  `R = 0.5`, an 8-byte cap, and a sampled 5-byte block read twice, it
  reported zero hits, two infinite-bin misses and two cap evictions. The
  second read should hit at 5 bytes. Align the distance cutoff with the
  oldest candidate's hit size and test mixed charges, cap boundaries and
  non-divisible bin widths. If a separate memory limit truncates history
  earlier, expose that limit as reduced curve support.
- **Observation identity and coverage.** Upstream's handler picks the first
  matching metric per subtask, while the estimator is cache-owned.
  Validate one observation per physical cache, attribute slot-shared traffic
  only when ownership is unambiguous, and treat partial rosters as unavailable
  for decisions. Reset the baseline when an attempt, cache roster, estimator
  configuration or parallelism changes. Comparing merged counter totals alone
  can conceal a replacement cache when other caches keep accumulating.
- **Missing and stale telemetry.** Validate consistent SHARDS header settings
  and bins. Propagate an explicit unavailable status for empty, insufficient
  or stale samples. Preserve the deployed memory profile while the curve is
  unavailable.

## Work repo PRs

| PR | Reuse in the SHARDS port |
| --- | --- |
| [#2](https://github.com/johnymeng/flink-kubernetes-autoscaling/pull/2) | Use the cache-owned QuickMRC observation and aggregation tests as references for deduplication and for preserving deployed profiles during telemetry holds. The closed PR is a reference for targeted changes. |
| [#3](https://github.com/johnymeng/flink-kubernetes-autoscaling/pull/3) | Adapt explicit unavailable outcomes, sanitized gauge names and curve validation. Its remaining coverage and pooled-cache attribution questions also apply to SHARDS. Test active-attempt and physical-cache rosters, header and bin compatibility, and baseline reset when cumulative observations change generation. |
| [#5](https://github.com/johnymeng/flink-kubernetes-autoscaling/pull/5) | Use the monitor shutdown change and lifecycle test in `3c57eae8` when wiring native histogram polling. Review it independently of the draft PR's slot-reuse changes. |

The native sample-cap correction is independent of these PRs. Before
porting the Java telemetry, check which relevant fixes are in the chosen
integration base and bring across the applicable tests with any missing
behavior.

## Implementation and acceptance

1. **Native cap correction and JNI.** Start with a FRocksDB regression for
   the two reads of a sampled 5-byte block at `R = 0.5` and cap 8. Change
   eviction to use `(trackedBytes - oldestCharge) / R + oldestCharge`,
   consistent with the hit-size calculation. Require a hit on the second
   read, retain entries at the cap and check eviction above it with mixed
   block charges. Verify that a partial final bin respects the advertised
   byte limit. Rerun `shards_mrc_estimator_test`, `block_cache_tracer_test`
   and both `shards_trace_replay` modes. Then build and test `LRUCache` JNI
   using the same JAR the image will bundle. Verify full-sample byte-LRU against an
   independent oracle and compare online counts with `--estimator` replay
   for identical keys, charges and seed. Exercise Get, MultiGet and iterator
   traffic, repeated opens and closes, and concurrent snapshot collection.
   Measure lock cost, resident estimator memory and cap evictions at the
   proposed 1% sampling rate.
2. **Runtime and operator.** Enable SHARDS explicitly on the managed
   RocksDB cache and publish histogram v2. Test mixed formats, incompatible
   configurations, warm-up, cache recreation with stable parallelism, and
   absent curves. Start with plain SHARDS and expose `SHARDS_adj` as a
   comparison setting. Verify the snapshot's counts and finite byte range, the
   REST payload and the in-memory window. Keep full curves out of the
   persisted autoscaler metric history.
3. **End-to-end evidence.** Use a local, fixed-profile stateful A4S job for
   the first smoke run. Inspect the TaskManager metrics, JobManager response,
   and operator window across a restart or rescale with scaling decisions
   disabled. Check QuickMRC on the same images. Then compare online
   QuickMRC, plain SHARDS and SHARDS_adj against independent exact byte-LRU
   replay of full eligible user-data traces, using matched windows, cache
   sizes and settings. The offline `ShardsMRC` mode works in sampled block
   counts, and `--estimator` reuses the online implementation. Use a
   separate exact oracle for the accuracy comparison. Start with
   `db_bench` and add comparable Nexmark Q4, Q18 and Q20 runs. Per the
   ECE496 proposal (R2), require a 10% relative MAE reduction over QuickMRC
   on the same cache-size points. Measure CPU and peak memory in matched
   runs with tracing disabled. Validate per-task managed-memory conversion
   against measured data-cache and write-buffer usage before enabling
   SHARDS-driven scaling.

The four native patches landed on `jason/shards-upstream` at `db6ae5ba`.
All 16 native tests passed, both replay modes passed a small trace check,
and the Java and JNI code compiled. The standalone sample-cap regression
still reproduces. JNI packaging, Flink integration and workload measurements
remain acceptance work.
