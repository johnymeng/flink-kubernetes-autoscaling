# A4S verification and benchmark evidence

The comparison baseline is GitHub main at `a285fcf4453d38feff9c80b53bf57c8207d30930`. The implementation and its lab artifact pins are separate from the unmerged online-calibration branch.

## Verified locally

| Area | Result |
| --- | --- |
| Standalone API, core, runner and replay | Fifteen Maven tests passed. The packaged JAR served a Q8-shaped snapshot without Flink, returned a state-preserving recommendation for six subtasks, and agreed byte-for-byte with embedded evaluation and replay. The packaged JAR also replayed a captured HTTP recommendation byte for byte with verification enabled. Regression cases cover a mismatched recommendation, a missing response and preservation of existing output after failure. The tests cover malformed and oversized requests, unsupported actions, measured vs nominal cache coordinates, timeouts and duplicate requests. A forced child-JVM exit while a request was evaluating produced no plan. A replacement runner returned the same plan as embedded evaluation on retry. Worker checks reject plans whose tasks cannot fit together despite sufficient aggregate resources. Both memory and CPU fragmentation are tested, including a fractional CPU boundary. Cache sizing checks the final allocation after rounding and cache bounds. Regressions cover rounding past the measured endpoint and forecasts that are nonfinite or below demand. Evaluation and plan validation share the snapshot and histogram-window checks. Regression cases reject expired requests and stale histograms, and accept the first permitted parallelism for an operator whose cache allocation is unchanged. |
| Flink 1.18 operator | All 41 `ScalingExecutorTest` cases passed as part of 91 focused Flink tests. A real loopback HTTP request exercised the remote client and executor together. Embedded and remote modes both stored P2 and a 116 MiB managed reservation for the matched fixture. A stale response, HTTP 503 or a response that stalled after sending its headers left scaling history and overrides empty. Other executor cases covered a generation rollover, a concurrently changed applied profile, and an unresolved reconfiguration. A subtask restart with an unchanged job and vertex summary invalidated the old measurement window. A local MiniCluster smoke parsed the roster fields from the real vertex-details REST response. Another smoke observed a complete JobDetails output count of 20 across two source subtasks. A local MiniCluster with the updated A4S handler returned an observed backlog of 7 from two real source subtasks. Seven targeted runtime handler tests passed, covering complete zero, missing counters, retired attempts, invalid counters, aliases, and omission of the field for legacy requests. Adapter regressions cover missing and resetting counter samples, a complete zero-rate window, and conversion to records per second. An injected state-store flush failure reproduced an uncommitted apply before the fix. The regression test now verifies no realization after a failed flush, rejection of incomplete override pairs and normal realization of a complete pair after a successful retry. Quota regressions cover a scale-up that adds a partially occupied TaskManager while other vertices stay unchanged, continued slot sharing, and per-worker memory growth at fixed parallelism. The full autoscaler module suite reported the same eight pre-existing failures on both branches in option registration, dataset expectations, and the ordinary parallelism policy. The new code compiled in the complete operator reactor against the pinned lab runtime. |
| Stateful recovery | Both existing `RocksIncrementalCheckpointRescalingTest` cases passed with Flink's local keyed-operator harness using Java 11 source/target and the matched RocksDB JNI artifact with Snappy. Both cases of the existing `RescaleCheckpointManuallyITCase` also passed in a local Flink MiniCluster with the matched JNI artifact. Each case restored a real RocksDB checkpoint at a different parallelism and checked the expected aggregates for 42 keys. Both [local checkpoint rescale smoke](a4s-standalone.md#local-checkpoint-rescale-smoke) modes passed against patched Flink 1.18. The executor validated and stored a standalone HTTP recommendation using synthetic demand and miss-rate inputs and an in-memory executor state store. The test driver submitted it through Justin and verified a new RUNNING epoch, source parallelism 1, keyed-state parallelism 2 and unchanged applied profiles. Both modes verified 200 distinct IDs with their expected keyed counts and replayed the recorded recommendation. |
| Kafka offset and output correctness | The local Kafka 3.7.2 broker reported committed offsets of 50 and 100 on each of two input partitions at the first and final checkpoints. The Flink Kafka sink used exactly-once transactions. After the job stopped, an independent `read_committed` consumer scanned through the output partition's end offset and verified all 200 committed outputs against the deterministic ID and keyed-count oracle. Committed output correctness with the same oracle on Kubernetes remains unverified. |
| SHARDS | The independent C++11 test passed. The built RocksDB adapter and the original estimator emitted identical histograms and counters on nine 8,000-access traces covering three seeds and three sampling rates. The measured time field was excluded from equality. Full JNI rebuild and live resource overhead are outside that check. |

The original A4S placement and MRC-to-MPC suites passed 22 tests before extraction. On the supported [retry fixture](../sources/a4s/examples/legacy-retry.json), the legacy P1 point needs 700 MiB and exceeds the reservation bound. The original and standalone policies select P2 and reserve 116 MiB per task, including the 16 MiB base. On the [archived-curve fixture](../sources/a4s/examples/q8-archived-curve.json), the original plateau fallback selected P4 and a 17 MiB managed reservation. V1 reports an infeasible model target. This is an intentional validation difference.

The second fixture carries an unchanged 38-point nominal `scaledMrc` from the first available sample in the archived Q8 run `live-b4273691`, with measurement epoch `1795588652004` and source-record SHA-256 `64c177ac2035aa42810f9eb1bbfe7f303ceff17b4e6d2d834fa0fcdd570eb98c`. The target demand, current configuration, timing and model settings in the fixture are test inputs. Replay checks the policy's response to that curve and provides no counterfactual throughput evidence.

The [three-way decision check](a4s-standalone.md#compare-legacy-embedded-and-remote-decisions) compared 41 identical requests using the compiled original policy, embedded core and packaged runner over loopback. It used all six nonempty curve observations in `checkpoint-and-native.jsonl` from `live-b4273691`, five synthetic demand levels per curve, nine single-operator cases, and two chained-operator retry cases. All embedded and remote plans matched byte for byte, and all 41 recorded responses passed replay verification. The original and extracted policies agreed to the byte on 598 supported MPC points and agreed on the selected allocations or exhausted retry outcome in 26 requests. The original produced 226 additional MPC points from its plateau fallback. It selected a configuration based on that fallback in 15 requests. V1 found a higher supported parallelism in nine and held the current allocation in six. The archived curves use nominal bytes, so the check verifies legacy slot reservations and model support for the supplied targets. Physical placement and throughput require measurement on the deployment.

### Revalidate forecast metadata before scaling

Two local regressions reproduced acceptance of inconsistent forecast metadata and a call to the migration estimator for an over-budget candidate. The validator now builds the candidate from the local forecasts, checks budgets before estimating migration, and compares the reported prediction uncertainty and migration estimate with the local values.

All 19 standalone Maven tests and 49 Flink scaling-executor cases passed. The executor rejected a loopback runner reply with an unsupported migration estimate and kept scaling history and both override maps empty. The offline Flink test used the established formatter and Checkstyle skip flags. The new runner replayed all 209 archived Q8 and Q20 decisions exactly, and the 89 actionable plans passed local validation with their recorded inputs and clocks.

### Source backlog transport

The Q20 archive has 222 source entries across 111 A4S requests, all with unavailable snapshot backlog. The operator was reading the optional REST field but dropped it before constructing the request. The adapter now carries the latest verified count as a `Long` outside the persisted rate history. A missing or invalid observation on the next collection leaves the next snapshot field unavailable. Flink's existing lag input to the sizing policy remains available through the ordinary metric collector.

A local Flink 1.18 MiniCluster ran two emitting source vertices with two subtasks apiece. The normal operator collector read their verified REST backlog counts of 0 and 7, used confirmed applied slot profiles in the normalized request, and submitted it to a separate HTTP runner. The runner returned `NO_CHANGE`, and replay verified the captured response exactly. The collector-to-snapshot regression also verified `Long.MAX_VALUE` through JSON, exclusion of a non-source observation, and removal of previously available backlog on a later missing or invalid response. All 56 focused collector, evaluator and autoscaler tests passed.

### Conditional Justin runtime write

Six focused Flink runtime tests passed. They covered the independent applied-profile and execution-token opt-ins, resource serialization, and a real Dispatcher rejecting a conditional request before any JobGraph-store write. A local Flink 1.18 MiniCluster tested the REST path with a separate A4S runner. A stale token returned HTTP 409 before scaling, and a current token applied the 1-to-2 checkpoint rescale with the expected slot profiles. Reusing the old token after the new execution started returned HTTP 409 and the join stayed at parallelism 2. The fixture verified all 200 expected IDs and keyed counts, and replay reproduced the recorded A4S recommendation.

The local checkpoint test used RAM-backed scratch after an initial run hit the workspace disk limit. The test driver submitted the conditional write. [Runtime contract](a4s-standalone.md#conditional-justin-writes)

### Conditional operator reconciliation

A second local MiniCluster check used the normal Flink adapter and a separate A4S runner. The adapter persisted the evaluated token with the target without staging deployment overrides. The production native service sent one conditional PUT. The test discarded the accepted reply, and the service observed the new running execution and confirmed both slot profiles without issuing a second write. Flink kept the same job ID, restored RocksDB state at parallelism 2, and produced all 200 expected IDs and keyed counts. Replay reproduced the recorded recommendation exactly.

Focused autoscaler tests cover a changed token during evaluation, an unsuccessful state-store flush, and promotion of confirmed overrides. Kubernetes tests cover HTTP 409, ambiguous replies, ConfigMap recovery, and updating deployment status only after confirmation. The Kubernetes reconciliation tests use local fixtures.

### Checkpoint recovery capability

The patched AdaptiveScheduler reports whether a completed checkpoint is retained alongside the opt-in Justin execution token. It checks the retained checkpoint again before a guarded write. The operator confirms the capability during evaluation and before submission. A missing field from an older runtime leaves recovery capability unavailable in guarded mode.

A local MiniCluster ran the checkpoint fixture with the periodic interval set beyond the smoke's duration. Before the first checkpoint, the scheduler rejected a PUT with the correct token and a caller-supplied recovery claim with HTTP 409. The allocation and token stayed the same. The separate runner returned `UNSUPPORTED_ACTION` when the adapter supplied the unavailable capability. After one manually completed checkpoint, the runner returned `RESCALE`. The production operator confirmed the new allocation after a deliberately dropped reply and issued one PUT. Flink restored RocksDB state at parallelism 2 and produced all 200 expected IDs and keyed counts. Both recorded recommendations replayed exactly.

Eight runtime and nine Kubernetes operator tests passed. The autoscaler batch passed 90 of 91 cases. The remaining `MetricsCollectionAndEvaluationTest.testEndToEnd` metric-set assertion also failed on the recorded original baseline.

### Pending targets after a configuration change

A local Flink MiniCluster with a separate runner produced a valid conditional target and then received a lower CPU quota before submission. The real autoscaler discarded the stale target, and the native service sent zero PUTs. The Flink execution token and allocation stayed the same. After a fresh evaluation, the test discarded the reply to one accepted PUT, then switched away from Justin. The autoscaler confirmed the already accepted allocation, preserved all 200 expected outputs across checkpoint recovery and kept the write count at one. All three recorded recommendations replayed exactly.

Focused tests covered changed quotas, switched policies, disabled autoscaling, and pending records written before the settings fingerprint existed. A Kubernetes reconciliation test supplied a newer deployment generation and verified that the stale reconcile stopped before contacting Flink.

### Verified TaskManager placement

The Flink adapter now carries running subtask placement from the attempt-level REST roster into the standalone request. A changed TaskManager changes the execution generation even when the subtask index and attempt number stay the same. An incomplete or restarted roster defers evaluation. Finished vertices have no running placement.

A local Flink MiniCluster with a separate runner recorded three requests across the guarded checkpoint rescale fixture. The placement maps for both vertices in each request exactly matched the TaskManager IDs returned by direct Flink REST reads. The three recommendations replayed exactly, and the restored job produced all 200 expected outputs. All 95 focused autoscaler tests passed, including placement changes, reordered roster entries, missing subtasks and HTTP request decoding.

### Measured slot cache capacity

A local Flink job with two RocksDB column families reported four native capacity gauges for one 55,924,053-byte block cache. The task’s applied managed reservation was 67,108,864 bytes. The real operator REST collector and evaluator carried the native value into a snapshot and the separate runner received it once as vertex telemetry. The stateless source had unavailable cache capacity. Both recorded recommendations replayed exactly. The checkpoint rescale preserved the counts from both column families and produced all 200 expected outputs. The snapshots retained the existing legacy allocation semantics.

Runtime tests reject duplicate cache identities across subtasks, conflicting aliases, retired attempts, partial coverage and arithmetic overflow. All eight runtime tests, 16 standalone tests and 95 focused autoscaler tests passed. The contract tests verified exact integer transport above the precision of a Java double and omission when capacity is unavailable.

### Measured slot cache usage

In a local Flink run, the normal operator collector and evaluator reported 16,524,648 bytes used in a 55,924,053-byte slot cache. Four native column-family usage gauges reported the same occupancy. Both requests to the separate runner contained the single native usage value. Both decisions replayed exactly, and the checkpoint rescale preserved the keyed counts and all 200 expected outputs.

The live fixture enabled capacity and usage gauges while leaving hit and miss counters disabled. The collector now leaves the hit rate unavailable when either counter is missing. Nine runtime tests and 108 focused autoscaler tests passed. They cover the newest sample across duplicate gauges, zero usage, retired attempts, conflicting or shared identities, missing counters and exact integer transport.

### Paired cache hit and miss counters

The local Flink fixture checkpointed the keyed state to SST files, then processed twenty more inputs to exercise block reads. Direct RocksDB statistics gauges reported six hits and two misses for the live database. The production operator collector and evaluator carried the same integer pair. Both requests to the standalone runner recorded that pair for the stateful vertex and unavailable counters for the source. Both decisions replayed exactly, and the checkpoint rescale preserved all 200 expected IDs and keyed counts.

Ten runtime tests and 108 focused autoscaler tests passed. Coverage includes multiple chained databases, stale attempts, repeated aliases, zeros, counter regression, overflow and exact transport above double precision.

### Measured live SST state size

The local Flink fixture checkpointed two RocksDB column families. Their native live-data estimates were 1,008 and 1,009 bytes. The operator collector and evaluator reported a single database estimate of 2,017 bytes. Both requests to the standalone runner carried that value for the stateful vertex. Both decisions replayed exactly, and the checkpoint rescale preserved all 200 expected outputs.

Eleven runtime tests and 108 focused autoscaler tests passed. Coverage includes chained databases, duplicate gauges, retired attempts, a valid zero, decreasing estimates, overflow and exact integers above double precision. A native monitor lifecycle test verified that the gauge starts unavailable, reports zero for an empty database after sampling and clears on close.

### Observed RocksDB point-read timing

A local Flink checkpoint fixture enabled the native `DB_GET` timing option and collected 291,000 ns across 240 calls. The normal REST collector and evaluator carried the pair into both standalone requests for the stateful vertex. The stateless source reported unavailable timing. Both requests replayed exactly after the rescale, which preserved all 200 expected IDs and keyed counts.

All 13 focused runtime handler tests and 55 operator cases passed. The tests covered exact integer transport above double precision, current-attempt attribution, aliases, complete zero observations, and invalid or missing sample pairs. The local smoke used the changed Flink classes over the pinned runtime artifacts and the existing JNI library.

### Verified task-time windows

A local Flink MiniCluster supplied a complete window to the production metrics collector and evaluator. The stateful vertex’s collected counters advanced by 2 ms busy and 484 ms idle, with zero backpressured milliseconds. The request to the independent runner reported busy and idle fractions of 0.004115 and 0.995885. The source lacked a complete breakdown in this fixture. The measured-window recommendation and two checkpoint fixture recommendations replayed exactly. The checkpoint rescale preserved all 200 expected outputs.

Sixteen runtime tests passed, including a clock-controlled check for ongoing idle and backpressure before the periodic refresh. The 119 focused autoscaler tests cover missing samples, roster changes, counter resets, and zero elapsed time. A separate operator test verified that the roster and counters survive ConfigMap recovery.

## Local packaging check

The 44-module Flink dependency reactor used by the operator image built with its existing Java source target. Replacing local `var` declarations in three runtime test classes fixed test compilation in that build. All 15 tests in those three classes passed. Both the pinned original and refactored operator Maven reactors produced shaded JARs against the same rebuilt Flink dependencies. The packaged runtime served backlog from two real source subtasks, and the packaged refactored executor, runtime and standalone runner passed the local Kafka checkpoint rescale and replay check using the pinned RocksDB JNI artifact. JVM class-load logs identified the JAR used for each of those components.

### Current Maven packages

The current branch built the full Flink runtime and RocksDB backend modules, the shaded Kubernetes operator and the standalone runner from committed sources with matching Flink SNAPSHOT dependencies. The operator’s two focused suites passed all eight tests through Maven. The operator declares Mockito in test scope and uses Flink’s Byte Buddy version for the tests. Inspection of the shaded JAR verified that the test libraries stayed out of the package.

A local MiniCluster used the rebuilt runtime and backend JARs with the pinned RocksDB JNI library. The collector and native scaling service loaded from the newly shaded operator JAR, and the runner ran in its own JVM. The collector recorded a live task-time window. The guarded rescale restored the state and produced all 200 expected IDs and keyed counts. All three recorded recommendations replayed exactly with the packaged runner. Checksums and class-loading evidence identified the actual artifacts. The shared-lab throughput and latency criteria remain open.

### Full Flink distribution

The 104-module Maven reactor compiled and shaded the Flink distribution from `14ce6b95` on Linux amd64 with Java 17. The reactor used `-DskipTests` to retain the test JARs required by downstream modules. After the initial `bin` assembly, temporary storage ran out during the optional library copy. A second Maven invocation ran the repository's `opt.xml` and `plugins.xml` descriptors against the compiled module JARs. Checksums covered all 125 assembled files, and the optional libraries and plugins matched their build inputs.

The packaged CLI submitted the bundled WordCount to its own JobManager and TaskManager and observed the job finish. A separate local checkpoint rescale loaded the Flink runtime and RocksDB classes from the assembled distribution JAR. The bundled native library matched the pinned JNI input byte for byte. The separate runner evaluated a live task-time window, the guarded rescale recovered all 200 expected IDs and keyed counts, and all three recorded decisions replayed exactly. The Q8 comparison below verifies startup of the same runtime JAR under Java 11 in the lab.

## Full local RocksDB JNI check

The vendored `rocksdbjava` target built a fresh x86-64 JNI JAR with Snappy on Ubuntu 24.04 using GCC 13. The local build supplied the standard C++ headers required by this compiler and used the Makefile's warning override. The original estimator was also linked using the same compiler, compression libraries and common object files. Both JARs passed all four RocksDB `LRUCacheTest` cases. The standalone SHARDS library passed its C++11 test independently.

The JNI trace made 512 deterministic reads across two databases sharing one cache. Every run preserved the sample and cold-miss counts, kept the databases' cold references distinct, and passed through Flink's SHARDS histogram JSON and MRC conversion. Seven of twelve paired traces matched after excluding estimator timing. The other five shifted repeat accesses by one bin when the allocator-reported tracked bytes differed. Both binaries exhibited all four tracked-byte trajectories. Within each trajectory, every histogram bin and every remaining header field matched. The fresh feature JNI JAR also passed the packaged local Kafka rescale, committed-output and replay checks.

The local ELF requires `GLIBC_2.38`. The lab image build targets Ubuntu 22.04 and must use the repository's matching JNI build path.

### Native profiler overhead on resident RocksDB reads

The original and extracted JNI builds above read an identical workload pinned to one local CPU. Each trial used a fresh JVM and warmed a 64 MiB RocksDB block cache before timing 1,310,720 point reads. The SST contained 32,768 keys with 1,000-byte values. The driver checked every returned value and observed one data-block hit per read with zero misses. The two series used separate RAM-backed fixtures. Each series rotated the six combinations of binary and sampling rate through all six run positions, giving 12 pairs per sampling rate across both series.

| SHARDS sampling | Original CPU ns/read | Extracted CPU ns/read | Paired CPU change, median and range | Original / extracted anonymous RSS growth |
| --- | ---: | ---: | ---: | ---: |
| Off | 1,695 | 1,741 | +2.50% (−2.06% to +6.61%) | 678 / 686 KiB |
| 2% | 1,797 | 1,812 | +2.21% (−0.73% to +5.15%) | 816 / 810 KiB |
| 100% | 2,135 | 2,199 | +3.57% (−2.69% to +16.41%) | 6,400 / 6,392 KiB |

The read-thread CPU time and process-wide anonymous RSS deltas are medians. Both binaries selected 25,200 of the 1,310,720 timed accesses in each 2% trial. The paired CPU difference at 2% was within the range observed with profiling off. Full sampling was more variable. The experiment isolates resident point reads and provides no estimate of end-to-end Flink performance or allocator-only memory use.

To reproduce the six-block comparison on Linux, use the matched JNI JARs with their native dependencies available on `LD_LIBRARY_PATH`. Pass an allowed CPU ID and a writable scratch directory with capacity for the fixture. The launcher records JAR hashes, the SST checksum, the JDK version and per-trial measurements.

```bash
TMPDIR=/dev/shm scripts/a4s/measure-native-shards.sh \
  "$ORIGINAL_JNI_JAR" "$EXTRACTED_JNI_JAR" "$OUTPUT_DIR" "$CPU_ID"
```

## Local control-plane overhead

A Linux x86-64 VM ran a Java 17 loopback client against the standalone JAR using the generic Q8-shaped JSON example. The request and response bodies were 1,583 and 844 bytes. The client warmed each path, then measured three repetitions with 5,000 embedded evaluations and 100 HTTP round trips per repetition. HTTP samples include serialization, process scheduling and loopback. All plans matched the embedded output.

| Repetition | Embedded p50 / p95 / p99 (ms) | HTTP p50 / p95 / p99 (ms) |
| --- | ---: | ---: |
| 1 | 0.00970 / 0.01645 / 0.02606 | 42.93476 / 45.89506 / 49.47713 |
| 2 | 0.00113 / 0.00240 / 0.00627 | 41.97409 / 43.15703 / 43.94976 |
| 3 | 0.00060 / 0.00067 / 0.00085 | 41.95502 / 42.04983 / 43.10738 |

Runner RSS increased from 73,884 to 93,572 KiB during the loopback test and the process consumed approximately one CPU second at the precision of the sampled process counter. These are control-plane observations on one VM. The matched throughput regression target, latency and tail latency, recovery time, backend resource consumption, and worker capacity must be evaluated in the lab.

The runner now includes a `route` field beside `durationMicros` in request logs, so `/v1/evaluate` service timings can be counted separately from readiness checks. The value covers server-side handling and response writing. The local HTTP round-trip test above also includes client and transport time.

A loopback replay also timed requests captured during the live Q8 and Q20 comparisons. For each query the harness alternated the embedded and remote payloads, warmed both paths, and verified all 200 measured responses against the recorded decisions. The Q20 requests produced the recommendations applied during the checkpoint-preserving memory updates and were 24,581 and 29,597 bytes. Times below are in milliseconds. The HTTP measurement includes constructing the request, sending it, and receiving the complete response body. Request encoding and response decoding were timed separately.

| Snapshot | Requests | Encode p95 | HTTP p50 / p95 / p99 | Decode p95 | Server handling p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q8 embedded | 100 | 0.120 | 41.724 / 42.488 / 42.650 | 0.147 | 0.779 |
| Q8 remote | 100 | 0.107 | 41.734 / 42.514 / 42.684 | 0.155 | 0.773 |
| Q20 embedded | 100 | 0.228 | 41.640 / 42.535 / 42.576 | 0.145 | 1.107 |
| Q20 remote | 100 | 0.286 | 41.642 / 42.510 / 42.643 | 0.155 | 1.193 |

The production `DecisionClient` replayed all 69 fixed-allocation Q8 recordings over loopback. After one warmup pass, three measured passes verified 207 responses byte for byte. Encoded request bodies were 43,628–81,525 bytes, and response bodies were 978–1,780 bytes. The client includes the same bounded response subscriber and request timeout used by the Flink adapter.

| Phase | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| Request encoding | 0.296 | 0.439 | 0.471 |
| HTTP exchange | 41.928 | 42.774 | 43.395 |
| Response decoding | 0.052 | 0.093 | 0.123 |
| Total client phases | 42.265 | 43.185 | 44.262 |
| Server handling within the exchange | 1.067 | 1.431 | 2.164 |

With the built runner listening locally, reproduce the timing CSV with Java 11 or later. The measurement script verifies the recorded recommendation before publishing each row.

```bash
java --class-path "$A4S_RUNNER_JAR" scripts/a4s/MeasureA4sClient.java \
  "$RUNNER_ORIGIN" "$RECORDED_EVALUATIONS" 3 > client-timing.csv
```

### Histogram-backed evaluation

For requests that carry a histogram, the core now builds the miss-rate curve once per operator and reuses it for candidate sizing and forecasting. The fixed-latency implementation shares that prepared curve. Models implementing the original forecast method continue to receive the original operator metrics and candidate allocation.

In a local Java 17 fixture with an 8,192-bin histogram and a cache budget that exhausted 1,000 candidates, median embedded evaluation time over nine measured repetitions fell from 71.2 ms to 1.0 ms. The before and after builds produced byte-identical plans for both the exhausted search and a feasible histogram-backed recommendation. The full standalone reactor passed 17 Maven tests. The candidate runner also replayed 209 decisions from the archived Q8, paced Q20, and saturated Q20 runs exactly. The measured speedup covers the raw-histogram path. The Flink adapter normally supplies an already-computed `scaledMrc`.

## Live experiment

The first matched comparison uses the existing isolated Q8 DataStream harness. It keeps the workload JAR, runtime image, initial profiles, input schedule, checkpointing, and physical worker fixed across original, embedded and remote modes. The original and extracted operators are built independently from the recorded source revisions against the same installed Flink snapshot artifacts. Scoped layer publication is used on c165 because the lab account cannot access the Docker daemon.

The standalone runner passed an isolated Kubernetes Service smoke on c182. The pinned Java 11 runner returned the sample `RESCALE` plan for six subtasks, its live JAR digest matched the build receipt, and scoped cleanup removed the namespace. The Dockerfile also passed the GitHub Actions container smoke.

The initial Q8 stage pinned runtime `3015abd8e505569ac914c55e9e795b89d5053d6c` and the same JNI and workload artifacts for every mode. The original arm `live-33543ca8` stopped at the startup guard because its evidence receipt omitted the native binary checksum and its configuration filter missed the slot-limit key. Those harness inputs were corrected. The retry `live-b0feb46b` was stopped during setup at the user's request, before the input release. Both scopes were removed and the shared worker lock was released. Both attempts predate the optional attempt-scoped backlog transport.

After lab access resumed, freshly built operators ran on `c167` with a common telemetry overlay and the same pinned workload and JNI. The original arm `live-43e4133b` completed its 600-second window. Attempt-bound counters measured 21.57k join-input records per second over 594.85 seconds of valid observations. The join stayed at parallelism 1. The operator repeatedly deferred its proposed rescale because it could not preserve an applied source slot profile. In embedded arm `live-367af2a6`, the live Justin endpoint confirmed zero of three applied profiles. Setup was stopped before input release, and the remote arm never launched. Both scopes cleaned up and released the worker lock. This run provides a baseline observation for the shared-slot configuration.

The exact timed Q8 workload reproduced zero confirmed profiles with its default shared group in a local MiniCluster. The optional `--a4s-exclusive-slots true` gives the two sources and the join distinct slot-sharing groups. The timed workload then confirmed all three applied profiles with the same vertex IDs and input settings. A matched run must use the same slot-sharing mode in each arm. Check the response from Justin GET with `include-applied-profile=true` using [check-q8-applied-profiles.sh](../scripts/a4s/check-q8-applied-profiles.sh) before releasing input. The gate rejected the live missing-profile response and accepted the local exclusive-slot response.

### Exclusive-slot Q8 comparison on c167

One matched repetition completed with 600 seconds per arm and a configured input cap of 80,000 records/s. Each arm started in isolation with five physical allocations and three confirmed vertex profiles of one CPU core and 192 MiB managed memory per slot. All scoped cleanup receipts passed and the worker lock was released. The recorded measurements are attempt-bound join input counter deltas. Coverage excludes gaps and execution changes.

| Mode | Run | Join input records/s | Valid seconds | Final 120 s join input records/s | Final join parallelism |
| --- | --- | ---: | ---: | ---: | ---: |
| Original | `live-8cf40b49` | 40,970 | 449.6 | 78,030 | 8 |
| Embedded | `live-35ffd3b2` | 41,636 | 524.7 | 33,273 | 8 |
| Remote | `live-206595dd` | 34,773 | 524.5 | 24,146 | 6 |

Remote scored 16.5% below embedded over the full window. The arms selected different parallelism from different measured inputs. Both recorded recommendations matched the standalone HTTP runner on identical local replay inputs. The embedded join saw a 31-point curve and demand of 4,343.375 records/s. The remote join saw an 18-point curve and demand of 3,848.157 records/s. Substituting the remote curve into the embedded snapshot selected six subtasks. Substituting just the remote demand retained eight. The recorded requests were 5,660 and 4,933 bytes, with 1,776-byte HTTP responses when replayed locally. This single repetition leaves the transport-only throughput regression target and end-to-end record latency unverified.

The first complete target rosters were observed about 71, 69 and 69 seconds after the respective parallelism-changing reports. The corresponding captures already contained processed join inputs. The configured stateless upgrade and deployment memory tuning yielded redeployments, with zero checkpoint restores reported by each final job. Their throughput includes catch-up on replayed input. Physical parallelism was confirmed, and final per-vertex memory recommendations remain unverified. The standalone process grew from 57,916 to 72,640 KiB RSS and consumed 293 CPU ticks between recorded process samples. The results archive retains the raw samples, resource and runner receipts, snapshots, replay results, and Figure 10 inputs. The deployed runner logged health and evaluation durations together, so those logs leave the separate live evaluation latency unverified.

The operator now keeps deployment-wide memory tuning out of the Justin A4S loop and leaves an unsuccessful in-place scale pending when the configured fallback would be stateless. The packaged operator passed 44 focused tests covering A4S execution, ignored memory tuning, blocked stateless fallback, and successful in-place scaling. The Q20 comparison below uses this package.

### Capture DataStream Q8 record latency

Add `--q8-latency-sample-every 1024` to the Query 8 arguments to sample auction IDs by a deterministic hash. Zero leaves the probe disabled. Start each comparison arm with the same benchmark JAR and stride, and keep those settings across the arm's checkpoints and restores. The probe requires operator chaining so its wall-clock timestamp is assigned in the Auction source chain. The query continues to use its generated event timestamps for watermarking and its original join conditions. Each matched result for a sampled auction contributes one observation when it reaches the sink. The opt-in path carries a timestamp field on the existing stream and publishes one aggregate gauge per sink task.

Read the vertex containing `Q8 latency sink`. If the sink is chained with the join, use the ID of that combined vertex. Capture one histogram per task before and after the interval, using the REST endpoint of the isolated Flink job.

```bash
python3 scripts/a4s/q8-latency.py capture "$FLINK_REST" "$JOB_ID" "$SINK_VERTEX_ID" > before.json
# Capture again at the interval's end, while the same attempts are still running.
python3 scripts/a4s/q8-latency.py capture "$FLINK_REST" "$JOB_ID" "$SINK_VERTEX_ID" > after.json
python3 scripts/a4s/q8-latency.py summarize before.json after.json
```

The tool rejects partial coverage, a changed roster, and reset counters. It merges bucket deltas before taking p50, p95 and p99. A percentile is an inclusive 10 ms bin, with a lower bound only if it lands in the overflow bucket at 120 seconds. Use a common clock for the source and sink, such as a single lab worker. Negative elapsed times are reported as clock errors. The capture interval is subject to Flink's metric fetch interval, and the measured latency includes event-time window and watermark waiting.

The updated Q8 probe records when each TaskManager samples its histogram. Use `capture-series` to retain those timestamps and skip cached reads. For mode comparisons, use intervals whose sample bounds for every sink task fall inside the measured window. Earlier Q8 captures still work with `summarize`.

```bash
python3 scripts/a4s/q8-latency.py capture-series "$FLINK_REST" "$JOB_ID" "$SINK_VERTEX_ID" \
  --metric q8AuctionLatencyV1 --duration-sec 75 --interval-sec 5 > q8-latency-series.jsonl
```

A local Q8 smoke with in-memory state produced 11 fresh two-task intervals containing 5,200 sampled results, with zero clock errors or overflows. The RocksDB attempt produced two valid intervals before exhausting local scratch space. Three archived lab scorecards produced identical output before and after the collector change.

Two local Flink 1.18 MiniCluster startup smokes ran Q8 with two joined-result subtasks, the default 10-second event-time window and a sample stride of 16. With the probe disabled, the three original vertex IDs and parallelisms matched the archived workload. With it enabled, the original three chains still fit in the exclusive-slot layout. Both sink gauges contributed to each capture and both reported zero negative or overflow samples.

| Startup capture | Sampled results | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| First | 15,450 | 4,460–4,469 | 5,400–5,409 | 5,400–5,409 |
| Second | 45,550 | 4,480–4,489 | 8,410–8,419 | 8,410–8,419 |

These results verify the collection and show variability during startup.

### Fixed-allocation Q8 crossover on c167

Run `live-0e13269a` used one 600-second timed DataStream Q8 deployment at an 80,000-record/s configured input cap. It used operator `86d298c1` and the workload probe from `a239ef59` overlaid on the archived Q8 replay job. The two source vertices each ran at parallelism 2, and the join ran at 1. Admission verified five occupied slots on two four-core TaskManagers, a 192 MiB applied managed reservation per slot, and a 1,536 MiB total managed memory pool. The latency histogram was empty before timed input release.

The same operator and runner stayed up throughout the run. The operator evaluated snapshots with `job.autoscaler.scaling.enabled=false`. After 120 seconds of warmup, the run alternated the runner URL and snapshot filename in the job's operator settings. Six periods started 75 seconds apart in embedded, remote, remote, embedded, embedded, remote order. Each period allowed 20 seconds for reconciliation and captured counters for about 50 seconds. Every capture verified the same running attempts and applied profiles. There were 47 evaluations in the measured intervals, with all 23 remote calls returning HTTP 200 and zero evaluation calls to the runner in embedded intervals. All 69 archived evaluations, including the guards, replayed exactly.

| Period | Mode | Source output records/s | Join input records/s | Joined results sampled | p50 / p95 / p99 lower bin edge (ms) | Cache miss fraction |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Embedded | 21,384 | 21,561 | 50 | 780 / 780 / 780 | 34.5% |
| 2 | Remote | 22,319 | 22,319 | 4,950 | 760 / 930 / 930 | 33.3% |
| 3 | Remote | 21,048 | 21,845 | 2,600 | 560 / 580 / 850 | 30.5% |
| 4 | Embedded | 20,791 | 21,194 | 2,550 | 630 / 630 / 1,230 | 38.4% |
| 5 | Embedded | 22,394 | 22,394 | 2,600 | 700 / 770 / 1,220 | 30.7% |
| 6 | Remote | 16,454 | 16,950 | 200 | 670 / 1,180 / 1,180 | 44.3% |

Latency bins are 10 ms wide and measure from the source chain's timestamp to the joined-result sink. The 50 samples in period 1 all landed in the same bin. All periods had zero clock-error and overflow samples. The join's measured block-cache capacity was 160 MiB throughout. Repeated column-family capacity readings referred to the same slot cache. The cache miss fractions use the database-level hit and miss counter deltas. Replay backlog grew by 2.88–3.15 million records per measured interval.

For the three adjacent pairs, remote join-input throughput changed by +3.5%, +3.1%, and −24.3% relative to embedded. In the last remote period, estimated live window state grew from about 760 to 909 MiB, with a 44.3% miss fraction compared with 30.7% in the preceding embedded period. Remote server handling in that late period took 4.0–5.0 ms per request, within the earlier remote timing range. The run has one deployment, a growing backlog, and uneven latency sample counts. The observed differences leave the 5% transport-only steady-state throughput criterion and steady-state latency regression unverified.

The separate ten-second REST trace places the slowest measured interval at 550–560 seconds. It processed 4,873 join-input records/s and ended with 630 MiB of estimated pending compaction data, the highest observed value for `window-contents`. RocksDB reported compaction pending at that boundary. The next interval reached 24,564 records/s with the compaction estimate back at zero and remote mode still active. The runner handled one evaluation in the slow interval and two in the recovery interval. The join reported 1,000 ms/s busy at both boundaries. Compaction is a plausible contributor. The end-of-interval gauges leave causation and time spent waiting for storage unverified.

Across all 23 in-window remote evaluations, server handling took 5.8 ms p50, 13.2 ms p95, and 15.0 ms p99. Runner RSS grew from 58,260 to 77,704 KiB and process CPU increased by 451 ticks across the scoped samples. Flink completed 45 checkpoints and reported zero restores. The owned namespace was removed and the worker lock was released. The latest branch also passed 47 focused `ScalingExecutorTest` cases, including a successful remote recommendation blocked from actuation by observe-only configuration.

Recompute the period summaries from the captured run directory, which contains `execution-spec.json` and `scout/crossover`. When `scout/operational-snapshots.jsonl` is available, the scorer also emits same-attempt join rates, end-of-interval compaction gauges, and runner call counts for the ten-second observations. [Scorer](../scripts/a4s/score-q8-crossover.py)

```bash
python3 scripts/a4s/score-q8-crossover.py "$Q8_RUN_BUNDLE"
```

### Repeat with reversed mode order

A second 600-second deployment, `live-6ea76950`, reversed every mode assignment on c167. Both deployments used the same pinned workload, operator, native library and runner, with scaling disabled. The comparison verifies the workload settings, starting profiles and unchanged five-task roster within each deployment. Each deployment is one experimental unit with six observation periods. The order below refers to measured periods after warmup.

| Deployment | Observation order | Embedded join input records/s | Remote join input records/s | Remote change | Remote change by adjacent pair |
| --- | --- | ---: | ---: | ---: | --- |
| `live-0e13269a` | E R R E E R | 21,716 | 20,372 | −6.2% | +3.5%, +3.1%, −24.3% |
| `live-6ea76950` | R E E R R E | 20,749 | 20,606 | −0.7% | −0.1%, −1.5%, −0.5% |

The deployment rates weight the measured periods by duration and include the slow interval. The repeat's late embedded period held 20,595 join-input records/s. Its largest observed pending-compaction estimate was 600 MiB at 470 seconds while the join processed 23,739 records/s over the preceding ten seconds. The first deployment reached a low of 4,873 records/s over ten seconds. The repeat's ten-second minimum was 10,037 records/s. The two outcomes leave the 5% throughput target open.

| Repeat period | Mode | Source output records/s | Join input records/s | Join output records/s | Latency samples | p50 / p95 / p99 lower bin edge (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | Remote | 19,002 | 20,710 | 45,908 | 50 | 980 / 990 / 990 |
| 2 | Embedded | 19,503 | 20,720 | 48,948 | 5,000 | 630 / 1,200 / 1,200 |
| 3 | Embedded | 19,501 | 20,931 | 50,370 | 2,650 | 1,090 / 1,140 / 1,250 |
| 4 | Remote | 19,190 | 20,615 | 49,622 | 2,500 | 780 / 800 / 1,170 |
| 5 | Remote | 18,909 | 20,493 | 50,049 | 4,900 | 1,160 / 1,260 / 1,320 |
| 6 | Embedded | 19,788 | 20,595 | 49,796 | 5,000 | 760 / 860 / 860 |

The latency intervals are 10 ms wide. The direction of the p95 and p99 differences varied by pair, and the first remote period captured only 50 joined results. Every period reported zero clock errors and zero overflow samples. The repeat verified a 160 MiB join block cache, 47 completed checkpoints, and zero restores. All 24 measured remote requests returned HTTP 200. Runner handling time was 4.9 ms p50, 11.0 ms p95 and 21.6 ms p99. All 72 recorded recommendations replayed exactly, including guard intervals. Scoped teardown removed the run namespace and released the worker lock.

The repeat sampled cumulative CPU and working-set memory from the worker's Kubernetes stats endpoint every five seconds. Mean CPU uses stable container counters whose timestamps fall inside each measured period. Per-container counter spans ranged from 15 to 42 seconds. Flink used 1.89–1.94 cores in embedded periods and 2.05–2.08 in remote periods across the JobManager and both TaskManagers. The operator used 0.17–0.19 and 0.12–0.21 cores respectively. The runner stayed resident across all periods, using 1.3–2.1 millicores in embedded periods and 3.5–15.5 millicores in remote periods. Its peak observed working set was 57 MiB. These are measurements from the reverse-order deployment.

The scorer verifies both complete archives and reports each deployment separately. Include `scout/pod-resources.jsonl` when resource samples were captured.

```bash
python3 scripts/a4s/score-q8-crossover.py "$Q8_RUN_BUNDLE" \
  --counterbalanced-bundle "$Q8_REVERSED_RUN_BUNDLE"
```

### Q8 with the rebuilt Flink distribution

Run `live-9509f7eb` used the Flink distribution, operator, and standalone runner built from `14ce6b95`. Three digest-pinned layers placed the rebuilt JARs on the existing timed Q8 images. Startup verified the in-container JAR checksums and Java 11 for all three roles. One c167 workload deployment held two four-core TaskManagers, the original five occupied slots, a 192 MiB applied managed reservation for each vertex's tasks, and 1,536 MiB of total TaskManager managed capacity. The existing launcher scheduled its isolated virtual API pod on c182. The 80,000-record/s configured cap and six observation periods followed the earlier fixed-allocation protocol. Actuation stayed disabled.

| Period | Mode | Source output records/s | Join input records/s | Join output records/s | Latency samples | p50 / p95 / p99 lower bin edge (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | Remote | 20,943 | 20,942 | 47,550 | 50 | 1,210 / 1,230 / 1,230 |
| 2 | Embedded | 20,145 | 21,114 | 50,030 | 5,000 | 1,080 / 1,170 / 1,230 |
| 3 | Embedded | 17,785 | 18,409 | 42,987 | 2,650 | 740 / 770 / 1,130 |
| 4 | Remote | 18,804 | 20,127 | 49,628 | 2,500 | 1,190 / 1,280 / 1,280 |
| 5 | Remote | 18,363 | 19,269 | 47,432 | 4,900 | 720 / 780 / 1,660 |
| 6 | Embedded | 19,422 | 20,023 | 49,504 | 5,000 | 830 / 1,250 / 1,260 |

Duration-weighted join input was 20,113 records/s remote and 19,848 embedded, a 1.3% difference. The adjacent pairs measured −0.8%, +9.3%, and −3.8%. Latency used 10 ms bins and reported zero invalid clock and overflow samples. The first remote period had 50 samples, and paired p95 and p99 differences varied. Source backlog grew by 2.93–3.08 million records per measured period. This is one deployment of the rebuilt release. The earlier deployments used different binaries, and the steady-state throughput and latency criteria remain open.

All 72 recorded requests included the 20-core autoscaler quota, the join's measured 160 MiB block-cache capacity and occupancy, hit and miss counters, state size, and task-time fractions. Both source backlog counts were present in every request. Reported join state size ranged from 196 to 749 MiB, and per-period cache miss fractions ranged from 26.5% to 42.5%. All 25 measured remote evaluations returned HTTP 200, with server handling times of 6.0 ms p50, 20.7 ms p95, and 20.7 ms p99. Re-encoded requests ranged from 39,309 to 75,999 bytes. Mean runner CPU ranged from 3.4 to 26.7 millicores in remote periods, and the peak observed working set was 52.2 MiB.

The runner replayed all 72 recordings exactly, including seven recommendations to rescale. Flink completed 45 checkpoints with zero restores and retained one execution roster and its applied profiles. The run-owned namespace was removed, the shared-resource comparison passed, and the worker lock was available after cleanup. The supplied Figure 10 renderer used the 121 five-second samples from release through the 600-second cutoff.

### Prepare the Q20 follow-up

The [Q20 renderer](../scripts/a4s/prepare-q20-comparison.py) uses the existing [checkpoint experiment template](../experiments/1729-checkpoint-aware-rescaling/instruction.md). It creates three repetitions, rotating original, embedded, and remote through each position. It pins the workload image, replaces the template's `tier` selectors with the requested hostnames, and places the operator and remote runner on the selected JobManager host. Both Flink roles can use one idle worker when capacity permits. Each arm gets a separate namespace and checkpoint path. The renderer keeps the shared 600 MiB A4S limit, periodic checkpoints with a one-minute default, and last-state upgrades. It disables deployment-wide memory tuning so that additional pod memory edits do not force redeployment during a Justin scale, and disables the template's separate checkpoint-gated producer pause. Python 3.9 or later and PyYAML are required.

```bash
scripts/a4s/prepare-q20-comparison.py "$OUTPUT_DIR" --prefix "$SERIES_ID" \
  --workload-image "$Q20_IMAGE" --original-operator-image "$ORIGINAL_OPERATOR_IMAGE" \
  --operator-image "$REFACTORED_OPERATOR_IMAGE" --runner-image "$A4S_IMAGE" \
  --bootstrap-servers "$ISOLATED_KAFKA" --jobmanager-node "$JM_NODE" --taskmanager-node "$TM_NODE"
```

Each image argument takes a digest-pinned reference. Before live runs, verify the built artifacts against their receipts and verify both node selectors against the isolated lab scope. Use `run-order.json` and the existing Q20 producer setup with the same input generator build, seed, starting offsets, rate schedule, and observation window for each arm. Leave the input topics intact during each arm's rescaling. Collect equally long observations for both rescale and hold outcomes.

The offline preflight rendered and checked all nine job configurations. The local Helm render confirmed each operator's watched namespace, digest and recording settings. It used historical image receipts as fixture inputs. Use a common runtime built with support for the renderer's `pipeline.exclusive-vertex-slots` setting. The exact Q20 SQL workload produced one shared slot group and returned zero applied profiles with the setting disabled. With it enabled, all three vertices returned confirmed profiles in a local Flink run against an empty Kafka broker. The setting leaves default submissions alone and rejects a shared group with an explicit resource budget before modifying the graph.

### Optional Q20 record latency

The timed SQL variant samples the elapsed time from the bid record's Kafka append timestamp to the joined row reaching the discard sink. Set `message.timestamp.type=LogAppendTime` on the isolated bid topic before producing records and verify that the broker and TaskManager clocks agree. The original SQL and producer remain available in the workload JAR. The timed query carries one extra timestamp through the join. Start a fresh checkpoint lineage and use the same probe JAR and sampling rate in every measured arm.

Build the workload overlay with Java 11 or later. Pass `--latency-probe` to the Q20 renderer and pin all arms to the workload image whose `nexmark-flink-0.3-SNAPSHOT.jar` contains this overlay. The renderer selects `q20_latency_unique` for every arm.

```bash
python3 scripts/a4s/build-q20-latency.py "$Q20_BASE_JAR" "$Q20_TIMED_JAR" \
  --base-sha256 "$Q20_BASE_SHA256" --flink-classpath "$FLINK_HOME/lib/*"
```

Capture the join vertex at both ends of each observation window. The script verifies the running subtask roster and rejects a window that spans an attempt change. After recovery, start a new window with the new roster. The histogram samples one out of 1,024 joined rows by default. It uses 10 ms bins below two seconds, 100 ms bins up to one minute, and one-second bins up to the 900-second overflow bucket.

```bash
python3 scripts/a4s/q8-latency.py capture "$FLINK_REST" "$JOB_ID" "$JOIN_VERTEX" \
  --metric q20KafkaLatencyV2 > before.json
# Capture after.json with the same command at the end of the observation window.
python3 scripts/a4s/q8-latency.py summarize before.json after.json
```

A local Kafka and Flink test verified that the timed query kept all three original Q20 vertex IDs. The broker replaced deliberately old producer timestamps with its append time. The query recorded exactly seven expected joined rows in the first attempt. After replacing the TaskManager, Flink restored one RocksDB checkpoint and joined two new bids against the restored auction without replaying the previous outputs into the new window. Both windows had zero invalid or overflow samples. The collector rejected the window spanning the old and new attempts.

The lab pilot `live-414743c5` used the rebuilt runtime on Java 11 and the v1 latency probe with one-in-64 sampling. The virtual API, broker, producer, Flink, operator, and runner were scoped to c167. Startup verified the exact image and JAR pins, an empty latency gauge, and `LogAppendTime` on the bid topic. The job completed three fixed-allocation observation windows in embedded, remote, remote order. Their join input rates were 9,000, 8,925, and 9,123 records/s. Their p95 Kafka-to-join latencies were 123.18, 178.32, and 236.52 seconds, with 3,850 valid samples and zero timestamp or overflow errors. All 32 recorded decisions replayed exactly. The last checkpoint observation showed 12 completed checkpoints and zero restores. The partial run leaves the performance comparison open.

The fourth window stopped when Flink returned an empty gauge value. The last valid v1 gauge was 63,657 bytes. Flink's actual metric serializer omitted the same gauge after a test added 400 occupied bins and grew it to 67,657 bytes. A fully populated v2 gauge with maximum positive counts serialized and deserialized exactly at 42,764 bytes. The collector still reads archived v1 captures. The run-owned namespace was removed and the shared worker lock was available after teardown.

### Exclusive-slot Kafka Q20 comparison on c167

The benchmark used the exact `q20_unique` Kafka SQL query, one isolated KRaft broker per arm, 24 partitions per topic, and a common producer capped at 40,000 mixed events/s and 24 million events starting at ID 1. The 600-second windows used one worker and a 16-slot cap. The two source vertices began at parallelism 6 and 3, and the join began at 1. Each applied slot profile reserved one CPU core and 192 MiB managed memory. Flink ran one-minute checkpoints in exactly-once mode and used `last-state` upgrades with deployment memory tuning disabled. The common runtime JAR had SHA-256 `eb1d07e26430eb5bc6ec3acf29724015bae943f0899b23859ff782b01c8bdfc8`. The operators were built from `a285fcf4453d38feff9c80b53bf57c8207d30930` and `86d298c1843ae2e75d19da50dc4a0277dc65d930`.

The original arm `live-08d3078f` completed the REST observation window. Its first in-place Justin update restored checkpoint 6 and reduced the applied join reservation from 192 MiB to 36 MiB at parallelism 1. Its first post-restore input was observed 4.6 seconds after Flink recorded the restore. The last complete Kafka sample before the restore and the first comparable sample after it showed rewinds on all 24 bid partitions. These are observed consumer positions during replay and recovery. The query writes to a blackhole sink. The independent deterministic Kafka test above verifies committed output correctness.

The original operator later requested 18 join subtasks, which would need 27 exclusive slots with the nine source subtasks. Flink reached the 16-slot limit and remained `CREATED` through cutoff. The measured join input rate was 4,310 records/s over 333.0 seconds of valid running intervals, with no valid running interval in the final 120 seconds. The observer recorded all 600 samples. Its normal exit near cutoff raced the supervisor's process check, so final logs and broker offsets were missed. The scoped cleanup passed. The remaining arms use the same observer and inputs with a corrected wait that checks the observer's exit status and cutoff receipt.

The embedded arm `live-ad59f066` reduced the applied join reservation to 37 MiB at parallelism 1 via an in-place Justin update. Flink restored checkpoint 6 and the first recovered join input was observed 5.5 seconds after the restore. The final Flink snapshot reported 13 completed checkpoints and one restore. The pinned operator's final CPU quota check rejected two later recommendations for parallelism 24, which would have required 36 CPU cores after rounding to whole TaskManagers. The remaining 52 recommendations reported `INFEASIBLE_CONFIGURATION` because the measured curves gave no supported configuration for the join's target.

The remote arm `live-e714b3cb` applied a 39 MiB reservation at parallelism 1 through the same in-place path. Flink restored checkpoint 6 and the first recovered join input was observed 3.8 seconds later. The final snapshot showed 12 completed checkpoints, one in progress and one restore. The quota check rejected 55 recommendations targeting parallelisms 9, 18, 20 or 24 alongside the nine source slots. In both refactored arms the job stayed `RUNNING` at join parallelism 1 through cutoff. The comparable Kafka samples around each restore showed rewinds on all 24 bid partitions. The final committed bid offsets left broker lag of 20.91 million and 21.01 million records in the embedded and remote arms, respectively.

The applied embedded and remote recommendations recorded join-input targets of 2,621 and 3,239 records/s. The first complete same-attempt counter window of at least ten seconds that reached the respective target ended 41.5 seconds after restore in embedded mode and 41.8 seconds after restore in remote mode. The windows measured 3,903 and 3,366 records/s. Both windows also met the higher of the two targets. The original arm's initiating target was unavailable in the archive. This measures first observed recovery in one deployment per mode. The sustained throughput comparison uses the full eligible window.

| Mode | Source output records/s | Join input records/s | Valid join-input seconds | Join output records/s | Final 120 s join input records/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original | 4,347 | 4,310 | 333.0 | 812 | Unavailable |
| Embedded | 2,821 | 2,805 | 593.0 | 516 | 33 |
| Remote | 2,646 | 2,629 | 594.0 | 479 | 116 |

Throughput uses eligible intervals within one execution roster. The join-output rate uses separately sampled operator counters. Remote join input was 6.3% below embedded over those eligible intervals. The elapsed windows include warmup, a restore, different managed reservations and substantial backlog. This single repetition leaves the provisional 5% steady-state regression target and end-to-end record latency unverified. All run-owned scopes were removed and the worker lock was released.

While the job was `RUNNING`, each arm reported 12 registered worker cores and 2,304 MiB total TaskManager managed memory. The original arm later occupied 16 slots while waiting for resources. The mean measured JobManager plus TaskManager JVM CPU usage was 1.87, 1.58 and 1.53 cores in original, embedded and remote mode. Median per-task estimated live RocksDB state was 112, 142 and 150 MiB, with median reported block-cache usage of 26.2, 26.0 and 28.7 MiB. Actual cache capacity was unavailable in the capture. The gauge filter missed the underscore-named RocksDB hit and miss counters, so measured cache miss rate is unavailable.

The runner handled 51 successful evaluation requests within the remote measurement window. Route-specific server handling times were 4.4 ms p50, 14.3 ms p95 and 366.3 ms p99. The first request took 687.4 ms and dominates the tail estimate. All 56 scoped evaluation requests returned HTTP 200, including final capture time. The recorded request median was 9,973 bytes and the maximum was 29,597 bytes. Runner RSS rose from 58,236 to 76,612 KiB and process CPU increased by 481 ticks between resource samples.

All 111 embedded and remote recordings replayed byte for byte. Replaying copies with the 16-core budget supplied rejected all 57 oversized candidates and retained the two applied memory-only recommendations. The branch now publishes that constraint when verified applied slots support the accounting. The change passed all 46 focused `ScalingExecutorTest` cases, including a case that omits slot group IDs as the lab REST response did. The benchmark continued on the pinned binaries and the new adapter constraint received local verification.

### Full timed Q20 comparison with the bounded gauge

Run `live-a7afda59` used the rebuilt Flink distribution, operator, and runner from `14ce6b95` and the bounded probe from `2a248155`. The query, broker, and producer used the pilot's input settings with one-in-64 sampling. All run-owned pods stayed on c167. The JobManager ran Java 11, and the TaskManager mapping matched the pinned RocksDB library. Three TaskManagers registered 12 cores and 2,304 MiB of total managed capacity. Across the six windows the job held ten occupied slots, with the same three applied vertex profiles and task roster. Scaling stayed disabled through the 600-second workload.

| Period | Mode | Source output records/s | Join input records/s | Join output records/s | Latency samples | p50 / p95 / p99 lower bin edge (s) |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | Embedded | 9,763 | 9,979 | 1,982 | 1,544 | 102 / 117 / 119 |
| 2 | Remote | 9,165 | 9,356 | 1,566 | 1,220 | 157 / 173 / 176 |
| 3 | Remote | 8,438 | 8,438 | 1,743 | 1,358 | 210 / 230 / 234 |
| 4 | Embedded | 7,620 | 7,773 | 1,382 | 1,078 | 282 / 292 / 294 |
| 5 | Embedded | 6,805 | 6,692 | 947 | 738 | 334 / 359 / 362 |
| 6 | Remote | 6,266 | 6,346 | 1,537 | 1,197 | 400 / 413 / 415 |

Duration-weighted join input was 8,148 records/s embedded and 8,046 remote, a remote change of −1.2%. The adjacent pairs measured −6.3%, +8.5%, and −5.2%. The pooled p95 lower bin edges were 353 seconds embedded and 408 seconds remote. Period p95 rose throughout the run and was higher in the later window of each pair, including the pair with embedded second. The first and last periods had median reported source backlogs of 3.58 million and 14.86 million records. At terminal capture, committed offsets left 16.84 million bids and 1.09 million auctions in Kafka. The growing lag and a single deployment leave steady-state throughput and the effect of evaluation mode on latency open.

The six windows recorded 7,135 valid latency samples without timestamp errors or overflow. The largest gauge was 8,878 bytes. All 64 recorded decisions replayed exactly, and all 23 measured runner calls returned HTTP 200. Runner handling took 6.9 ms p50, 23.9 ms p95, and 25.7 ms p99. Re-encoded requests were 73,919 bytes at the median and 104,452 bytes at the maximum. The runner used 4.1–12.8 millicores in remote periods, and its peak observed working set was 52.8 MiB. Flink averaged 1.79 JVM cores across the full observation window. The median join cache occupancy was approximately 160 MiB and the median sampled cache miss fraction was 3.5%. This capture had no measured block-cache capacity gauge.

Flink completed 16 checkpoints with zero restores. Scoped cleanup passed and returned the worker lock. The Figure 10 rendering retained the missing values in its 601 release-aligned rows. The renderer used finite samples to set axis limits.

### Paced Q20 comparison

Run `live-4ee9a97c` reused the same image pins and bounded probe. The producer started at event ID 1 with a cap of 4,500 mixed records/s and 2.7 million total events. That cap was below the preceding run's late join-input rate of 6,346 records/s. The startup allowance was 390 seconds. Warmup and measurement boundaries kept the same offsets from input release, and the mode order was reversed. The block-cache capacity gauge was enabled for both modes. All run-owned pods stayed on c167. The job held ten occupied slots and the same task roster and applied profiles throughout all six windows.

| Period | Mode | Source output records/s | Join input records/s | Join output records/s | Latency samples | p50 / p95 / p99 lower bin edge (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | Remote | 4,094 | 4,202 | 911 | 710 | 50 / 100 / 240 |
| 2 | Embedded | 4,122 | 4,203 | 842 | 657 | 50 / 120 / 410 |
| 3 | Embedded | 4,146 | 4,144 | 828 | 631 | 50 / 110 / 210 |
| 4 | Remote | 4,118 | 4,205 | 797 | 622 | 50 / 120 / 550 |
| 5 | Remote | 4,182 | 4,182 | 766 | 598 | 50 / 130 / 460 |
| 6 | Embedded | 4,146 | 4,203 | 723 | 563 | 50 / 110 / 510 |

All 49 in-window evaluations carried both source-backlog counters, and each reported a total of zero. The terminal Kafka consumer-group query counted 106,380 bids and 6,937 auctions after the last committed positions. The duration-weighted join input was 4,183 records/s embedded and 4,196 remote, a remote change of +0.3%. That comparison describes processing under the paced input cap. The adjacent pairs measured −0.02%, +1.49%, and −0.51%.

Pooled Kafka-to-join p95 bins were 110–119 ms embedded and 120–129 ms remote. Pooled p99 bins were 290–299 ms embedded and 430–439 ms remote. The adjacent-pair p99 differences were −170, +340, and −50 ms from the lower bin edges. Omitting the second pair reversed the pooled p99 ordering to 410 ms embedded and 240 ms remote. The 3,781 samples had zero timestamp errors and overflows. They describe one deployment, and the sensitivity to period selection leaves the tail-latency effect open.

The highest-p99 remote period had eight runner evaluations, with a maximum server handling time of 8.0 ms. The first remote period had a lower p99 and a higher maximum runner handling time of 24.0 ms. Every measured window overlapped a completed checkpoint. The checkpoint in the highest-p99 window took 1.02 seconds, and a checkpoint in an embedded window took 1.32 seconds. The archived latency histograms were captured at window boundaries, and the archived RocksDB samples omitted compaction gauges. This timing evidence leaves the cause of the tail variation unresolved.

The largest gauge was 588 bytes. The 25 measured remote calls returned HTTP 200 with 7.5 ms p50, 21.4 ms p95, and 24.0 ms p99 runner handling time. Median and maximum re-encoded request sizes were about 62.3 and 104.7 KB. The runner used 4.3–18.4 millicores in remote periods and peaked at 55.4 MiB working set. Flink averaged 2.47 JVM cores across the 600-second observation. The measured join cache held 160 MiB of capacity and approximately 160 MiB of occupancy at the median. The median sampled cache miss fraction was 1.5%, and estimated live state peaked at about 280 MiB.

All 73 decisions replayed exactly. The job completed 14 checkpoints with zero restores. Scaling stayed disabled, and scoped cleanup returned the worker lock.

### Capture Q20 latency intervals

The timestamped V2 gauge records the TaskManager clock when Flink samples its cumulative histogram. The collector emits newline-delimited JSON with a baseline per running roster, fresh intervals with per-subtask sample bounds, and gaps for unavailable data. Repeated REST reads of a cached gauge leave the baseline in place. A fresh gauge with an empty delta records a zero-sample interval. A roster change or a capture gap starts a new baseline.

```bash
python3 scripts/a4s/q8-latency.py capture-series "$FLINK_REST" "$JOB_ID" "$JOIN_VERTEX" \
  --duration-sec 75 --interval-sec 5 > latency-series.jsonl
```

Use the updated probe JAR for interval capture. Earlier probe archives still work with `summarize`. Align checkpoints and runtime logs to each interval's `subtaskIntervalsMs`, and compare intervals contained in the intended evaluation windows. Use the same Flink `metrics.fetcher.update-interval` in both modes. The Flink 1.18 default is 10,000 ms. Faster REST polling can return the same cached gauge, so the TaskManager timestamps determine the available interval resolution.

The local Kafka and RocksDB checkpoint fixture polled once per second with a two-second metric refresh. It recorded 14 fresh intervals and retained exactly seven latency samples before recovery and two afterward, with separate baselines for the two attempts. A fully populated timestamped gauge round-tripped through Flink's serializer at 42,792 bytes. The earlier matched Q8 scorecard reproduced byte for byte.

## Acceptance status and remaining work

| Status | Evidence and scope |
| --- | --- |
| Verified | The independent Maven build, packaged runner, Kubernetes Service smoke, and standalone CI exercise the same v1 evaluation path without Flink. Embedded and HTTP evaluation agree on matched inputs. The live Q8 and Q20 captures reproduce through offline replay. |
| Verified | Local Flink recovered RocksDB state at a different parallelism and preserved the Kafka transactional output oracle. Live Q20 restored checkpoints during memory-only updates. Runner failure, stale recommendations, and incomplete or uncommitted overrides were tested separately. |
| Implemented, partially verified | The rebuilt Q8 release carried the configured exclusive-slot CPU quota, per-source backlog, and join telemetry in every evaluation. The live Kafka workload uses a blackhole sink, so the deterministic committed-output oracle has only been exercised locally. |
| Observed performance differences | The adaptive remote arm measured 16.5% lower eligible join-input throughput in Q8 and 6.3% lower in Q20. Their allocations diverged. Two earlier fixed-allocation Q8 deployments measured remote join input 6.2% and 0.7% lower. The rebuilt release measured 1.3% higher in one Q8 deployment, 1.2% lower in saturated Q20, and 0.3% higher in one paced Q20 deployment. The paced run reported zero backlog in all in-window requests and its pooled p99 was sensitive to the selected windows. The 5% steady-state throughput and latency criteria remain open. The archived embedded and remote Q20 measurements first reached the initiating throughput targets about 42 seconds after checkpoint restore in both deployments. |
| Platform dependencies | The Flink adapter supplies demand from the existing Flink evaluator and consumes the JobManager's aggregate `scaledMrc`. The operator persists one conditional Justin target at a time and confirms the running allocation before recording it in deployment status. Runtime and operator verification use local fixtures. |
| Unsupported in v1 | Physical placement, a calibrated migration cost model, and a verified Flink 2.x or ForSt integration remain outside the current implementation. The optimizer checks a packing when the snapshot supplies worker capacities. Online cost calibration and learned models have extension points and would need their own data and qualification. See the [future adapter guide](a4s-standalone.md#native-instrumentation-and-future-adapters). |
