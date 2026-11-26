# A4S verification and benchmark evidence

The comparison baseline is GitHub main at `a285fcf4453d38feff9c80b53bf57c8207d30930`. The implementation and its lab artifact pins are separate from the unmerged online-calibration branch.

## Verified locally

| Area | Result |
| --- | --- |
| Standalone API, core, runner and replay | Eight Maven tests passed. The packaged JAR served a Q8-shaped snapshot without Flink, returned a state-preserving recommendation for six subtasks, and agreed byte-for-byte with embedded evaluation and replay. The tests cover malformed and oversized requests, unsupported actions, measured vs nominal cache coordinates, timeouts, duplicate requests and runner restart. |
| Flink 1.18 operator | All 36 `ScalingExecutorTest` cases passed. A real loopback HTTP request exercised the remote client and executor together. Embedded and remote modes both stored P2 and a 116 MiB managed reservation for the matched fixture. A stale response or HTTP 503 left scaling history and overrides empty. Other executor cases covered a generation rollover, a concurrently changed applied profile, and an unresolved reconfiguration. The complete operator test suite reported the same eight pre-existing failures on both branches in option registration, dataset expectations, and the ordinary parallelism policy. The new code compiled in the complete operator reactor against the pinned lab runtime. |
| Stateful recovery | Both existing `RocksIncrementalCheckpointRescalingTest` cases passed with Flink's local keyed-operator harness using Java 11 source/target and the matched RocksDB JNI artifact with Snappy. This covers incremental RocksDB state when scaling up and down. An operator-driven remote recommendation with Kafka offsets and a committed output oracle remains a separate end-to-end check. |
| SHARDS | The independent C++11 test passed. The built RocksDB adapter and the original estimator emitted identical histograms and counters on nine 8,000-access traces covering three seeds and three sampling rates. The measured time field was excluded from equality. Full JNI rebuild and live resource overhead are outside that check. |

The original A4S placement and MRC-to-MPC suites passed 22 tests before extraction. On the supported [retry fixture](../sources/a4s/examples/legacy-retry.json), the legacy P1 point needs 700 MiB and exceeds the reservation bound. The original and standalone policies select P2 and reserve 116 MiB per task, including the 16 MiB base. On the [archived-curve fixture](../sources/a4s/examples/q8-archived-curve.json), the original plateau fallback selected P4 and a 17 MiB managed reservation. V1 reports an infeasible model target. This is an intentional validation difference.

The second fixture carries an unchanged 38-point nominal `scaledMrc` from the first available sample in the archived Q8 run `live-b4273691`, with measurement epoch `1795588652004` and source-record SHA-256 `64c177ac2035aa42810f9eb1bbfe7f303ceff17b4e6d2d834fa0fcdd570eb98c`. The target demand, current configuration, timing and model settings in the fixture are test inputs. Replay checks the policy's response to that curve and provides no counterfactual throughput evidence.

## Local control-plane overhead

A Linux x86-64 VM ran a Java 17 loopback client against the standalone JAR using the generic Q8-shaped JSON example. The request and response bodies were 1,583 and 844 bytes. The client warmed each path, then measured three repetitions with 5,000 embedded evaluations and 100 HTTP round trips per repetition. HTTP samples include serialization, process scheduling and loopback. All plans matched the embedded output.

| Repetition | Embedded p50 / p95 / p99 (ms) | HTTP p50 / p95 / p99 (ms) |
| --- | ---: | ---: |
| 1 | 0.00970 / 0.01645 / 0.02606 | 42.93476 / 45.89506 / 49.47713 |
| 2 | 0.00113 / 0.00240 / 0.00627 | 41.97409 / 43.15703 / 43.94976 |
| 3 | 0.00060 / 0.00067 / 0.00085 | 41.95502 / 42.04983 / 43.10738 |

Runner RSS increased from 73,884 to 93,572 KiB during the loopback test and the process consumed approximately one CPU second at the precision of the sampled process counter. These are control-plane observations on one VM. The matched throughput regression target, latency and tail latency, recovery time, backend resource consumption, and worker capacity must be evaluated in the lab.

## Live experiment

The first matched comparison uses the existing isolated Q8 DataStream harness. It keeps the workload JAR, runtime image, initial profiles, input schedule, checkpointing, and physical worker fixed across original, embedded and remote modes. The original and extracted operators are built independently from the recorded source revisions against the same installed Flink snapshot artifacts. Scoped layer publication is used on c165 because the lab account cannot access the Docker daemon.

The standalone runner passed an isolated Kubernetes Service smoke on c182. The pinned Java 11 runner returned the sample `RESCALE` plan for six subtasks, its live JAR digest matched the build receipt, and scoped cleanup removed the namespace. The Dockerfile also passed the GitHub Actions container smoke.

The staged Q8 comparison pinned the same runtime at `3015abd8e505569ac914c55e9e795b89d5053d6c` and the same JNI and workload artifacts for every mode. The original arm `live-33543ca8` stopped at the startup guard because its evidence receipt omitted the native binary checksum and its configuration filter missed the slot-limit key. Those harness inputs were corrected. The retry `live-b0feb46b` was stopped during setup at the user's request, before the input release. Both scopes were removed and the shared worker lock was released. Embedded and remote Q8 arms, the follow-up Q9 or Q20 workload, and the deterministic live Kafka output check are pending. There is no matched live throughput or latency result.

## Remaining work

The fixed model still depends on Flink's existing demand evaluation and on aggregate MRC observations in the first adapter. It takes the pending-reconciliation and live resource inputs from the operator. An atomic Flink execution-generation precondition for Justin writes would close the remaining check-to-use interval. The current greedy optimizer uses attested aggregate bounds and returns no physical placement plan.

There is no verified Flink 2.x or ForSt adapter, online model port, learned model or concrete migration model. A future adapter must prove positive platform capabilities and separately validate stateful recovery, actual cache geometry, and performance calibration.
