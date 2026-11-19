# Q8 comparison with the built-in A4S autoscaler

Run two Kubernetes operators compiled from the pinned main and online revisions. Each operator must exclusively watch the workload assigned to it. Separate Kubernetes contexts or disjoint watched namespaces support concurrent runs. Each operator makes its own decision and submits the in-place update to the running Flink job. The harness submits the initial FlinkDeployment and observes it through Kubernetes and Flink REST.

The external Nexmark producer targets 80,000 generated events/s for 360 seconds, including bids. The `q8_unique` SQL query consumes persons and auctions. With the standard 1:3:46 event mix, its sources receive roughly 6,400 of the 80,000 generated events/s. Broker per-topic offset deltas measure the delivered mix and the Q8 charts show the consumer source rate. Each arm uses its own initially empty broker and topics. The runner submits the same Kafka-backed `com.github.nexmark.flink.sql.SqlQueryJob` used by experiment 1730. It uses experiment 1724’s shared `submit-job.sh`, `job-status.sh`, and `stop-job.sh` scripts for the consumer. A separate producer follows Q20’s stop, reset topics, and replay sequence on a detected consumer restart.

Both consumers start with two physically occupied slots on one TaskManager. The runner verifies this through Flink REST before input release. The branch enables fitted online costs and main uses its original fixed costs. Identical runtime artifacts, Q8 SQL, initial geometry, stabilization and metric windows apply to both arms. The default concurrent runs share the worker’s CPU and SSD.

## Run the comparison

Use the common workload artifacts from [BUILD.md](BUILD.md). Place `rocksdbjni.jar`, `abi3-mrc-telemetry.jar` and the two-slot `savepoint-profiles.jar` in `ARTIFACTS`. Copy `nexmark-flink-0.3-SNAPSHOT.jar`, `flink-sql-connector-kafka-3.3.0.jar`, and `kafka-clients-3.4.0.jar` from the Q8/Q20 benchmark image to the same directory. The application JAR must contain the `q8_unique` SQL resource and the `SqlQueryJob` entry point. The overlay also includes the serializable per-vertex Justin resource request used when Flink persists an in-place update. The runner uses the JDK’s `javap` to check this before submission. Rebuild older two-slot overlays with the command below. The shared-start configuration applies ordinary slot sharing to the initial job and passes native Justin rescaling to the runtime’s per-vertex allocator. Build each operator from its stated revision and keep the matching autoscaler JAR SHA-256 from that build. [Prepare a native operator](native-operator/README.md). The runner verifies the JAR inside the ready operator container before submission and again during cleanup. Enable DEBUG logging on `org.apache.flink.autoscaler.ScalingMetricEvaluator` to capture which costs the branch actually used.

```bash
bash shared-start/build-shared-start-overlay.sh "$REPO" "$DIST_LIB" "$BUILD_DEPS" \
  "$ORIGINAL_ARTIFACTS/savepoint-profiles.jar" "$ARTIFACTS/savepoint-profiles.jar"

export MAIN_NAMESPACE MAIN_OPERATOR_NAMESPACE MAIN_OPERATOR_DEPLOYMENT MAIN_REVISION
export MAIN_OPERATOR_JAR MAIN_OPERATOR_JAR_SHA256 MAIN_BOOTSTRAP_SERVERS MAIN_KAFKA_HOOK
export BRANCH_NAMESPACE BRANCH_OPERATOR_NAMESPACE BRANCH_OPERATOR_DEPLOYMENT BRANCH_REVISION
export BRANCH_OPERATOR_JAR BRANCH_OPERATOR_JAR_SHA256 BRANCH_BOOTSTRAP_SERVERS BRANCH_KAFKA_HOOK
bash quick_test.sh "$RESULTS/q8-native-80k"
```

Assign these variables to the deployed identities, external brokers, per-arm producer hook executables and build receipts before launching. A producer hook may reset only its own benchmark broker. The submitted job uses `--query q8_unique --source kafka --bootstrap-servers <arm-broker>` and a distinct job name. `*_OPERATOR_JAR` is the absolute in-container path used by its operator JVM. The default container is `flink-kubernetes-operator`. Set `MAIN_OPERATOR_CONTAINER` or `BRANCH_OPERATOR_CONTAINER` to select a different container. Set `MAIN_KUBECONFIG` and `BRANCH_KUBECONFIG` for separate contexts, or use one shared `KUBECONFIG`. A label-selected installation can set `MAIN_LABEL` and `BRANCH_LABEL` to its respective `key=value`. Set `RYAN_PLOTTER` to the pinned Q8 plotter when working offline.

The included `kafka_host.py` hook runs the Q20 standalone `insert_kafka_unique` producer on the external Kafka host. It uses dedicated broker and producer containers for each arm. On reset it recreates that broker with empty storage, recreates the three 24-partition topics, and starts the next generation at event ID 1. Use an SSH account on the producer host that can run the same Docker commands as experiment 1724.

```bash
export Q8_RUN_ID="q8-$(date -u +%Y%m%d-%H%M%S)"
export Q8_KAFKA_HOST=c153 Q8_KAFKA_HOST_IP=142.150.234.153
export MAIN_KAFKA_HOOK="$PWD/kafka_host.py" BRANCH_KAFKA_HOOK="$PWD/kafka_host.py"
export MAIN_BOOTSTRAP_SERVERS="$Q8_KAFKA_HOST_IP:29082" BRANCH_BOOTSTRAP_SERVERS="$Q8_KAFKA_HOST_IP:29182"
Q8_ARM=hardcoded "$MAIN_KAFKA_HOOK" provision
Q8_ARM=calibrated "$BRANCH_KAFKA_HOOK" provision
# Run quick_test.sh with the operator and artifact variables above. After the run:
Q8_ARM=hardcoded "$MAIN_KAFKA_HOOK" cleanup
Q8_ARM=calibrated "$BRANCH_KAFKA_HOOK" cleanup
```

The wrapper starts both arms in parallel. Set `Q8_ORDER=AB` or `BA` to run sequentially. When a single operator installation is switched between revisions, run `quick_pair.py` once per revision with the same output root and `--arm hardcoded` or `--arm calibrated`. The per-arm command accepts an optional explicit epoch-millisecond `--release-ms` for synchronized inputs. The default schedules the release 125 seconds after artifact staging, or eight seconds after both slots are confirmed if startup takes longer. Each arm has a distinct job ID, namespace-scoped resources and fresh state directories.

`native_check.py` reads observed operator events and Flink allocation and task samples. An operator acknowledgement, a changed slot count and resumed processing are separate measurements. Preserve the observation duration for arms without a verified resize. `plot_quick.py` generates the original Q8 throughput, managed-memory and occupied-slot charts from the per-arm resource samples.

## Historical savepoint-redeployment measurements

The following measurements used an external policy driver and an explicit savepoint redeployment. They describe the earlier experiment.

### Six-minute lab measurement

Both jobs offered 28,800,000 input records over the window and started with two occupied slots on one TaskManager. Counts are exclusive source offsets summed across the four logical partitions. The two jobs shared the same worker.

| Controller | Occupied slots, start → last observed | Final join placement | Records processed | Time of count |
|---|---:|---:|---:|---:|
| main | 2 → 2 | 1 × 158 MiB | 6,250,667 | 355.5 s |
| Online branch | 2 → 16 | 12 × 32 MiB | 2,787,745 | 355.5 s |

The paired run took 579.8 s from setup through cleanup. Main recorded 58 evaluator failures on empty stateless-source curves while the join had a populated curve.
The branch requested 12 join subtasks at 151.4 s and verified their placement after savepoint restore at 345.2 s. The saved offsets matched the restored source positions.
Replaying the fixed penalty coefficients in the branch planner on the decision snapshot selected the same placement.

During the first attempt with the runtime’s default ten-second stabilization window, Flink began constructing a 16-slot replacement with ten acquired slots. It failed to assign all tasks and its JobManager timed out. The checker rejected that attempt for the performance comparison. The rerun gave both JobManagers more time to acquire the proposed slots.

[Comparison chart and time series](results/80k-two-slots-2026-11-19/) · [First attempt](results/80k-two-slots-2026-11-19/initial-attempt/)

## Verified lab runs on November 19, 2026

Both pairs used the same Q8 runtime and matched starting allocations. Each result below uses four logical source offsets at the last complete five-second observation at or before 360 seconds, or the exact end offsets for a job that had already finished. Sampling timestamps are included so the counts can be compared at the same phase.

| Pair | Controller | Join allocation at cutoff | Occupied slots after scaling decision | Input records observed | Time of count after release |
|---|---|---:|---:|---:|---:|
| Default comparison | main | 1 × 158 MiB | 5 | 3,503,600 | 355.5 s |
| Default comparison | branch | 2 × 79 MiB | 6 | 2,623,978 | 355.5 s |
| Placement diagnostic | main | 2 × 33 MiB | 6 | 2,751,932 | 355.6 s |
| Placement diagnostic | branch | 2 × 79 MiB | 6 | 3,052,795 | 355.5 s |

The default test took 9 min 25 s from runner startup through cleanup. Main repeatedly rejected empty curves for the stateless sources. The populated join curve and errors are retained in the measurements. The online branch applied a qualified live estimate and restored the requested allocation.

The separate placement diagnostic applied a two-subtask join in both arms. Online had 10.9% more observed source records near the 360-second mark. At the savepoint the branch was already ahead by 221,372 records. From those saved offsets through the comparison sample, branch advanced another 146,165 records and main advanced 66,674. This six-minute window was selected retrospectively for the diagnostic. During the longer full-drain attempt, the online job finished in 449.0 seconds from release and main exceeded the safety timeout. That full-drain diagnostic took about 12 min 38 s including cleanup.

Each pair ran concurrently on one worker. The results include savepoint overhead and reflect this input schedule, worker, observation window, and all controller changes on the branch.

[Default comparison and samples](results/quick-2026-11-19/literal/) · [Placement diagnostic and samples](results/quick-2026-11-19/placement/)
