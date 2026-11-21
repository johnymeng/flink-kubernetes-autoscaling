# Q8 comparison with the built-in A4S autoscaler

Plan a bounded pair with the same operator, runtime, JNI, overlay, workload and effective settings. Arm A uses fixed costs and arm B uses online costs. The built-in operator handles any authorized action. Comparisons between different pinned branches are implementation bundles.

The recorded Kafka workload targets 80,000 **mixed Nexmark events/s**. The Person:Auction:Bid mix is 1:3:46, so the expected Q8-relevant input is 6,400 Person/Auction events/s. A target of 80,000 relevant events/s under that mix describes a separate workload. The existing executor keeps the recorded mixed-event load and empty named topics. The plan exposes projected targets and leaves actual counts unavailable until the broker and consumer produce evidence.

## Prepare an offline plan

Render each arm's requested FlinkDeployment from the staged local files. This uses the same renderer as the runner and hashes the files directly. The preview job ID and generated names are placeholders for the eventual trial.

```bash
python3 quick_pair.py --render-arm hardcoded --namespace <fixed-namespace> \
  --worker <worker> --owner <fixed-owner> --bootstrap-servers <fixed-broker> \
  --artifacts <directory>
python3 quick_pair.py --render-arm calibrated --namespace <online-namespace> \
  --worker <worker> --owner <online-owner> --bootstrap-servers <online-broker> \
  --artifacts <directory>
bash quick_test.sh --dry-run comparison-plan.json
```

The offline commands read local files and print JSON. They never launch a workload, check a Kubernetes context, run SSH, contact Kafka, invoke Docker or call a producer hook. An invalid or incomplete plan emits its findings and exits with status 2. A valid plan describes comparable intended settings. Its identity status can still be unverified.

The plan uses `schema_version: 1` and carries `comparison` (`same-build` or `implementation-bundle`), `claim` (`measurement` or `learned-action`), `orders` (a finite list such as `["AB", "BA"]`) and `execution` (sequential by default). Supply `arms.A` and `arms.B` with the fields below.

| Field | Required evidence |
|---|---|
| `trial_id`, `mode` | Unique trial ID and `fixed` for A or `online` for B |
| `source` | Full `revision`, tested `tree`, `native_gitlink`, and apply-ordered `native_patches` entries with `path` and `sha256` |
| `artifacts` | `requested` hashes for immutable operator/runtime images, base and overlay JARs, runtime telemetry, JNI and benchmark JAR, Kafka connector and client. See `ARTIFACT_KEYS` and `WORKLOAD_DEPENDENCIES` in `benchmark_plan.py`. Supply `deployed` and `observed_loaded` only from independent receipts. An evidence reference has `reference` and `sha256` fields. The loaded JNI ELF has its own SHA-256. |
| `manifest`, `operator_configuration` | The rendered workload manifest and effective operator configuration, including the reconcile cadence |
| `runtime_policy`, `actuation` | Estimator, timing instrumentation and ABI, metadata placement, managed/WBM settings, per-vertex floors and resource limits. Declare `built-in-operator-native` for a current comparison. |
| `placement` | Flink and broker workers, all relevant `worker_ids`, physical `storage_device_ids` and matching hardware/storage configuration. Concurrent arms require disjoint IDs and an `isolation_evidence` reference for each inventory. |
| `workload` | Query, source and producer implementations, pinned producer/broker images, producer hook hash, workload population and target, event mix, seed or dataset hash, first event ID, total event budget, topic names and partitions, initial offsets, state/reset policy and completion criterion |
| `horizon` | Explicit positive `duration_seconds`. For a learned-action claim, declare `action_budget_seconds` and `post_action_seconds`. The receipt checks the effective stabilization and collection windows against the paired-fit requirement. |

Same-build comparison ignores the cost selector and the harness's specific generated names, IDs, owner paths and broker endpoints. It retains the rest of the manifest and placement configuration for comparison. A concurrent plan also allows the physical worker and device IDs to differ when the supplied evidence documents disjoint resources. It records those IDs and evidence separately.

The release-to-cutoff budget includes time to qualify and time to observe the intended action and score its results. A measurement claim can describe an observation-only run. An insufficient learned-action plan stays at its requested duration. A valid time budget establishes planning feasibility while actual fit and actuation remain dependent on evidence.

## Runner interface

An authorized run binds the plan with `Q8_COMPARISON_PLAN`. The wrapper checks both arms before creating the result directory. Configure the existing per-arm `MAIN_*` and `BRANCH_*` operator identities, namespaces, broker addresses, hook paths and Kubernetes contexts. Set `OBSERVATION_SECONDS` explicitly and choose the predeclared entry with `Q8_PAIR_INDEX` and `Q8_ORDER`. Order defaults to AB. Use the plan's immutable producer and broker images through `Q8_SQL_IMAGE` and `Q8_KAFKA_IMAGE`. Per-arm worker and broker hosts can be set with `MAIN_WORKER`, `BRANCH_WORKER`, `MAIN_KAFKA_HOST` and `BRANCH_KAFKA_HOST`, with the corresponding broker IP values as required by the hook.

The explicit historical path uses `Q8_LEGACY_IMPLEMENTATION_BUNDLE=1` with a sequential order. Direct `quick_pair.py` calls use `--comparison-plan` or `--legacy-implementation-bundle` and require `--observation-seconds`. A plan provides no live execution authorization.

The arm directory preserves the submitted manifest, staged artifact hashes, deployment receipt and operator class-origin probe. The workload receipt measures broker offset deltas for all three topics and for Person/Auction, with raw consumer observations preserved separately in `resource-series.jsonl`. The current observer records task placement and native cache observations where available. A process-classpath probe supplies evidence for the listed classes. The worker's loaded JNI ELF needs its own receipt.

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
