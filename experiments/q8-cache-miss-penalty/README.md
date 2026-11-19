# Paired Q8 autoscaling benchmark

For paired runs of main and the online branch, use [the built-in A4S benchmark](QUICK.md). The operator installed for each revision owns autoscaling and in-place updates. The default runner submits the official Kafka-backed `q8_unique` SQL job, targets 80,000 mixed Nexmark events/s from each external producer, and starts each consumer with two physically occupied slots.

## Historical external-policy experiment

The remainder of this note documents the earlier savepoint-redeployment harness and its archived results.

This experiment compares the original A4S cost model with the production online model on a sealed, checkpointed Nexmark Q8 workload. The fixed arm uses 5 µs per hit and 5 ms per miss. When coverage qualifies, the online arm uses its fitted baseline **and** its fitted incremental miss cost per input record. The fixed-intercept variant is a diagnostic evaluated at the same decision and never applied to the job.

The private controller collects the same native telemetry for both arms. Its standard Flink autoscaler computes the parallelism trigger. The selected policy applies its placement through an owned FlinkDeployment savepoint upgrade. Before applying it, the harness runs the alternate policy and the fixed-intercept diagnostic through the same placement code with an independent in-memory state store. Each receives the same metric history, miss-rate curve, standard scaler proposal and still-running slot allocation. The shared Kubernetes operator continues to reconcile the jobs. Its own scaling loop is disabled to prevent competing writes.

### Reproduce the historical experiment

The lab needs its existing Flink operator, the `a4s-bench-ryan-q8` namespace, the runtime image pinned in `deployment.json`, and a worker with the mounted lab SSD. Set `KUBECONFIG` to the lab kubeconfig. Prepare the JARs using [the build notes](BUILD.md). Set `DIST_LIB` to the pinned Flink distribution's lib directory and execute from this experiment directory.

```bash
cp_path="$AUTOSCALER_JAR:$ARTIFACTS/abi3-mrc-telemetry.jar:$DIST_LIB/*"
mkdir -p "$BUILD/classes"
javac -proc:none -cp "$cp_path" -d "$BUILD/classes" RyanQ8Policy.java
jar cf "$BUILD/policy.jar" -C "$BUILD/classes" .
python3 run.py --legacy-external --output "$RESULTS" --worker c167 --artifacts "$ARTIFACTS" \
  --classpath "$cp_path" --policy "$BUILD/policy.jar"
python3 analyze.py "$RESULTS"/pair-* --output "$RESULTS/summary.json"
```

Use a fresh output directory for each invocation. The default order is fixed/online, then online/fixed. Each arm starts from one join subtask and two subtasks of each source. The source parallelisms stay fixed. The job uses the same per-partition schedule, key generation, resource limits, and job artifacts across arms. Each arm gets its own future release time, state paths, and initially empty RocksDB cache. The runner records binary hashes, submitted and deployed manifests, initial allocation, complete decision-time curves and model coefficients, requested resources, savepoint receipts, and verified post-restore native cache capacities.

The plan encodes `seconds:persons_per_second_per_subtask:auctions_per_second_per_subtask`. Its default phases offer 16,000, 80,000, 3,600 and 80,000 combined input records/s. The third phase permits observation of recovery; the final burst measures drain time. Flink's standard trigger determines if and when to resize. A no-action arm is an experimental outcome.

The source exposes its sealed backlog as `q8Backlog` for verification. The historical burst benchmark uses the legacy source metrics and observed throughput and utilization. The sustained 80k scenario also exports the standard `pendingRecords` gauge, so those controllers can measure ingress demand under backpressure. Its measured target rate is recorded separately from the offered schedule.

The primary result is completed sealed input records divided by time from release through job completion, including downtime. The analyzer checks per-partition savepoint and restore offsets, terminal input offsets, matching starting allocations and binaries, observed backlog and evidence of autoscaling. Five-second samples within an arm are correlated. Report the paired outcomes and their phase behavior individually.

## Q8 results from November 19, 2026

Each run completed all 5,634,000 scheduled input records. Flink's ordinary autoscaler triggered one join resize per run. All four source partitions resumed from their saved offsets and subsequently reached their scheduled end offsets.

| Execution order | Applied model | Zero-miss baseline (µs/record) | Miss coefficient (µs/record) | Completion (s) | Completed inputs/s | Request to verified restore (s) | Peak sampled backlog (records) |
|---|---|---:|---:|---:|---:|---:|---:|
| Pair 1, run 1 | Fixed | 5.00 | 4,995.00 | 446.717 | 12,612.0 | 126.5 | 2,613,559 |
| Pair 1, run 2 | Online | 43.75 | 12.27 | 451.943 | 12,466.2 | 120.3 | 2,335,074 |
| Pair 2, run 1 | Online | 44.00 | 10.41 | 458.347 | 12,292.0 | 119.0 | 2,277,264 |
| Pair 2, run 2 | Fixed | 5.00 | 4,995.00 | 461.950 | 12,196.1 | 118.2 | 2,357,822 |

Both models use the QuickMRC miss fraction as input. Multiplying that fraction by the miss coefficient and adding the zero-miss baseline gives estimated busy time per input record. In the fixed model the coefficient is 4,995 µs, the difference between 5 ms and 5 µs. Each online arm applied both of its fitted coefficients.

At the first trigger, the fixed model allowed a 2.44% miss fraction. The lowest fraction on the measured curve was 77.75%. The online model allowed 100%. All four decisions had this pattern. Neither model identified a measured cache size that justified changing the total managed-memory budget. Both planners split the existing budget across two join subtasks at 79 MiB of managed memory per slot. Each live RocksDB cache reported 69,031,253 bytes after restore.

Online throughput relative to fixed was -1.16% for the first pair and +0.79% for the reversed pair. These two pairs ran on a shared lab cluster and exercised identical placements. They measure end-to-end performance for this workload. The performance effect of choosing different allocations remains unmeasured. Backlog peaks are sampled. Request-to-verified times include controller observation delays. Both source-offline boundary events were available only for the first online run, so the table uses request-to-verified timing consistently.

The per-arm results include decision coefficients, alternative proposals evaluated on each arm’s exact snapshot, confirmed allocations, checkpoint offsets, and artifact hashes. [Machine-readable results](results/2026-11-19.json)

The accompanying raw evidence archive, `q8-autoscaling-evidence.tar.gz`, has SHA-256 `0d6e74b6e0cdb346c1d9652693a24f0cb03653d3ee4a0791d889db4332cb9c3f`. Its internal manifest covers the decision snapshots, live deployment receipts, source offsets, and build verification.
