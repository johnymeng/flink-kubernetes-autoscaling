# Paired Q8 autoscaling benchmark

This experiment compares the original A4S cost model with the production online model on a sealed, checkpointed Nexmark Q8 workload. The fixed arm uses 5 µs per hit and 5 ms per miss. When coverage qualifies, the online arm uses its fitted baseline **and** its fitted incremental miss cost per input record. The fixed-intercept variant is a diagnostic evaluated at the same decision and never applied to the job.

The private controller collects the same native telemetry for both arms. Its standard Flink autoscaler computes the parallelism trigger. The selected policy applies its placement through an owned FlinkDeployment savepoint upgrade. Before applying it, the harness runs the alternate policy and the fixed-intercept diagnostic through the same placement code with an independent in-memory state store. Each receives the same metric history, miss-rate curve, standard scaler proposal and still-running slot allocation. The shared Kubernetes operator continues to reconcile the jobs. Its own scaling loop is disabled to prevent competing writes.

## Run on the lab

The lab needs its existing Flink operator, the `a4s-bench-ryan-q8` namespace, the runtime image pinned in `deployment.json`, and a worker with the mounted lab SSD. Set `KUBECONFIG` to the lab kubeconfig. Prepare the JARs using [the build notes](BUILD.md). Set `DIST_LIB` to the pinned Flink distribution's lib directory and execute from this experiment directory.

```bash
cp_path="$AUTOSCALER_JAR:$ARTIFACTS/abi3-mrc-telemetry.jar:$DIST_LIB/*"
mkdir -p "$BUILD/classes"
javac -proc:none -cp "$cp_path" -d "$BUILD/classes" RyanQ8Policy.java
jar cf "$BUILD/policy.jar" -C "$BUILD/classes" .
python3 run.py --output "$RESULTS" --worker c167 --artifacts "$ARTIFACTS" \
  --classpath "$cp_path" --policy "$BUILD/policy.jar"
python3 analyze.py "$RESULTS"/pair-* --output "$RESULTS/summary.json"
```

Use a fresh output directory for each invocation. The default order is fixed/online, then online/fixed. Each arm starts from one join subtask and two subtasks of each source. The source parallelisms stay fixed. The job uses the same per-partition schedule, key generation, resource limits, and job artifacts across arms. Each arm gets its own future release time, state paths, and initially empty RocksDB cache. The runner records binary hashes, submitted and deployed manifests, initial allocation, complete decision-time curves and model coefficients, requested resources, savepoint receipts, and verified post-restore native cache capacities.

The plan encodes `seconds:persons_per_second_per_subtask:auctions_per_second_per_subtask`. Its default phases offer 16,000, 80,000, 3,600 and 80,000 combined input records/s. The third phase permits observation of recovery; the final burst measures drain time. Flink's standard trigger determines if and when to resize. A no-action arm is an experimental outcome.

The primary result is completed sealed input records divided by time from release through job completion, including downtime. The analyzer checks per-partition savepoint and restore offsets, terminal input offsets, matching starting allocations and binaries, observed backlog and evidence of autoscaling. Five-second samples within an arm are correlated. Report the paired outcomes and their phase behavior individually.
