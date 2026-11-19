# Short Q8 comparison of main and the online cache-cost branch

The default test measures six minutes of input from a common release, then cleans up the jobs. With prebuilt artifacts the target is under ten minutes including setup. It runs the two real autoscaler controllers against identical bounded Q8 inputs. It compares the fixed controller built from main at `f8ce7fbb` with the online controller from the cache-miss-penalty branch. Both jobs use the same Flink runtime, native RocksDB library, checkpoint-resumable Q8 workload, and savepoint actuator. The measured treatment includes the complete controller changes, including cache sizing and the retention rule.

The test starts each join with one subtask and 158 MiB of managed memory. The four source partitions offer a total of 3,930,000 records, including a 95-second warmup at 16,000 records/s and a 26-second burst at 80,000 records/s. It sets the same 32 MiB managed-memory base in both controllers so a small cache proposal is usable after restore. The standard autoscaler determines when and how to resize. By default the jobs start together on the same worker, sharing CPU and SSD bandwidth. This makes the pair quick to run and limits what one timing difference can establish.

## Prepare the pinned controllers once

Use [the build instructions](BUILD.md) for the common worker artifacts and the online autoscaler. On the Linux lab host, set `REPO`, `DIST_LIB`, `BUILD_DEPS`, `ARTIFACTS`, `BUILD`, and `KUBECONFIG` to the paths for that build. The local Git repository must contain the pinned main commit. Use the same branch revision that built the online autoscaler when setting `BRANCH_REVISION`. Compile the two measurement drivers from the experiment directory.

```bash
export BRANCH_REVISION=$(git -C "$REPO" rev-parse HEAD)
bash quick-main/build-main-policy.sh "$REPO" "$DIST_LIB" "$BUILD_DEPS" \
  "$ARTIFACTS/abi3-mrc-telemetry.jar" "$BUILD/main"
mkdir -p "$BUILD/q8-branch-driver"
javac -proc:none -cp "$ARTIFACTS/branch-autoscaler.jar:$ARTIFACTS/abi3-mrc-telemetry.jar:$DIST_LIB/*" \
  -d "$BUILD/q8-branch-driver" RyanQ8Policy.java
jar cf "$BUILD/policy.jar" -C "$BUILD/q8-branch-driver" .
```

## Run a pair

Give each invocation a fresh output directory. The wrapper runs both jobs, checks the live allocations and input offsets, and plots the four Q8 metrics. Python 3 with Matplotlib is required for rendering. The plotter loads Ryan's original Q8 plotting code at the pinned revision and verifies its SHA-256. On an offline host, set `RYAN_PLOTTER` to a local copy of that file. [Original Q8 plotter](https://github.com/RyanL123/memory-aware-scaling/blob/7d32b6abb0423453c4fbf3f44206abf5c2a81d9e/scripts/experiments/plot_run.py)

```bash
bash quick_test.sh "$RESULTS/q8-main-vs-online"
```

**Default comparison.** The main driver always calls main's evaluator with the original live metric history. When the evaluator throws, the driver archives the exception and rethrows it. An unchanged main allocation can qualify only with repeated recorded evaluator failures, live resource measurements, comparable source offsets, and a verified online resize. Both arms must have matching inputs, starting allocations, and runtime artifacts.

**Placement diagnostic.** To compare the two placement policies after both can evaluate the join, run a separate pair. Its main driver first records the raw evaluator's empty-curve failure. It then represents zero-point curves as missing, which main already knows how to skip. It preserves the populated join curve. Main's placement code and fixed cost model still make the proposal.

```bash
bash quick_test.sh "$RESULTS/q8-placement" omit-empty
```

The summary is in `quick-results.json`. The `plots/` directory contains a matched overview, each arm's original Q8 chart, and the time series in CSV format. The charts show the Flink source output rate, summed TaskManager managed memory used and total, and occupied task slots at five-second intervals. Managed memory is the Flink managed pool. Each arm's verified join reservation and native cache are recorded separately. Gaps mark unavailable samples during recovery. At six minutes the checker compares the last complete sample of all four logical partition offsets from each job, and records timestamps and the next sample when available. A window ending while Flink is running is recorded explicitly. Release-to-finish throughput is reported separately when both jobs drain naturally. Use `quick_pair.py --observation-seconds 0` for a full drain with an appropriate `--timeout`. The timer in `pair.json` also includes setup and teardown.

For sequential measurements use `quick_pair.py --order AB` and `--order BA` in fresh output directories. The result checker and same Q8 plotter also accept those runs.

## Sustained 80,000 inputs/s with two starting task slots

The `two-slot-80k` scenario offers 8,000 Persons and 32,000 Auctions per second from each of two source partitions per stream for the whole observation window. A six-minute run releases 28,800,000 input records per job. During backpressure, pending input accumulates and resumes from checkpointed source offsets. Both autoscalers receive Flink's standard `pendingRecords` gauge. They calculate target rate from measured ingress and lag growth. The chart shows observed source throughput and a reference at the offered rate.

Both jobs start with one two-slot TaskManager. Its two CPU cores and 316 MiB managed pool provide 158 MiB per slot. Flink's standard slot-sharing allocator places the four source subtasks and one join subtask into those two slots. The common scheduler overlay enables this layout before a controller submits per-vertex resource requests. A savepoint rescale uses the A4S runtime's normal per-vertex slot allocator. Both JobManagers wait up to five minutes for a partial resource allocation to stabilize and proceed immediately when the full request arrives. The default ten-second stabilization window allowed the runtime to build a partial placement while new TaskManagers were still registering. A jump in slot count therefore includes the move to dedicated slots. Each controller chooses its join parallelism and memory. The source partition count stays fixed to preserve logical input offsets, and the join can scale to the job's 24 key groups. The checker compares the starting physical allocations with the four zero source offsets and records proposals, applied placements and measured join targets.

Build the common artifacts and the two drivers above from this revision. Create a separate artifact directory for the shared-start scheduler, then launch from the experiment directory with a fresh result path.

```bash
shared_artifacts=$(mktemp -d)
for jar in Query8.jar rocksdbjni.jar abi3-mrc-telemetry.jar branch-autoscaler.jar; do
    cp "$ARTIFACTS/$jar" "$shared_artifacts/$jar"
done
bash shared-start/build-shared-start-overlay.sh "$REPO" "$DIST_LIB" "$BUILD_DEPS" \
    "$ARTIFACTS/savepoint-profiles.jar" "$shared_artifacts/savepoint-profiles.jar"
ARTIFACTS="$shared_artifacts" bash quick_test.sh "$RESULTS/q8-80k-two-slots" literal two-slot-80k
```

`quick-results.json` reports the actual starting and ending occupied slots, input processed at the comparison sample, and the target rates observed by each controller. Both controllers may choose to keep their allocations. The runner verifies live measurements in that case as well.

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
