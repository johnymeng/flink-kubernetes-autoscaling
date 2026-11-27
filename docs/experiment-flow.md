# Experiment Flow

Use this guide to run and compare Flink autoscaling experiments on the fs.csl
lab. The first reference profile is the 80,000 records/s Nexmark Q8 Java
DataStream comparison shown in [PR #25](https://github.com/johnymeng/flink-kubernetes-autoscaling/pull/25).
The [external Kafka Q20 procedure](#legacy-external-kafka-q20) below uses its
own producer and input lifecycle.

## Deploy on the fs.csl lab

1. Connect through `fs.csl.utoronto.ca` to the lab control host `c165` with your
   lab account. `c167` was the SSD worker for the Q8 reference. Check current
   worker reservations, access, and capacity before selecting it.

   ```bash
   ssh -J "${LAB_ACCOUNT:?set your lab account}@fs.csl.utoronto.ca" "$LAB_ACCOUNT@c165"
   ```

2. From the checkout on `c165`, load the [lab configuration](../cluster/config/README.md)
   and inspect the physical cluster. The configured Unix group grants access
   to the shared kubeconfig. Keep the physical kubeconfig explicit during
   preflight so a kubeconfig inherited from a previous run cannot select a
   different API.

   ```bash
   source cluster/config/env.sh
   lab=(kubectl --kubeconfig "$SHARED_KUBECONFIG")
   "${lab[@]}" config current-context
   "${lab[@]}" get nodes -o wide
   "${lab[@]}" describe node c167
   "${lab[@]}" get flinkdeployments -A
   "${lab[@]}" get pods -A --field-selector spec.nodeName=c167 -o wide
   ```

   Check node pressure, requested CPU and memory, storage space and mounts, and
   capacity through the largest planned resize. Include the operator,
   JobManager, virtual-cluster overhead, and all planned TaskManagers. Check
   placement and pullability for the selected image digests on the worker.

3. Use a fresh run ID, result directory, run-owned physical namespace, and
   virtual cluster with one operator watching a single benchmark namespace.
   Use the lab bootstrap and cleanup bundle for that run. For a fresh
   checkout, obtain the lab's vCluster chart and values, storage helpers, and
   observer from the run owner alongside the pinned build receipt. The
   physical cluster is shared. Run the bootstrap against the physical API
   only within the run's owned scope. Install the Flink CRDs and the pinned
   operator Helm chart through the virtual API. Set `watchNamespaces` to
   the benchmark namespace and pin the operator image by digest. Record the
   namespace UIDs and the owner label. Stage the worker-local RocksDB
   directory and checkpoint/HA storage with fresh run-owned paths and the
   permissions used by the job.

4. Check the virtual API identity, namespace, FlinkDeployment CRD, operator
   watch scope, Flink service account, and operator rollout. Render and
   save the manifest with exact image digests and workload arguments. Dry-run
   against the selected API and confirm the workload name is free before
   creating it. Some historical launchers replace a deployment named `flink`;
   check their scope before invoking them.

5. Before releasing input, confirm the expected Flink job and subtasks are
   `RUNNING`, the startup profiles and placement match, and the sampler is
   attached. Keep a single run ID across the manifest, sampler, operator
   logs, and result index. Record the input release time and cutoff
   separately from deployment and cleanup time.

6. Save the rendered deployment, release and startup receipts, raw
   samples and counters, operator decisions, checkpoint and resize records,
   and terminal status before deleting live evidence. Use the run's cleanup
   helper after checking its namespace UIDs and owner label. Verify the
   owned jobs, storage and namespace are gone and shared workload identities
   are unchanged. Retain partial evidence and record failures at the stage
   they occurred. Exclude kubeconfigs, tokens, and SSH material from the
   archive.

## Make runs comparable

- Before launching a pair, write down the treatment, builds, workload and
  input starting point, worker placement, starting resources, scheduler and
  checkpoint settings, sampling cadence, and scoring windows. Use the same
  artifact identities across arms for a policy-only comparison. When the
  treatment requires different builds, record both and describe the result
  as a build comparison.
- Set the time origin to the recorded input release. Keep the offered rate,
  source output, and stateful operator input as separate measurements.
  Calculate a counter-derived rate as total accepted counter increase divided
  by elapsed time over the accepted intervals. Use compatible endpoints for
  the same subtasks and attempts, exclude reset or missing intervals, and
  report coverage in seconds alongside the rate. Compare the same time
  windows and report any different coverage in each arm.
- Treat an operator scaling decision as a request. Record when the requested
  subtasks are all running and which resource profiles were actually applied.
  Record pending placement and incomplete resizes at cutoff. To establish an
  in-place rescale, compare the JobManager pod UID and restart counts.

## Reference benchmark: Nexmark Q8 DataStream

This profile comes from Main `live-fe10954f` and Online `live-51dae839`, the two
runs in the [Q8 comparison](plot-examples/q8-comparison.png). It runs
`be.uclouvain.gepiciad.nexmark.Query8` with Person and Auction generators in the
job and a discarding sink. A Kafka-backed `q8_unique` run has a different
input path and should have its own profile.

| Setting | Q8 reference |
| --- | --- |
| Offered workload | Two Person subtasks at 8,889 records/s each and two Auction subtasks at 31,111 records/s each. All four start at offset zero. |
| Timed input | `--q8-input-plan 600:8889:31111` and a fresh `--q8-release-epoch-ms`. At release + 600 s, the plan stops offering demand. |
| Starting placement | Two TaskManagers on `c167`, one running join/window subtask, and five reserved slots. JobManager has 1 CPU and 2,048 MiB process memory. |
| TaskManager resources | 4 CPU, 4 slots, 2,220 MiB process memory and a fixed 768 MiB managed memory pool per TaskManager. The worker envelope was five TaskManagers and 20 TaskManager CPU. |
| Startup witness | 1,536 MiB aggregate TaskManager managed capacity, 192 MiB reported managed use, and two execution allocations. Record reservations and allocations separately. |
| Scheduling and state | Adaptive scheduler, native in-place scaling, managed RocksDB on local SSD, write-buffer ratio 0.5, and 20 s checkpoint interval on the run-owned state mount. |
| Autoscaler | A4S and native scaling execution enabled. 3 min metrics window, 2 min stabilization, target utilization 0.7 with boundary 0.1, and max vertex parallelism 24. Person and Auction sources stay fixed at parallelism 2. |
| Treatment | Main uses fixed costs of 5 µs per hit and 5 ms per miss. Online enables `job.autoscaler.a4s.online-cache-costs.enabled`. Both keep shadow mode off and memory tuning enabled. |
| Run order | Main then Online on the same SSD worker. Complete capture and scoped teardown for Main before starting Online with fresh state. |
| Sampling and scoring | Resource samples every 5 s at nominal T+0, 5, …, 600 s, including the boundary observation. Score the timed window and the late window from T+300 s through cutoff. Use actual compatible counter timestamps and report coverage. |

The original pair differed in runtime and operator build as well as the online
cost flag. For an exact replay, use these archived release identities. Verify
the digests still resolve and the running pods load the expected JARs and
FRocksDB JNI binary. If the lab registry no longer has an image, recover it
from the release owner or record a replacement build. See [Image Builds](image-builds.md)
for the repository's build conventions.

| Arm | Source revision | Runtime image in the lab registry | Operator image in the lab registry |
| --- | --- | --- | --- |
| Main | `f8ce7fbbfae48869d8b1869748ad517801a9f726` | `ece496-a4s-r12-literal-main/q8@sha256:5c68e1a1374b7bba650ba340527c81e48c5d315a0eb43868bdd5c237166c0e7a` | `ece496-a4s-r12-literal-main/flink-kubernetes-operator@sha256:61a0c8c9f3263b78a1b57f00bce2f9d2b2467afe4cb5278190f61501830d4480` |
| Online | `273a09a86795e1c9eaea329db1c31a069987ff43` | `ece496-a4s-q8-online-20261121/q8@sha256:ff8ba7601a53fbdddc9700b01f23136626f7b6db49e9ffefc6e716432c0136ca` | `ece496-a4s-q8-online-20261121/flink-kubernetes-operator@sha256:c7625c2e1e28f0f585f74925882271bd7ad20c99e1c7dd68a71b2852ba10bb75` |

Prefix the full image references with the lab `REGISTRY` from
`cluster/config/env.sh`. Both images used
`local:///opt/flink/usrlib/Query8.jar` with SHA-256
`8992573104399f86a61bb096a676329e6e447cb060961a842242867077983a9f`.
Verify that the JAR accepts the bounded input and release arguments before
using this profile. The original run bundle is indexed on the benchmark
workstation under `outputs/q8-main-startup-memory-comparison/original-pair/`.
Its `execution-plan.json`, per-arm `release-receipt.json`, and startup witness
record the exact artifacts and admitted configuration.

### Render and submit Q8

Use the [reference manifest template](q8-datastream-reference.json) with the
scoped lab runner. It carries the shared Flink settings from the pair,
including checkpointing, source vertex exclusions, and telemetry. For a
different workload JAR, verify the expected source vertex IDs against its job
graph before deploying.

Set the variables below from the runner's spec and ownership receipts.
`BENCH_RUNTIME_IMAGE`
is the digest-qualified image for the selected arm and `BENCH_ONLINE` is
`false` for Main or `true` for Online. Use the arm's fresh
`BENCH_RUN_ID` and `BENCH_JOB_ID`, future `BENCH_RELEASE_EPOCH_MS`,
`BENCH_WORKER`, `BENCH_NAMESPACE`, and prepared `BENCH_STATE_HOST_PATH` and
`BENCH_ROCKSDB_HOST_PATH`. Export these variables for `jq`. Use the arm's
existing `BENCH_RUN_DIR` and the scoped `BENCH_KUBECONFIG` and `BENCH_CONTEXT`.
Allow time for artifact checks and startup. Record a missed startup witness
or sampler attachment as an invalid trial and retry with a fresh run ID.

```bash
jq -e '
  .metadata.namespace = env.BENCH_NAMESPACE
  | .metadata.labels["a4s-pr2-run"] = env.BENCH_RUN_ID
  | .spec.image = env.BENCH_RUNTIME_IMAGE
  | .spec.flinkConfiguration["$internal.pipeline.job-id"] = env.BENCH_JOB_ID
  | .spec.flinkConfiguration["job.autoscaler.a4s.online-cache-costs.enabled"] = env.BENCH_ONLINE
  | .spec.job.args[-1] = env.BENCH_RELEASE_EPOCH_MS
  | (.spec.jobManager, .spec.taskManager).podTemplate.spec |= (
      .nodeSelector["kubernetes.io/hostname"] = env.BENCH_WORKER
      | .volumes[] |= (
          if .name == "state" then .hostPath.path = env.BENCH_STATE_HOST_PATH
          elif .hostPath then .hostPath.path = env.BENCH_ROCKSDB_HOST_PATH
          else . end
        )
    )
  | if ([.. | select(. == null or . == "")] | length) > 0
    then error("A required BENCH environment variable is missing")
    elif (env.BENCH_ONLINE != "true" and env.BENCH_ONLINE != "false")
    then error("BENCH_ONLINE must be true or false")
    else . end
' docs/q8-datastream-reference.json > "$BENCH_RUN_DIR/deployment.json"

bench=(kubectl --kubeconfig "$BENCH_KUBECONFIG" --context "$BENCH_CONTEXT" -n "$BENCH_NAMESPACE")
"${bench[@]}" create --dry-run=server -f "$BENCH_RUN_DIR/deployment.json"
```

Use the verified runner to create that deployment and start observation against
the owned `flink-rest` service before the sealed release. Confirm the expected
Person/Auction/window parallelism of 2/2/1, the startup witness in the table
above, and zero source progress before release. Check that counters advance
after release. At cutoff, retain the final job and checkpoint snapshot and
have the runner suspend or stop the job. Account for any pending native resize
using the cutoff, even if the job recovers during teardown.

### Capture Q8 measurements

Collect from the same Flink REST endpoints in both arms and record the job ID,
attempts, scheduled elapsed time, actual capture time, and sampling errors.

| Measure | What to capture |
| --- | --- |
| Offered input | The generator schedule, including each event type's configured rate and offsets. Report this separately from measured rates. |
| Source throughput | Sum `numRecordsOutPerSecond` across the running Person and Auction source subtasks for the chart. Save cumulative `numRecordsOut` and the source attempts for counter-derived rates. |
| Join/window throughput | Save cumulative `numRecordsIn` for the `TumblingEventTimeWindows` vertex and attempt IDs. Score same-attempt deltas and report the accepted interval coverage. |
| Managed memory | Sum `Status.Flink.Memory.Managed.Used` and `Status.Flink.Memory.Managed.Total` across registered TaskManagers. Save per-vertex applied profiles separately. If container memory or RocksDB cache occupancy is recorded, give each its own label. |
| Occupied slots | Use Flink `/overview` slots total minus slots available for reserved slots. Save TaskManager counts and active execution allocations separately. |
| Rescale and backlog | Record request and completion times, running parallelism, applied profiles, and incomplete requests at cutoff. Capture `pendingRecords` and checkpoint history around changes. |

Keep every scheduled sample slot. Preserve real zeros and leave failed or
missing readings blank or NaN. Store the raw evidence and produce a separate
plotting CSV with `timestamp_epoch`, `source_records_per_second`,
`total_managed_memory_used_bytes`, `total_managed_memory_total_bytes`, and
`task_slots_used` under `nexmark/q8/<run_id>_<environment>_<autoscaler>/samples.csv`.
Record those three fields and `run_commit` in `nexmark/q8/runs.csv`. Report
source output and measured join input as separate rates. The discarding sink
means these measurements describe processing progress.

```bash
MPLBACKEND=Agg python3 experiments/plot_run.py --query q8 \
  --results-root "$Q8_RESULTS_ROOT" --run-id "$Q8_MAIN_RUN_ID" \
  --compare-run-id "$Q8_ONLINE_RUN_ID" --label Main --compare-label Online \
  --output q8-comparison.png
```

When the arms have separate indexed roots, pass the Online root with
`--compare-results-root`. Keep the run index and raw sampler evidence with
the figure. Use the recorded release and cutoff for scoring. The plot aligns
each run to its first recorded sample.

## Legacy external Kafka Q20

The `q20_unique` SQL benchmark lives under `experiments/1724-kafka-q20-unique`.

At a high level, the experiment separates data generation from the autoscaled
Flink job:

1. A Kafka broker runs outside the Kubernetes cluster, currently on `c153`.
2. A standalone Flink producer job also runs on `c153`. It runs the
   `insert_kafka_unique` query and writes Nexmark events into Kafka.
3. The in-cluster Flink consumer job is submitted as a `FlinkDeployment`. It
   runs the `q20_unique` query and reads from Kafka through
   `kafka-external:9092`.
4. The Flink Kubernetes Operator watches that `FlinkDeployment`. Its autoscaler
   decides when to rescale the consumer job, and the operator applies those
   changes by updating the Flink deployment, restarting the job when needed, and
   creating or removing TaskManager pods.

The producer is not managed by the Flink Kubernetes Operator. It is kept outside
the cluster so the experiment can control the input stream independently from
the consumer job being autoscaled.

### Repository Layout

- `jobs/`: q20_unique consumer `FlinkDeployment` YAMLs for DS2 and Justin.
- `external-kafka/setup/`: one-time setup for the external Kafka host and the
  Kubernetes Service/Endpoints alias.
- `external-kafka/run/`: commands for the external Kafka broker, standalone
  Flink producer, and scaling coordinator.
- `analysis/`: scripts for exporting metrics/logs and plotting completed runs.
- `experiment-data/`: generated export output, ignored by Git.

### Data Path

Kafka runs on the external host. The setup script creates a Kubernetes Service
and Endpoints object named `kafka-external`, which points in-cluster consumers
at the external Kafka broker.

```text
standalone Flink producer on c153
  -> external Kafka on c153
  -> kafka-external:9092 inside Kubernetes
  -> q20_unique FlinkDeployment consumer job
```

The producer uses the benchmark runtime image and runs Flink in local mode on
the external host. The consumer uses the same benchmark runtime image inside
Kubernetes.

### Autoscaling Path

The consumer job is the autoscaling target. Submit either the Justin or DS2
consumer manifest from `experiments/1724-kafka-q20-unique/jobs/`:

```bash
scripts/autoscaling/job-management/submit-job.sh experiments/1724-kafka-q20-unique/jobs/justin/experiment.yaml
scripts/autoscaling/job-management/submit-job.sh experiments/1724-kafka-q20-unique/jobs/ds2/experiment.yaml
```

Both manifests enable the Flink autoscaler. The Justin manifest sets
`job.autoscaler.justin.enabled: 'true'`; the DS2 manifest sets it to `'false'`.

During a run, the operator reads metrics from the running Flink job and records
autoscaler decisions. When a scaling decision is applied, Flink's adaptive
scheduler moves the job through a fast restart/reconfiguration cycle. Kubernetes
then starts or removes TaskManager pods to match the new resource shape.

Use these scripts to watch the job and autoscaler state:

```bash
scripts/autoscaling/job-management/job-status.sh
scripts/autoscaling/job-monitoring/observe-scaling.py --follow
scripts/autoscaling/status.sh
```

Stop the in-cluster consumer job with:

```bash
scripts/autoscaling/job-management/stop-job.sh
```

### External Kafka Sequence

Run the setup scripts once for a producer host. For the current setup that host
is `c153`; override `TARGET_HOST`, `TARGET_IP`, and `HOST_TAG` if you move the
producer to another machine.

```bash
experiments/1724-kafka-q20-unique/external-kafka/setup/01-prepare-generator-host.sh
experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh apply
```

After that one-time setup, normal experiment runs use the scripts under
`external-kafka/run/`. Reset or start Kafka, start or stop the producer, and run
the scaling coordinator from there:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
```

Useful repeated-run commands:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
```

The scaling coordinator watches the in-cluster consumer job through the Flink
REST API. When it detects a rescale, it pauses the standalone producer, stops
it, resets Kafka topics, waits for the consumer job to return to `RUNNING`, and
then restarts the producer from the beginning of the Nexmark event stream.

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps 50000 \
  --events 300000000
```

That reset behavior keeps each post-rescale run aligned with an empty Kafka
topic and a producer that starts again from event id `1`.

## Export and Plot Results

After a run, export metrics, logs, and metadata:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py
```

Every experiment has a `plot.py` command that uses the same stacked charts.
Plot an existing Prometheus export by passing its directory:

```bash
experiments/1724-kafka-q20-unique/plot.py \
  --run-dir experiments/1724-kafka-q20-unique/experiment-data/my-run \
  --output q20.png
```

For an indexed archive, pass the root containing `nexmark/<query>/runs.csv`
and select a run. Use `--latest` or omit run selection to use the last entry.
The Kafka experiment commands default to their own query. Supply `--query`
for indexed checkpoint-aware runs or when using `experiments/plot_run.py`.

```bash
experiments/1725-kafka-q4-unique/plot.py \
  --results-root /path/to/archive --run-id first-run \
  --compare-run-id second-run --compare-results-root /path/to/second-archive \
  --label Justin --compare-label DS2 \
  --metrics source_records_per_second task_slots_used \
  --output q4-comparison.png
```

Use `--samples` for a standalone CSV with numeric timestamps in seconds and
one column per metric. `--run-dir` also accepts a directory containing
`samples.csv`. Use `--compare-samples` or `--compare-run-dir` to overlay another
file or directory. Run `--list-metrics` with your input arguments to see the
exact names accepted by `--metrics`. Comparisons default to all metrics shared
by both inputs. Without `--output`, the image is saved with the first run.

Each subplot starts at zero on both axes. An indexed run uses its own first
recorded timestamp as zero, while a Prometheus export uses its recorded export
start when available. Blank and NaN samples remain gaps, with dotted lines
marking gaps between measured points. Managed memory used and capacity share
a panel when both are selected.
