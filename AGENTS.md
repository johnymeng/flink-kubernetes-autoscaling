# ECE496 - Efficient Autoscaling in Stream Processing Systems

Read this guide when starting work in the ECE496 workspace. The capstone studies how Apache Flink can keep up with a changing input rate while using CPU and RocksDB managed memory efficiently. A4S uses a miss-rate curve (MRC) to estimate how a vertex would perform with a different cache size, chooses memory and parallelism, and asks Flink to realize that choice. An ideal change must account for the cost of moving state and recovering throughput after a resize and meet the throughput requirements while utilizing minimal CPU and memory resources.

## Find and change the owning code

Paths in this table are relative to the selected checkout. A4S, Justin and DS2 experiments use different implementations. Trace the manifest's images and entry point back to their sources first. [A4S architecture](flink-a4s-online-main/A4S.md)

| What changes | Where to start |
| --- | --- |
| Native cache sampling, histogram or JNI | `sources/libs/frocksdb`, the checkout's Git submodule pin, and `sources/libs/patches/`. Inspect the patch list in `images/frocksdb/build.sh`. |
| RocksDB metrics, REST aggregation, state restore or slot allocation | `sources/flink/flink-runtime-1-18-a4s/`. Look in `flink-state-backends/flink-statebackend-rocksdb`, `flink-a4s` and `flink-runtime`. |
| Choosing memory and parallelism | `sources/operators/flink-kubernetes-operator-a4s/flink-autoscaler/`. Follow `RestApiMetricsCollector` → `ScalingMetricEvaluator` → `A4S` → `ScalingExecutor`. The existing Justin resource-requirements endpoint passes decisions to Flink's adaptive scheduler. |
| Workload, experiment, deployment | `sources/benchmarks/` owns the job and input semantics. `experiments/` owns manifests and protocols, `images/` packages binaries, `cluster/` holds infrastructure configuration, and `scripts/autoscaling/` contains operational helpers. |

- **Keep telemetry meaningful across a resize.** Preserve units, wire compatibility, and the difference between missing observations and a measured zero. Bind a fit to the applicable attempt, worker process and physical cache. Requalify after a change in identity, capacity or parallelism. For shared caches, establish which vertex owns the observations. [Online QuickMRC model and qualification](flink-a4s-online-main/docs/a4s-online-quickmrc-cost.md)
- **Follow the decision into the runtime.** Reuse the existing scheduler and resource-profile paths. Track requested, acknowledged and actually applied resources separately. If a curve is missing, test what happens to the live managed-memory allocation during ordinary throughput recovery. Derive bounds from measured behavior or the runtime contract. A Q8-specific memory floor hides the failure on other jobs.
- **Test the failure at its owning layer.** Reproduce it on the base revision, change the responsible component, and rerun the focused test and its neighbors. For JNI lifetime bugs, exercise concurrent polling and close with the matching native library. For restore changes, cover failure/cancellation, shared state files and a subsequent checkpoint/restore. For policy changes, exercise missing, stale and valid observations, and the resulting profile request.
- **Check which classes actually ran.** Build against matching Flink snapshot artifacts, select the tests explicitly and inspect the executed test count. The image Dockerfiles skip tests. For example, after installing the matching dependencies, run `mvn -pl flink-autoscaler -Dtest=A4STest,ScalingMetricEvaluatorTest test` from the A4S operator source root. A packaged test and a timed lab run answer different questions. [Worked operator test with commands and logs](outputs/pr2-fixed-curve-20261120/verification.md)

## Build the A4S stack

The following Bash recipe matches `flink-a4s-online-main` at `4a4ea607`. Start at the root of a clean checkout on a builder with Docker/BuildKit and Linux amd64 support. The benchmark wrapper also invokes Maven on the host. Use a compatible JDK (Java 11 for this checkout) and make the matching Flink snapshot dependencies available to Maven. Confirm the branch's `scripts/env.sh`, Dockerfiles and experiment manifest agree on source trees, Java version, platform and tags. The generic `build-all-default.sh` follows a different image selection. [Build wrappers](flink-a4s-online-main/images/) · [Environment](flink-a4s-online-main/scripts/env.sh)

```bash
set -euo pipefail
git submodule update --init --recursive
bash images/test-a4s-build-inputs.sh
tag="a4s-$(git rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M%S)"
bash images/frocksdb/build.sh
bash images/flink-runtime-a4s/build.sh --tag "$tag"
DOCKER_DEFAULT_PLATFORM=linux/amd64 bash images/flink-benchmark-runtime/build.sh \
  --runtime-image "flink-runtime:$tag" --tag "$tag" --force-datastream-build
OPERATOR_A4S_TAG="$tag" bash images/flink-kubernetes-operator-a4s/build.sh
```

The native build installs the patched JNI JAR into the Flink source tree. Both the runtime and operator builds consume that tree. The benchmark image adds the workload JARs to the selected runtime. Force the DataStream rebuild when the workload changes because the wrapper can reuse prior JARs. The runtime and benchmark wrappers support `--dry-run`. The operator wrapper invokes Docker immediately and creates a local, registry-qualified tag.

For a lab release, identify the permitted builder and registry path for this task. Publish the intended images with run-specific tags, record their immutable digests, and compare the running pod image IDs and loaded JAR/JNI hashes to the build receipt. Check the operator as well as the workers. For Kafka Q8 with distribution overlays use its [artifact build instructions](flink-cache-miss-penalty/experiments/q8-cache-miss-penalty/BUILD.md).

## Work on the shared lab

| Workload | Entry point and input protocol |
| --- | --- |
| Figure 10 DataStream Q8, including the 80k investigation | Paced Person and Auction generators run inside the Java job. The [runbook](LAB_EXPERIMENT_RUNBOOK.md#java-datastream-q8-and-figure-10) gives the isolated launcher, artifact checks, fetch and plot commands. The archived c167 replay uses fixed profiles. For an online trial, verify the execution plan, policy flags and observed decisions. |
| SQL Q8 | Use [1730](flink-kafka-q8-unique/experiments/1730-kafka-q8-unique/README.md) for DS2/Justin, or the [native A4S pair](flink-cache-miss-penalty/experiments/q8-cache-miss-penalty/QUICK.md) for the main/miss-penalty comparison. The Kafka mixed input includes bids that Q8 never consumes. |
| Other SQL workloads or checkpoint-aware rescaling | The [launcher matrix](LAB_EXPERIMENT_RUNBOOK.md#pick-the-experiment) maps queries to experiments 1724–1728. [1729](flink-a4s-experiment-runners/experiments/1729-checkpoint-aware-rescaling/instruction.md) pauses and resumes the same producer across the checkpoint. Resetting the broker would invalidate that protocol. |

1. **Inspect the physical cluster.** Use the configured SSH route to `c165` for control and orchestration. `c167` and `c182` are the usual SSD workers. Legacy Kafka runs use `c153` for the external broker and producer. Confirm current access, free resources, node pressure, competing jobs, quotas and disk activity. A Ready node can be busy. For two-host restores, verify local SSD state paths and a shared checkpoint mount on both hosts.
2. **Establish run ownership.** Use a fresh run ID, a run-owned namespace with a correctly scoped operator or the experiment's isolated virtual cluster, and separate result, state and broker resources. After sourcing any environment file, check the effective context, namespace, deployment name and operator watch scope. Inspect the rendered manifest and run `kubectl -n "$ns" apply --dry-run=server -f "$manifest"` against that scope. The old Figure 10 helper deletes a deployment named `flink`. Check the target of any delete, broker reset or finalizer operation before invoking a wrapper.
3. **Verify startup and let the operator scale.** Record the manifest, revisions, applied starting profiles and placement. Wait for the Flink job and expected source subtasks to be running and source metrics to be available before releasing an external producer. Confirm counters advance. For native trials capture the operator's decisions and let it issue the resize. Check which hosts actually run the join subtasks.
4. **Collect evidence before teardown.** Save timed raw samples, controller decisions, checkpoint IDs, attempt/worker rosters, job and operator logs and the exit receipt under a fresh `outputs/<run-id>/`. Stop at the 600-second cutoff and report any pending restore with the target and actual running subtask count. Export while endpoints are live, clean up only verified run-owned resources, and record the cleanup result. Exclude credentials and kubeconfigs from archives.

## Decide what the run demonstrated

- Use identical workload/input, starting profiles, runtime, host resources, storage and measurement windows across comparison arms except for the intended change. For stateful recovery, match checkpoint contents and input offsets. A fresh deployment skips much of the work of restoring a populated job.
- Time the scaling decision, acknowledgement, last target subtask reaching `RUNNING`, and first resumed processing separately. Record whether the learned policy was selected, held, or fell back to ordinary throughput scaling.
- Score Q8 using join-input counter deltas over compatible task attempts. Report window length, coverage, backlog change and time spent recovering. Distinguish offered rate, source meter, processed rate and sustained throughput. A discard sink requires a separate output oracle for a correctness claim.
- Render with the supplied Python Matplotlib script and its comparison mode. For Figure 10 use the original five-second samples, retaining zeros, gaps and recovery ramps. Return the plot beside its evidence directory. Label managed-memory capacity, actual use and per-vertex requests explicitly. [Figure 10 renderer](outputs/q8-elastic-memory-80k-20261120/experiment-renderer/scripts/experiements/plot_run.py)


# Flink Benchmarking

- Before deploying or comparing benchmarks, read [Experiment Flow](docs/experiment-flow.md). It covers lab access through fs.csl, workload isolation, and the shared measurement procedure.
- Start with the [Nexmark Q8 DataStream reference](docs/experiment-flow.md#reference-benchmark-nexmark-q8-datastream) for the 80k comparison. Use the selected experiment's runbook for its input, deployment, and cleanup procedure.
- Verify the Kubernetes context, namespace ownership, worker capacity, and artifact identities before deployment. Keep SSH material, kubeconfigs, and registry credentials out of the repository and evidence archives.
