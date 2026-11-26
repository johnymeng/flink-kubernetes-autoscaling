# Nexmark Q8 native rescale experiment

Use the Figure 10 DataStream workload and its supplied Q8 launcher, isolated operator, and Figure 10 renderer. It uses two Person tasks at 8,889 records/s each and two Auction tasks at 31,111 records/s each. The offered load is 80,000 records/s. The experimental unit is a 600-second window on an isolated lab worker.

## Match the arms

- Build one runtime and one operator from the tested revision with the matching custom RocksDB JNI. Use identical digest-pinned runtime, workload, and operator images in both arms. Record SHA-256 hashes of the loaded JARs and native library and the observed pod image IDs.
- Use a fresh virtual cluster and owned storage scope. Verify namespace identity, placement, quotas, worker requests, and artifact identities. Keep checkpointing and input identical. Server dry-run and archive the manifests. Collect live evidence before cleaning up each run.
- Establish dedicated concrete profiles for the four source allocations and each join allocation. If Q8 starts with unspecified groups, seed it with a native request at unchanged parallelism outside the timed window. Use each vertex's confirmed `appliedResourceProfile` as its full requested profile. Wait for the new subtasks to run, verify their profiles, and require a completed checkpoint before timing. Match this warm-up across both arms.
- Use four CPU cores and slots per TaskManager. A P12 join requires 16 slots including the sources. A P24 join requires 28 slots and at least seven workers. Verify physical and namespace capacity for the workers, JobManager, and operator. Retain the experimental quotas and score quota rejections.
- Set `job.autoscaler.scaling.enabled: "false"` for manual arms. Change only `job.autoscaler.native-rescale.preallocation.enabled` from `"false"` to `"true"`. Use the same 15-second timeout and fallback action in both arms.
- Run at least three repetitions per arm and alternate the order across pairs on the same worker. Use equivalent startup state and the same transition schedule. Account for missing data, failures, and incomplete requests at the 600-second cutoff.

Start the initial manual series at join P1 and send P2 at 120 seconds, P12 at 280 seconds, and P4 at 460 seconds. Run a separate verification series for P2 → P4 → P8, P4 → P24 where permitted, vertical reconfiguration, supersession, and TaskManager or slot failure injection in the owned scope. For the automatic A4S series, enable the existing operator policy and omit manual transitions. A profile change from the policy uses the existing restart path.

## Record one arm

Forward the verified JobManager REST service on the lab control host. Supply the actual JobID and vertex IDs from the live job and the workload's sealed input-release epoch. Put the raw JobManager and operator logs, rendered manifests, artifact hashes, pod events, and cleanup receipt beside the output.

```bash
python3 experiments/native-rescale-preallocation/observe.py \
  --rest http://127.0.0.1:8081 --job "$JOB_ID" \
  --source-vertex "$PERSON_VERTEX" --source-vertex "$AUCTION_VERTEX" \
  --stateful-vertex "$JOIN_VERTEX" --revision "$GIT_COMMIT" \
  --preallocation "$PREALLOCATION_ENABLED" --duration 600 --interval 1 \
  --start-epoch "$INPUT_RELEASE_EPOCH" \
  --transition 120:2 --transition 280:12 --transition 460:4 \
  --output "$RUN_EVIDENCE"
```

The observer verifies the effective feature setting and initial profiles before submitting manual targets. It records HTTP outcomes and times, task-attempt counters, workers and slots, requested and applied resources, checkpoints, and missing samples. Use the workload-specific sampler in parallel for other metrics. An uninstrumented DataStream workload cannot establish input backlog, independent source arrivals, or end-to-end latency quantiles. Use instrumented DataStream counters for its source backlog and the Kafka SQL Q8 variant for consumer lag, keeping the same producer and topics through each rescale.

## Score both arms consistently

- Preparation duration spans preparation start to commit. An immediate ready path has no timed preparation interval.
- Decision-to-running spans the decision to the last target task reaching running. Record the acknowledgement separately.
- Execution interruption spans restart start to the first increase in a stateful input or output counter on a replacement attempt. Report sample bounds. Estimate observed downtime using the counter progress gap immediately surrounding the restart.
- Throughput recovery begins with ten consecutive seconds at or above 90% of the median stateful input throughput in the preceding 30-second pre-decision reference window. If samples are insufficient, mark recovery unavailable.
- Backlog clearance spans decision to the first sustained return to the pre-decision backlog band. Report peak backlog and backlog at cutoff. Keep genuine zeros and failed polls distinct.
- Calculate excess allocation by integrating CPU and memory requests over matched offsets. Report core-seconds and GiB-seconds, resource peaks, utilization, and timeout/fallback frequency.

Use the supplied Figure 10 `plot_run.py` with the original indexed sample files and its comparison arguments. Align any supplemental plots to input release and preserve the raw data. Earlier PR #5 timings measured decision-to-running under a different comparison. Estimating the new downtime effect requires the live matched repetitions.
