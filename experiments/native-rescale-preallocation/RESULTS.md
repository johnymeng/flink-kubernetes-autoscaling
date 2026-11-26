# Matched Q8 results on c167

Six 600-second runs used the same Flink and operator build from `3015abd8`, Q8 workload, TaskManager geometry, slot profiles, checkpoints, input plan, and c167 placement. Each fixed profile included its full CPU and memory fields. Both arms used a 15-second timeout and fallback action. The pre-allocation flag was the experimental difference. The maximum was four TaskManagers and 16 physical slots, including the four source allocations.

The arm order was enabled/immediate, immediate/enabled, and enabled/immediate. The offered input was 80,000 records/s. The observed source-rate sample means ranged from 8.2–10.1k records/s with immediate restart and 7.7–15.9k records/s with preparation. No run sustained the offered rate. All 18 timed decisions reached their target, with no preparation timeout in the measured windows.

| Transition | Immediate median processing gap | Enabled median processing gap | Paired median change and range | Paired median change in decision-to-running |
| --- | ---: | ---: | ---: | ---: |
| P1 → P2 | 15.0 s | 10.8 s | −2.3 s [−4.2, +1.8] | −1.0 s |
| P2 → P12 | 152.0 s | 122.0 s | −3.0 s [−81.0, +55.0] | +8.5 s |
| P12 → P4 | 72.0 s | 72.0 s | +3.0 s [−0.002, +34.0] | +4.0 s |

Changes are enabled minus immediate. The median paired change is computed from the three individual differences. The arm medians summarize each arm separately. The observed processing gap uses the last positive interval on the old join attempts and the first positive interval on the replacement attempts. The independent restart-to-observed-record upper bound for P2 → P12 improved by a paired median of 3.1 s, ranging from a 79.9 s gain to a 54.9 s regression.

## P2 → P12 lifecycle

In all three enabled arms the target became fully assignable before the graph stopped. The old join processed another 207,764, 218,639, and 32,778 records during the observed preparation intervals. The scheduler retained six occupied allocations, acquired the additional ten slots, and started two new TaskManagers. It spent 9.86–10.10 s preparing, with a 10.03 s median.

| Phase | Immediate median | Enabled median | Paired median change |
| --- | ---: | ---: | ---: |
| Preparation | — | 10.034 s | — |
| Old task termination | 1.250 s | 0.876 s | −0.067 s |
| Wait after termination before graph creation was requested | 8.740 s | 0.004 s | −8.737 s |
| Graph construction | 0.046 s | 0.010 s | −0.039 s |
| JobManager checkpoint restore | 0.005 s | 0.005 s | −0.002 s |
| From JobManager checkpoint restore to all tasks running | 136.088 s | 117.980 s | +5.778 s |
| Peak DataStream pending records | 31.23 M | 31.12 M | −0.194 M |

Most of the elapsed time was worker-side task startup and initialization. The paired difference in that stage ranged from −75.67 to +64.39 s. The coordinator's restore timestamp is a separate milestone. For example, all 12 join attempts in the first pair supplied both `INITIALIZING` and `RUNNING` timestamps. The slowest initialization took 145.31 s with immediate restart and 64.76 s with preparation. The available traces do not isolate that difference to a particular RocksDB or storage operation.

The direct gain was the removed slot wait. Preparation used a median additional 55.86 scheduled CPU-core-seconds and 30.27 GiB-seconds above the old allocation during P2 → P12. It moved that work into a period when the old job still processed records. The measured median reduction in the paired processing-gap percentage was 1.97%. The three results do not establish a repeatable 25% end-to-end reduction or a case to enable the feature by default.

## Coverage and limits

The old graph and its source counters were observed inside each P2 → P12 preparation. Checkpoint restore and resumed source positions were visible in the JobManager and TaskManager logs. The Q8 discard sink does not provide an independent output oracle. No run satisfied the observed sustained-throughput recovery or backlog-clearance rules. The pending-record series represents DataStream generator backlog. Independent Kafka arrivals, Kafka lag, container utilization, and end-to-end latency percentiles were unavailable for these six runs. TaskManager JVM metrics and scheduled Pod requests were recorded.

One preceding attempt was stopped after the startup supervisor did not receive the new launcher’s admission receipt. It made no timed decisions, and its namespace and storage cleanup succeeded. The six measured runs exclude that aborted setup and all six completed their scheduled windows.

| Pair | Immediate | Enabled |
| --- | --- | --- |
| 1 | `live-a38eff47` | `live-bb56a5bc` |
| 2 | `live-b8028252` | `live-44b52b00` |
| 3 | `live-fa4ba39e` | `live-622c40fa` |

[Machine-readable paired measurements](evidence/q8-three-pair-summary.json) · [Pair 2 stage timeline](evidence/q8-pair-2-stages.png) · [Protocol and scoring](README.md)

## Controlled Q8 lifecycle verification

A separate 600-second run at 8,000 offered records/s exercised lifecycle cases under the same four-TaskManager, 16-slot envelope and the defer policy. P1 → P2, P2 → P4, and P4 → P8 applied. While P12 was preparing on the P8 graph, P10 superseded it. A run-owned TaskManager was removed while P10 was preparing. Native recovery applied P10 without a restart for the obsolete P12 generation.

A 16 MiB increase in the join's managed memory at P10 went through the existing fallback path and the new profile was confirmed. After P10 → P4, a P24 request exceeded the configured slot cap and was deferred. The P4 attempts did not restart and processed another 4,552,317 observed join records over the subsequent 453 seconds. Both namespace and storage cleanup completed. [Controlled verification trace](evidence/q8-controlled-verification.json)

## End-to-end automatic A4S

Automatic A4S ran for a separate 600-second window with the existing policy and the same 16-slot cap. It issued two timed requests. A memory-only native fallback reduced the join from 158 MiB to 33 MiB at P1. The runtime reported the actual allocation and the autoscaler persisted `APPLIED`.

The second request targeted a P24 join, requiring 28 slots with the source tasks. It used the normal native path without preparation and remained `COMMITTING` at cutoff while the job waited for resources. That request was acknowledged and had no recorded restart completion. The experiment kept its original cap and included the incomplete outcome. [Automatic verification trace](evidence/q8-automatic-verification.json)

## Exploratory state-heavy SQL Q20 pair

Q20 was selected as the representative SQL follow-up. Two additional 600-second windows ran on c167 with the same runtime and operator build, matching SQL libraries, concrete seed profiles, and exactly-once checkpoint configuration. Each used a fresh isolated broker and one continuous producer on c182. It offered 60,000 mixed Nexmark events/s. The terminal Kafka log-end offsets summed to 35.41 million and 35.48 million respectively. The producer submitted one job per arm and its Pod did not restart.

| Transition | Immediate processing gap | Enabled processing gap | Decision to all tasks running, immediate → enabled |
| --- | ---: | ---: | ---: |
| P1 → P2 | 7.00 s | 7.82 s | 2.04 → 4.87 s |
| P2 → P4 | 76.02 s | 36.00 s | 70.85 → 43.36 s |

P2 → P4 acquired one additional TaskManager. The old join processed another 201,974 observed records during the 10.097-second preparation window, and all 13 target slots were ready before restart. The post-termination wait before graph construction was 9.164 s with immediate restart and 0.006 s with preparation. Extra scheduled requests during preparation were 29.36 CPU-core-seconds and 22.02 GiB-seconds above the old allocation.

Both arms restored a checkpoint and resumed the same topics. Observed sums over all 24 partitions of each consumed topic were positive after redeployment and subsequently increased. The producer and broker ran through the native changes with their original identities. Initial and final broker offsets, consumer positions, and the Flink partition offset series are included in the raw evidence. The immediate arm's offset sampler resumed with batched queries partway through the window. Its initial run of the sampler returned errors and is retained in the archive. The lag snapshots are terminal observations. This pair cannot establish a reproducible reduction across SQL workloads or state sizes. SQL evidence covers Q20.

[SQL paired observations](evidence/q20-exploratory-pair.json) · [SQL stage timeline](evidence/q20-stages.png)

## Supplemental telemetry

The first Q20 broker-capture attempt, `live-505f2dfb`, produced 120 samples across the 600-second window. There were 119 observed arrival-rate intervals and 109 complete consumer-lag samples. The native decision observer finished just before the launcher cutoff. The launcher's process check returned an error, skipping terminal logs and snapshots. Both cleanup exit codes were zero. The partial evidence is kept outside the paired comparison. The launcher now waits on the observer's actual exit status and preserves the timed window before capturing terminal evidence. [Supplemental telemetry protocol](telemetry/README.md)
