# Supplemental native-rescale measurements

The original validation and throughput runs remain intact. These measurements use separate matched runs. Each pair holds the workload, instrumentation, runtime, operator, input plan, and checkpoints constant while changing the preparation flag.

## Kafka lag and broker arrivals on Q20

Compile `NativeRescaleKafkaOffsets.java` with Java 11 and the lab Kafka client JAR. The admitted launcher copies the compiled class into the owned broker pod and runs `capture-kafka-broker.sh` from the validated run bundle. The sampler uses Kafka Admin reads at five-second intervals. It records all 72 partition log-end offsets, the `nexmark` group's committed offsets, and the sample boundaries. The launcher verifies the broker image, pod ownership, and namespace UID before connecting. No data or group state is written by the probe.

The lag series sums latest minus committed offsets over the bid and auction partitions that Q20 consumes. The arrival series is the observed rate of offset increments over all three produced topics. Log-end increments can include Kafka control records. Committed lag tracks checkpoint-aligned group progress, so a checkpoint can change the lag even when the live consumer advanced earlier. A sample with missing partition or group offsets remains missing. Flink documents the distinction between committed offsets and recovery state. [Kafka source offset monitoring](https://github.com/apache/flink-connector-kafka/blob/main/docs/content/docs/connectors/datastream/kafka.md)

Run `score_broker_offsets.py LAB_RECEIPT RESULTS_ROOT` to convert the broker samples into indexed Figure 10 CSVs and a coverage receipt. Use the supplied renderer with query `q20`. The clock receipt checks the broker worker's wall clock against the control host before timing.

## Auction emission to joined result on Q8

Use the exact Q8 JAR referenced by the original c167 comparison. Its SHA-256 is `8992573104399f86a61bb096a676329e6e447cb060961a842242867077983a9f`. Compile `InstrumentQ8Latency.java` with ASM 9 and compile `Q8RecordLatency.java` against the pinned workload and Flink distribution. The instrumenter inserts a probe immediately before source collection and another immediately after the existing join result is delivered to its chained sink. The launcher verifies that the join and sink are chained on the admitted graph. Only the two call-site class payloads change, and the JAR adds one stateless helper. The source partition, replay and checkpoint logic, event timestamps, join window, and sink stay as in the original workload.

The sampler selects auctions deterministically from a mix of `id`, `dateTime`, `seller`, and `reserve` and uses SHA-256 over the Beam Coder bytes for event identity. Distinct input events can reuse a Nexmark model ID. The fingerprint is stable across replay and avoids using a model ID alone to correlate the two log records. Each sampled result is measured from the first witnessed emission of its auction to the first witnessed result for that auction/person pair. Millisecond clocks are shared because all TaskManagers in this trial are on c167. Each recording includes original pod and generation evidence. The metric covers matched auction inputs and the existing Q8 result path. An auction that produces no witnessed result contributes no latency value.

Run `score_q8_latency.py LAB_RECEIPT RESULTS_ROOT` to report matched and unmatched witnesses, pooled P50/P95/P99, and 30-second time bins aligned to the input release. P50 requires two matches, P95 requires 20, and P99 requires 100. A result with insufficient samples remains missing. Use the supplied Figure 10 renderer with query `q8`. The logging probe is applied identically to every latency arm and does not provide an exactly-once assertion about the discard sink.

Run the adapter regression checks with `python3 -m unittest discover -s experiments/native-rescale-preallocation/telemetry -v`.
