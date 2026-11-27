/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class LocalAdaptiveA4sKafkaSmoke implements LocalAdaptiveA4sSmoke.Workload {
    private static final int INPUT_PARTITIONS = 2;

    private final String broker;
    private final Path evidence;
    private final String group = "a4s-checkpoint-" + UUID.randomUUID();
    private final String input = group + "-input";
    private final String output = group + "-output";

    private LocalAdaptiveA4sKafkaSmoke(String broker, Path evidence) {
        this.broker = broker;
        this.evidence = evidence;
    }

    public static void main(String[] args) throws Exception {
        var evidence = Path.of(args[1]);
        var workload = new LocalAdaptiveA4sKafkaSmoke(args[2], evidence);
        workload.createTopics();
        LocalAdaptiveA4sSmoke.run(args[0], evidence, workload);
    }

    private void createTopics() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", broker))) {
            admin.createTopics(
                            List.of(
                                    new NewTopic(input, INPUT_PARTITIONS, (short) 1),
                                    new NewTopic(output, 1, (short) 1)))
                    .all()
                    .get(20, TimeUnit.SECONDS);
        }
    }

    @Override
    public DataStream<Long> source(StreamExecutionEnvironment env) {
        var source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(broker)
                        .setGroupId(group)
                        .setTopics(input)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setProperty("commit.offsets.on.checkpoint", "true")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "deterministic-source")
                .slotSharingGroup("source")
                .map(Long::parseLong);
    }

    @Override
    public void sink(SingleOutputStreamOperator<Tuple2<Long, Long>> counts) {
        var sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(broker)
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix(group)
                        .setProperty("transaction.timeout.ms", "60000")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic(output)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();
        counts.map(result -> result.f0 + "," + result.f1)
                .sinkTo(sink)
                .name("committed-counts")
                .slotSharingGroup("stateful");
    }

    @Override
    public void publish(int first, int last) throws Exception {
        try (var producer =
                new KafkaProducer<>(
                        Map.of("bootstrap.servers", broker),
                        new StringSerializer(),
                        new StringSerializer())) {
            var writes = new ArrayList<Future<RecordMetadata>>();
            for (int id = first; id <= last; id++) {
                // Keep a key on one partition so its keyed counts have a defined order.
                writes.add(
                        producer.send(
                                new ProducerRecord<>(
                                        input, id % INPUT_PARTITIONS, null, Integer.toString(id))));
            }
            for (var write : writes) {
                write.get(15, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void verifyOffsets(int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        var partitions = List.of(new TopicPartition(input, 0), new TopicPartition(input, 1));
        int expectedPerPartition = expected / INPUT_PARTITIONS;
        try (var admin = Admin.create(Map.of("bootstrap.servers", broker))) {
            while (System.nanoTime() - deadline < 0) {
                var committed =
                        admin.listConsumerGroupOffsets(group)
                                .partitionsToOffsetAndMetadata()
                                .get(5, TimeUnit.SECONDS);
                if (partitions.stream()
                        .allMatch(
                                partition ->
                                        committed.containsKey(partition)
                                                && committed.get(partition).offset()
                                                        == expectedPerPartition)) {
                    System.out.println(
                            "Kafka checkpoint group offset on each input partition: "
                                    + expectedPerPartition);
                    return;
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError(
                "Kafka group did not commit offset "
                        + expectedPerPartition
                        + " on both partitions");
    }

    @Override
    public void awaitResults(int expected) {
        try (var consumer = outputConsumer("read_committed")) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
            int count = 0;
            while (count < expected && System.nanoTime() - deadline < 0) {
                count += consumer.poll(Duration.ofMillis(200)).count();
            }
            if (count != expected) {
                throw new AssertionError(
                        "Expected " + expected + " committed results, found " + count);
            }
        }
    }

    @Override
    public Iterable<Tuple2<Long, Long>> results() throws Exception {
        var partition = new TopicPartition(output, 0);
        var result = new ArrayList<Tuple2<Long, Long>>();
        var records = new ArrayList<String>();
        records.add("kafka_offset,id,keyed_count");
        long end;
        try (var uncommitted = outputConsumer("read_uncommitted")) {
            end = uncommitted.endOffsets(List.of(partition), Duration.ofSeconds(10)).get(partition);
        }
        try (var consumer = outputConsumer("read_committed")) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (consumer.position(partition, Duration.ofSeconds(5)) < end) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new AssertionError(
                            "Did not scan all committed records through offset " + end);
                }
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() < end) {
                        String[] fields = record.value().split(",", -1);
                        if (fields.length != 2) {
                            throw new AssertionError(
                                    "Malformed output at Kafka offset " + record.offset());
                        }
                        records.add(record.offset() + "," + record.value());
                        result.add(Tuple2.of(Long.parseLong(fields[0]), Long.parseLong(fields[1])));
                    }
                }
            }
        }
        Files.write(evidence.resolve("committed-output.csv"), records);
        return result;
    }

    private KafkaConsumer<String, String> outputConsumer(String isolation) {
        var consumer =
                new KafkaConsumer<>(
                        Map.of(
                                "bootstrap.servers",
                                broker,
                                "isolation.level",
                                isolation,
                                "enable.auto.commit",
                                "false"),
                        new StringDeserializer(),
                        new StringDeserializer());
        var partition = new TopicPartition(output, 0);
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        return consumer;
    }
}
