import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Read-only offset sampler for the dedicated Q20 broker and its Flink consumer group. */
public final class NativeRescaleKafkaOffsets {
    private static final long INTERVAL_MS = 5_000;
    private static final List<String> TOPICS =
            List.of("nexmark-auction", "nexmark-bid", "nexmark-person");

    public static void main(String[] args) throws Exception {
        long start = Long.parseLong(args[0]);
        long cutoff = Long.parseLong(args[1]);
        Map<TopicPartition, OffsetSpec> requested = new HashMap<>();
        List<TopicPartition> ordered = new ArrayList<>();
        for (String topic : TOPICS) {
            for (int partition = 0; partition < 24; partition++) {
                TopicPartition key = new TopicPartition(topic, partition);
                ordered.add(key);
                requested.put(key, OffsetSpec.latest());
            }
        }

        Properties configuration = new Properties();
        configuration.setProperty("bootstrap.servers", "kafka-external:9092");
        configuration.setProperty("client.id", "native-rescale-read-only-offset-sampler");
        configuration.setProperty("request.timeout.ms", "3000");
        configuration.setProperty("default.api.timeout.ms", "3000");
        try (Admin admin = Admin.create(configuration)) {
            long scheduled = start;
            while (scheduled < cutoff) {
                Thread.sleep(Math.max(0, scheduled - System.currentTimeMillis()));
                long before = System.currentTimeMillis();
                if (before >= cutoff) {
                    break;
                }
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = null;
                Map<TopicPartition, OffsetAndMetadata> committed = null;
                var endsFuture = admin.listOffsets(requested).all();
                var groupFuture =
                        admin.listConsumerGroupOffsets("nexmark").partitionsToOffsetAndMetadata();
                try {
                    ends = endsFuture.get(3, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException error) {
                    System.err.println("Log-end sample failed at " + before + " " + error);
                }
                try {
                    committed = groupFuture.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException error) {
                    System.err.println("Group sample failed at " + before + " " + error);
                }
                long after = System.currentTimeMillis();
                StringBuilder rows = new StringBuilder();
                for (TopicPartition partition : ordered) {
                    if (rows.length() > 0) {
                        rows.append(',');
                    }
                    var end = ends == null ? null : ends.get(partition);
                    var offset = committed == null ? null : committed.get(partition);
                    rows.append("{\"topic\":\"").append(partition.topic())
                            .append("\",\"partition\":").append(partition.partition())
                            .append(",\"end\":").append(end == null ? null : end.offset())
                            .append(",\"committed\":").append(offset == null ? null : offset.offset())
                            .append('}');
                }
                System.out.printf(
                        "{\"scheduled_ms\":%d,\"before_ms\":%d,\"after_ms\":%d,\"partitions\":[%s]}%n",
                        scheduled, before, after, rows);
                scheduled = start + INTERVAL_MS * (Math.max(0, after - start) / INTERVAL_MS + 1);
            }
            admin.close(Duration.ofSeconds(1));
        }
    }
}
