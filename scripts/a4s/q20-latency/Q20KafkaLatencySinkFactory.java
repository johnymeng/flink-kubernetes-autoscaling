package io.a4s.benchmark.q20;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Set;

/** Discards Q20 output after sampling latency from the Kafka bid timestamp. */
public final class Q20KafkaLatencySinkFactory implements DynamicTableSinkFactory {
    private static final String METRIC = "q20KafkaLatencyV2";
    private static final int SHORT_END_MILLIS = 2_000;
    private static final int MEDIUM_END_MILLIS = 60_000;
    private static final int OVERFLOW_MILLIS = 900_000;
    private static final int SHORT_WIDTH_MILLIS = 10;
    private static final int MEDIUM_WIDTH_MILLIS = 100;
    private static final int LONG_WIDTH_MILLIS = 1_000;
    private static final int MEDIUM_START_BIN = SHORT_END_MILLIS / SHORT_WIDTH_MILLIS;
    private static final int LONG_START_BIN = MEDIUM_START_BIN
            + (MEDIUM_END_MILLIS - SHORT_END_MILLIS) / MEDIUM_WIDTH_MILLIS;
    // Even with every bin present and 19-digit counts, the gauge stays below writeUTF's limit.
    private static final int OVERFLOW_BIN = LONG_START_BIN
            + (OVERFLOW_MILLIS - MEDIUM_END_MILLIS) / LONG_WIDTH_MILLIS;
    private static final String TIMESTAMP_FIELD = "bid_kafka_timestamp";
    private static final ConfigOption<Integer> SAMPLE_EVERY =
            ConfigOptions.key("sample-every").intType().defaultValue(1024);

    @Override
    public String factoryIdentifier() {
        return "a4s-q20-latency";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(SAMPLE_EVERY);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        var helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        int sampleEvery = helper.getOptions().get(SAMPLE_EVERY);
        DataType row = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        int timestamp = DataType.getFieldNames(row).indexOf(TIMESTAMP_FIELD);
        if (sampleEvery < 1 || timestamp < 0 || !row.getChildren().get(timestamp).getLogicalType()
                .copy(true).equals(DataTypes.TIMESTAMP_LTZ(3).getLogicalType())) {
            throw new IllegalArgumentException(
                    "Q20 latency requires positive sample-every and bid_kafka_timestamp TIMESTAMP_LTZ(3)");
        }
        return new LatencyTableSink(timestamp, sampleEvery);
    }

    private static final class LatencyTableSink implements DynamicTableSink {
        private final int timestamp;
        private final int sampleEvery;

        private LatencyTableSink(int timestamp, int sampleEvery) {
            this.timestamp = timestamp;
            this.sampleEvery = sampleEvery;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            var mode = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    mode.addContainedKind(kind);
                }
            }
            return mode.build();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkFunctionProvider.of(new LatencySink(timestamp, sampleEvery));
        }

        @Override
        public DynamicTableSink copy() {
            return new LatencyTableSink(timestamp, sampleEvery);
        }

        @Override
        public String asSummaryString() {
            return "Q20KafkaLatency";
        }
    }

    private static final class LatencySink extends RichSinkFunction<RowData> {
        private static final long serialVersionUID = 1L;
        private final int timestamp;
        private final int sampleEvery;
        private transient long records;
        private transient long invalid;
        private transient long[] buckets;
        private transient int attempt;

        private LatencySink(int timestamp, int sampleEvery) {
            this.timestamp = timestamp;
            this.sampleEvery = sampleEvery;
        }

        @Override
        public void open(Configuration parameters) {
            buckets = new long[OVERFLOW_BIN + 1];
            attempt = getRuntimeContext().getAttemptNumber();
            getRuntimeContext().getMetricGroup().gauge(METRIC, (Gauge<String>) this::snapshot);
        }

        @Override
        public void invoke(RowData row, Context context) {
            RowKind kind = row.getRowKind();
            if ((kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) && records++ % sampleEvery == 0) {
                long received = row.isNullAt(timestamp) ? 0 : row.getTimestamp(timestamp, 3).getMillisecond();
                record(received, System.currentTimeMillis());
            }
        }

        private synchronized void record(long received, long now) {
            if (received <= 0 || received > now) {
                invalid++;
                return;
            }
            buckets[bin(now - received)]++;
        }

        private synchronized String snapshot() {
            var json = new StringBuilder().append("{\"attempt\":").append(attempt)
                    .append(",\"sampledAtMs\":").append(System.currentTimeMillis())
                    .append(",\"segments\":[[").append(SHORT_END_MILLIS).append(',').append(SHORT_WIDTH_MILLIS)
                    .append("],[").append(MEDIUM_END_MILLIS).append(',').append(MEDIUM_WIDTH_MILLIS)
                    .append("],[").append(OVERFLOW_MILLIS).append(',').append(LONG_WIDTH_MILLIS).append("]]")
                    .append(",\"overflowAtMs\":").append(OVERFLOW_MILLIS)
                    .append(",\"invalid\":").append(invalid).append(",\"counts\":{");
            String delimiter = "";
            for (int bin = 0; bin < buckets.length; bin++) {
                if (buckets[bin] > 0) {
                    json.append(delimiter).append('"').append(bin).append("\":").append(buckets[bin]);
                    delimiter = ",";
                }
            }
            return json.append("}}").toString();
        }

        private static int bin(long elapsed) {
            if (elapsed < SHORT_END_MILLIS) {
                return (int) (elapsed / SHORT_WIDTH_MILLIS);
            }
            if (elapsed < MEDIUM_END_MILLIS) {
                return MEDIUM_START_BIN + (int) ((elapsed - SHORT_END_MILLIS) / MEDIUM_WIDTH_MILLIS);
            }
            return LONG_START_BIN + (int) ((Math.min(elapsed, OVERFLOW_MILLIS) - MEDIUM_END_MILLIS)
                    / LONG_WIDTH_MILLIS);
        }
    }
}
