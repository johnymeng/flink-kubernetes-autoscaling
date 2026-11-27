package be.uclouvain.gepiciad.nexmark;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

/** Opt-in auction emission to joined result latency for the DataStream Q8 benchmark. */
final class Query8Latency {
    static final String METRIC = "q8AuctionLatencyV1";
    private static final String EXTENDED_METRIC = "q8AuctionLatencyV2";
    private static final int BIN_MILLIS = 10;
    private static final int OVERFLOW_MILLIS = 120_000;
    private static final int SHORT_END_MILLIS = 2_000;
    private static final int MEDIUM_END_MILLIS = 60_000;
    private static final int EXTENDED_OVERFLOW_MILLIS = 900_000;
    private static final int MEDIUM_BIN_MILLIS = 100;
    private static final int LONG_BIN_MILLIS = 1_000;
    private static final int MEDIUM_START_BIN = SHORT_END_MILLIS / BIN_MILLIS;
    private static final int LONG_START_BIN = MEDIUM_START_BIN
            + (MEDIUM_END_MILLIS - SHORT_END_MILLIS) / MEDIUM_BIN_MILLIS;
    // 1,621 bins fit in a Flink writeUTF gauge even with 19-digit counts.
    private static final int EXTENDED_OVERFLOW_BIN = LONG_START_BIN
            + (EXTENDED_OVERFLOW_MILLIS - MEDIUM_END_MILLIS) / LONG_BIN_MILLIS;

    private Query8Latency() {}

    static final class Stamp implements MapFunction<Auction, Tuple2<Auction, Long>> {
        private final int sampleEvery;

        Stamp(int sampleEvery) {
            this.sampleEvery = sampleEvery;
        }

        @Override
        public Tuple2<Auction, Long> map(Auction auction) {
            int hash = MathUtils.murmurHash(Long.hashCode(auction.id));
            long emitted = hash % sampleEvery == 0 ? System.currentTimeMillis() : 0L;
            return Tuple2.of(auction, emitted);
        }
    }

    static final class Join implements FlatJoinFunction<
            Person, Tuple2<Auction, Long>, Tuple4<Long, String, Long, Long>> {
        @Override
        public void join(Person person, Tuple2<Auction, Long> auction,
                Collector<Tuple4<Long, String, Long, Long>> out) {
            out.collect(Tuple4.of(person.id, person.name, auction.f0.reserve, auction.f1));
        }
    }

    static final class Sink extends RichSinkFunction<Tuple4<Long, String, Long, Long>> {
        private transient Histogram latency;

        @Override
        public void open(Configuration parameters) {
            boolean extended = Boolean.parseBoolean(System.getenv("FLINK_Q8_LATENCY_EXTENDED"));
            latency = new Histogram(getRuntimeContext().getAttemptNumber(), extended);
            getRuntimeContext().getMetricGroup().gauge(
                    extended ? EXTENDED_METRIC : METRIC, (Gauge<String>) latency::snapshot);
        }

        @Override
        public void invoke(Tuple4<Long, String, Long, Long> result, Context context) {
            if (result.f3 != 0) {
                latency.record(System.currentTimeMillis() - result.f3);
            }
        }
    }

    static final class Histogram {
        private final int attempt;
        private final boolean extended;
        private final long[] buckets;
        private long invalid;

        Histogram(int attempt, boolean extended) {
            this.attempt = attempt;
            this.extended = extended;
            buckets = new long[(extended ? EXTENDED_OVERFLOW_BIN : OVERFLOW_MILLIS / BIN_MILLIS) + 1];
        }

        synchronized void record(long elapsedMillis) {
            if (elapsedMillis < 0) {
                invalid++;
            } else {
                buckets[bin(elapsedMillis)]++;
            }
        }

        private int bin(long elapsedMillis) {
            if (!extended) {
                return (int) (Math.min(elapsedMillis, OVERFLOW_MILLIS) / BIN_MILLIS);
            }
            if (elapsedMillis < SHORT_END_MILLIS) {
                return (int) (elapsedMillis / BIN_MILLIS);
            }
            if (elapsedMillis < MEDIUM_END_MILLIS) {
                return MEDIUM_START_BIN
                        + (int) ((elapsedMillis - SHORT_END_MILLIS) / MEDIUM_BIN_MILLIS);
            }
            return LONG_START_BIN
                    + (int) ((Math.min(elapsedMillis, EXTENDED_OVERFLOW_MILLIS) - MEDIUM_END_MILLIS)
                            / LONG_BIN_MILLIS);
        }

        synchronized String snapshot() {
            var json = new StringBuilder().append("{\"attempt\":").append(attempt)
                    .append(",\"sampledAtMs\":").append(System.currentTimeMillis());
            if (extended) {
                json.append(",\"segments\":[[").append(SHORT_END_MILLIS).append(',').append(BIN_MILLIS)
                        .append("],[").append(MEDIUM_END_MILLIS).append(',').append(MEDIUM_BIN_MILLIS)
                        .append("],[").append(EXTENDED_OVERFLOW_MILLIS).append(',').append(LONG_BIN_MILLIS)
                        .append("]]");
            } else {
                json.append(",\"binMs\":").append(BIN_MILLIS);
            }
            json.append(",\"overflowAtMs\":").append(extended ? EXTENDED_OVERFLOW_MILLIS : OVERFLOW_MILLIS)
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
    }
}
