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
    private static final int BIN_MILLIS = 10;
    private static final int OVERFLOW_MILLIS = 120_000;

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
            latency = new Histogram(getRuntimeContext().getAttemptNumber());
            getRuntimeContext().getMetricGroup().gauge(METRIC, (Gauge<String>) latency::snapshot);
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
        private final long[] buckets = new long[OVERFLOW_MILLIS / BIN_MILLIS + 1];
        private long invalid;

        Histogram(int attempt) {
            this.attempt = attempt;
        }

        synchronized void record(long elapsedMillis) {
            if (elapsedMillis < 0) {
                invalid++;
            } else {
                int bin = (int) (Math.min(elapsedMillis, OVERFLOW_MILLIS) / BIN_MILLIS);
                buckets[bin]++;
            }
        }

        synchronized String snapshot() {
            var json = new StringBuilder().append("{\"attempt\":").append(attempt)
                    .append(",\"binMs\":").append(BIN_MILLIS)
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
    }
}
