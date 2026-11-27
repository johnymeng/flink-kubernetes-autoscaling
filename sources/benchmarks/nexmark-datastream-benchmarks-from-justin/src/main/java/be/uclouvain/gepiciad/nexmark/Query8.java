package be.uclouvain.gepiciad.nexmark;


import be.uclouvain.gepiciad.sources.AuctionSourceFunction;
import be.uclouvain.gepiciad.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;

public class Query8 {

    private static final Logger logger  = LoggerFactory.getLogger(Query8.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));



        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        int latencySampleEvery = params.getInt("q8-latency-sample-every", 0);
        if (latencySampleEvery < 0) {
            throw new IllegalArgumentException("Q8 latency sampling stride must be nonnegative");
        }
        if (params.getBoolean("disabledOperatorChaining", false)) {
            env.disableOperatorChaining();
        }
        if (latencySampleEvery > 0 && !env.isChainingEnabled()) {
            throw new IllegalArgumentException("Q8 latency sampling requires operator chaining");
        }

        final int auctionSrcRate = params.getInt("auction-srcRate", 50000);

        final int personSrcRate = params.getInt("person-srcRate", 30000);
        final boolean exclusiveSlots = params.getBoolean("a4s-exclusive-slots", false);

        env.setParallelism(params.getInt("p-window", 1));

        logger.debug(String.valueOf(params.getInt("auction-srcRate")));

        SingleOutputStreamOperator<Person> personSource = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1));
        if (exclusiveSlots) {
            personSource.slotSharingGroup("q8-person");
        }
        DataStream<Person> persons = personSource.assignTimestampsAndWatermarks(new PersonTimestampAssigner());

        SingleOutputStreamOperator<Auction> auctionSource = env.addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1));
        if (exclusiveSlots) {
            auctionSource.slotSharingGroup("q8-auction");
        }
        DataStream<Auction> auctions = auctionSource.assignTimestampsAndWatermarks(new AuctionTimestampAssigner());

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        int windowSeconds = params.getInt("window-size", 10);
        if (latencySampleEvery == 0) {
            join(persons, auctions, auction -> auction.seller,
                    new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                        @Override
                        public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                            out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                        }
                    }, windowSeconds, exclusiveSlots)
                    .addSink(new DiscardingSink<>());
        } else {
            var sampled = auctions.map(new Query8Latency.Stamp(latencySampleEvery))
                    .name("Q8 auction latency sampling")
                    .setParallelism(auctionSource.getParallelism());
            if (exclusiveSlots) {
                sampled.slotSharingGroup("q8-auction");
            }
            join(persons, sampled, record -> record.f0.seller, new Query8Latency.Join(),
                    windowSeconds, exclusiveSlots)
                    .addSink(new Query8Latency.Sink())
                    .name("Q8 latency sink");
        }

        env.execute("Nexmark Query8");
    }

    private static <A, R> DataStream<R> join(
            DataStream<Person> persons, DataStream<A> auctions, KeySelector<A, Long> seller,
            FlatJoinFunction<Person, A, R> output, int windowSeconds, boolean exclusiveSlots) {
        KeySelector<Person, Long> personId = person -> person.id;
        DataStream<R> joined = persons.join(auctions)
                .where(personId)
                .equalTo(seller)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .apply(output);
        if (exclusiveSlots) {
            joined.getTransformation().setSlotSharingGroup("q8-join");
        }
        return joined;
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Person element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Auction> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Auction element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }
}
