package be.uclouvain.gepiciad.nexmark;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.nio.file.Files;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import javax.annotation.Nullable;

public class Query8CheckpointOracle {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));



        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        if (params.getBoolean("disabledOperatorChaining", false)) {
            env.disableOperatorChaining();
        }
        // enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(5000);

        FixturePlan fixture = FixturePlan.from(params);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(params.getRequired("checkpoint-dir"));

        env.setParallelism(params.getInt("p-window", 1));

        DataStream<Person> persons = env.addSource(new FrozenPersons(fixture))
                .name("Custom Source: Persons")
                .setParallelism(2)
                .assignTimestampsAndWatermarks(new PersonTimestampAssigner());

        DataStream<Auction> auctions = env.addSource(new FrozenAuctions(fixture))
                .name("Custom Source: Auctions")
                .setParallelism(2)
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner());

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                persons.join(auctions)
                        .where(new KeySelector<Person, Long>() {
                            @Override
                            public Long getKey(Person p) {
                                return p.id;
                            }
                        }).equalTo(new KeySelector<Auction, Long>() {
                            @Override
                            public Long getKey(Auction a) {
                                return a.seller;
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Time.seconds(params.getInt("window-size", 10))))
                        .apply(new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                            @Override
                            public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                                out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                            }
                        });


        joined.process(new WindowResults())
                .addSink(StreamingFileSink
                        .forRowFormat(new Path(params.getRequired("output-dir")),
                                new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .build())
                .name("Checkpoint-committed Q8 results");
        env.execute("Nexmark Query8 transition oracle");
    }

    private static final class FixturePlan implements Serializable {
        private static final long serialVersionUID = 1L;
        final String path;
        final String sha256;
        final long cutEventMillis;
        final long prefixReleaseMillis;
        final long suffixReleaseMillis;
        final long deadlineMillis;

        private FixturePlan(ParameterTool params) {
            path = params.getRequired("fixture");
            sha256 = params.getRequired("fixture-sha256");
            cutEventMillis = params.getLong("cut-event-millis");
            prefixReleaseMillis = params.getLong("prefix-release-millis");
            suffixReleaseMillis = params.getLong("suffix-release-millis");
            deadlineMillis = params.getLong("deadline-millis");
            if (!sha256.matches("[a-f0-9]{64}") || prefixReleaseMillis <= 0
                    || suffixReleaseMillis <= prefixReleaseMillis || deadlineMillis <= suffixReleaseMillis) {
                throw new IllegalArgumentException("Invalid sealed Q8 oracle or release schedule");
            }
        }

        static FixturePlan from(ParameterTool params) {
            return new FixturePlan(params);
        }
    }

    private abstract static class FrozenSource<T> extends RichParallelSourceFunction<T>
            implements CheckpointedFunction, CheckpointListener {
        private final FixturePlan plan;
        private final String kind;
        private volatile boolean running = true;
        private volatile long terminalCheckpoint = -1;
        private volatile boolean committed;
        private transient List<String[]> rows;
        private transient ListState<String> cursor;
        private transient String identity;
        private long offset;
        private boolean terminalWatermark;

        FrozenSource(FixturePlan plan, String kind) {
            this.plan = plan;
            this.kind = kind;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            int partition = getRuntimeContext().getIndexOfThisSubtask();
            int partitions = getRuntimeContext().getNumberOfParallelSubtasks();
            identity = kind + "/" + partitions + "/" + partition + "/" + plan.sha256
                    + "/" + plan.cutEventMillis + "/" + plan.prefixReleaseMillis + "/"
                    + plan.suffixReleaseMillis + "/" + plan.deadlineMillis + "/";
            byte[] fixture = Files.readAllBytes(java.nio.file.Path.of(plan.path));
            StringBuilder digest = new StringBuilder();
            for (byte octet : MessageDigest.getInstance("SHA-256").digest(fixture)) {
                digest.append(String.format(Locale.ROOT, "%02x", octet & 0xff));
            }
            if (!digest.toString().equals(plan.sha256)) {
                throw new IllegalArgumentException("Q8 oracle input hash mismatch");
            }
            rows = new ArrayList<>();
            for (String line : new String(fixture, StandardCharsets.UTF_8).split("\\R")) {
                String[] fields = line.split("\t", -1);
                if (fields[0].equals(kind) && Integer.parseInt(fields[1]) == partition) {
                    rows.add(fields);
                }
            }
            cursor = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("q8-oracle-cursor-v1", String.class));
            if (context.isRestored()) {
                var state = cursor.get().iterator();
                if (!state.hasNext()) {
                    throw new IllegalStateException("Q8 oracle restore has no checkpointed cursor");
                }
                String value = state.next();
                if (!value.startsWith(identity) || state.hasNext()) {
                    throw new IllegalStateException("Q8 oracle restore changed source assignment or input");
                }
                offset = Long.parseLong(value.substring(identity.length()));
                if (offset < 0 || offset > rows.size()) {
                    throw new IllegalStateException("Q8 oracle restored an out-of-range cursor");
                }
            }
            logCursor(context.isRestored() ? "restore" : "start",
                    context.getRestoredCheckpointId().orElse(-1), offset);
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            while (running && offset < rows.size()) {
                String[] fields = rows.get((int) offset);
                long timestamp = Long.parseLong(fields[2]);
                long release = timestamp < plan.cutEventMillis
                        ? plan.prefixReleaseMillis : plan.suffixReleaseMillis;
                long now = System.currentTimeMillis();
                if (now >= plan.deadlineMillis) {
                    throw new IllegalStateException("Q8 oracle source deadline reached before completing input");
                }
                if (now < release) {
                    Thread.sleep(Math.min(50, release - now));
                    continue;
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(value(fields));
                    offset++;
                }
            }
            if (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.emitWatermark(Watermark.MAX_WATERMARK);
                    terminalWatermark = true;
                }
            }
            while (running && !committed) {
                if (System.currentTimeMillis() >= plan.deadlineMillis) {
                    throw new IllegalStateException("Q8 oracle reached deadline before its terminal checkpoint");
                }
                Thread.sleep(50);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            cursor.update(Collections.singletonList(identity + offset));
            long checkpoint = context.getCheckpointId();
            if (terminalWatermark && terminalCheckpoint < 0) {
                terminalCheckpoint = checkpoint;
            }
            logCursor("snapshot", checkpoint, offset);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (terminalCheckpoint >= 0 && checkpointId >= terminalCheckpoint) {
                logCursor("complete", checkpointId, offset);
                committed = true;
            }
        }

        private void logCursor(String action, long checkpoint, long position) {
            System.out.printf("Q8_ORACLE_CURSOR action=%s source=%s checkpoint=%d next=%d end=%d%n",
                    action, identity, checkpoint, position, rows.size());
        }

        protected abstract T value(String[] fields);

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class FrozenPersons extends FrozenSource<Person> {
        private FrozenPersons(FixturePlan fixture) {
            super(fixture, "person");
        }

        @Override
        protected Person value(String[] fields) {
            return new Person(
                    Long.parseLong(fields[3]), fields[4], "", "", "", "",
                    Long.parseLong(fields[2]), "");
        }
    }

    private static final class FrozenAuctions extends FrozenSource<Auction> {
        private FrozenAuctions(FixturePlan fixture) {
            super(fixture, "auction");
        }

        @Override
        protected Auction value(String[] fields) {
            long time = Long.parseLong(fields[2]);
            return new Auction(
                    Long.parseLong(fields[3]), "", "", 0, Long.parseLong(fields[5]), time,
                    time + 60000, Long.parseLong(fields[4]), 1, "");
        }
    }

    private static final class WindowResults extends ProcessFunction<Tuple3<Long, String, Long>, String> {
        @Override
        public void processElement(Tuple3<Long, String, Long> value, Context context, Collector<String> out) {
            Long watermarkBoundary = context.timestamp();
            if (watermarkBoundary == null) {
                throw new IllegalStateException("Q8 window output has no event-time boundary");
            }
            out.collect(Math.addExact(watermarkBoundary, 1) + "\t" + value.f0 + "\t" + value.f1 + "\t" + value.f2);
        }
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
