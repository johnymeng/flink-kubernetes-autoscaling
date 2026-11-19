package be.uclouvain.gepiciad.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

/** Replays a sealed, partitioned Nexmark input while preserving offsets across Q8 join resizes. */
public abstract class Q8ReplaySource<T> extends RichParallelSourceFunction<T>
        implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Q8ReplaySource.class);
    private final Q8InputPlan plan;
    private transient GeneratorConfig generator;
    private transient ListState<String> state;
    private transient TreeMap<Long, Long> pendingCheckpoints;
    private transient int partition;
    private transient int partitions;
    private volatile boolean running = true;
    private volatile long nextOffset;
    private volatile long checkpointedOffset;
    private long restoredOffset;
    private long restoredCheckpointId = -1;
    private long lastOffered;

    Q8ReplaySource(String stages, long releaseMillis, boolean persons) {
        plan = new Q8InputPlan(stages, releaseMillis, persons);
    }

    public static final class Persons extends Q8ReplaySource<Person> {
        public Persons(String stages, long releaseMillis) {
            super(stages, releaseMillis, true);
        }

        @Override
        Person generate(long index, long id, long timestamp, GeneratorConfig config) {
            return PersonGenerator.nextPerson(id, new Random(id), timestamp, config);
        }
    }

    public static final class Auctions extends Q8ReplaySource<Auction> {
        public Auctions(String stages, long releaseMillis) {
            super(stages, releaseMillis, false);
        }

        @Override
        Auction generate(long index, long id, long timestamp, GeneratorConfig config) {
            return AuctionGenerator.nextAuction(index, id, new Random(id), timestamp, config);
        }
    }

    abstract T generate(long index, long id, long timestamp, GeneratorConfig config);

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        partition = getRuntimeContext().getIndexOfThisSubtask();
        partitions = getRuntimeContext().getNumberOfParallelSubtasks();
        generator = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
        state = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("q8-replay-offset-v1", String.class));
        pendingCheckpoints = new TreeMap<>();
        if (context.isRestored()) {
            java.util.Iterator<String> entries = state.get().iterator();
            String prefix = statePrefix();
            if (!entries.hasNext()) {
                throw new IllegalStateException("Q8 restore has no checkpointed source offset");
            }
            String entry = entries.next();
            if (!entry.startsWith(prefix) || entries.hasNext()) {
                throw new IllegalStateException("Q8 restore must keep input plan and source partition assignment");
            }
            restoredOffset = Long.parseLong(entry.substring(prefix.length()));
            if (restoredOffset < 0 || restoredOffset > plan.endOffset) {
                throw new IllegalStateException("Q8 checkpoint offset outside sealed input");
            }
            nextOffset = restoredOffset;
            checkpointedOffset = restoredOffset;
            lastOffered = restoredOffset;
            restoredCheckpointId = context.getRestoredCheckpointId().getAsLong();
        }
        logOffset(context.isRestored() ? "restore" : "start", restoredCheckpointId, nextOffset);
    }

    private void logOffset(String action, long checkpointId, long offset) {
        LOG.info("Q8_INPUT_OFFSET action={} source={} partition={} parallelism={} checkpoint={} next={} end={}",
                action, getClass().getSimpleName(), partition, partitions, checkpointId, offset, plan.endOffset);
    }

    private String statePrefix() {
        return getClass().getSimpleName() + "/" + plan.definition + "/" + plan.releaseMillis
                + "/" + partitions + "/" + partition + "/";
    }

    @Override
    public void open(Configuration configuration) {
        getRuntimeContext().getMetricGroup().gauge("q8NextOffset", () -> nextOffset);
        getRuntimeContext().getMetricGroup().gauge("q8RestoredOffset", () -> restoredOffset);
        getRuntimeContext().getMetricGroup().gauge("q8RestoredCheckpointId", () -> restoredCheckpointId);
        getRuntimeContext().getMetricGroup().gauge("q8CheckpointedOffset", () -> checkpointedOffset);
        getRuntimeContext().getMetricGroup().gauge("q8InputEnd", () -> plan.endOffset);
        getRuntimeContext().getMetricGroup().gauge("q8Offered", this::offered);
        getRuntimeContext().getMetricGroup().gauge("q8Backlog", () -> Math.max(0L, offered() - nextOffset));
    }

    private synchronized long offered() {
        lastOffered = Math.max(lastOffered, plan.offered(System.currentTimeMillis()));
        return lastOffered;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (running && nextOffset < plan.endOffset) {
            long available = offered();
            if (nextOffset >= available) {
                Thread.sleep(10);
                continue;
            }
            while (running && nextOffset < available) {
                long index = Math.addExact(Math.multiplyExact(nextOffset, partitions), partition);
                long id = generator.firstEventId + generator.nextAdjustedEventNumber(index);
                long timestamp = generator.timestampAndInterEventDelayUsForEvent(
                        generator.nextEventNumber(index)).getKey();
                T event = generate(index, id, timestamp, generator);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                    nextOffset++;
                }
            }
        }
        if (nextOffset == plan.endOffset) {
            logOffset("end", -1, nextOffset);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flink takes source snapshots under the same checkpoint lock as collect + advance.
        state.update(Collections.singletonList(statePrefix() + nextOffset));
        pendingCheckpoints.put(context.getCheckpointId(), nextOffset);
        logOffset("snapshot", context.getCheckpointId(), nextOffset);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Long offset = pendingCheckpoints.remove(checkpointId);
        if (offset != null) {
            checkpointedOffset = Math.max(checkpointedOffset, offset);
            logOffset("complete", checkpointId, offset);
        }
        pendingCheckpoints.headMap(checkpointId).clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        pendingCheckpoints.remove(checkpointId);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
