package be.uclouvain.gepiciad.sources;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Compile against Query8.jar and run with java -ea be.uclouvain.gepiciad.sources.Q8ReplaySmoke. */
public class Q8ReplaySmoke {
    private static final String PLAN = "1:8:7,2:9:11";
    private static final long CHECKPOINT = 17;

    public static void main(String[] args) throws Exception {
        Q8InputPlan plan = new Q8InputPlan(PLAN, 100_000, true);
        check(plan.offered(99_000) == 0 && plan.offered(100_999) == 7, "before first boundary");
        check(plan.offered(101_000) == 8 && plan.offered(101_500) == 12, "second stage");
        check(plan.offered(200_000) == 26 && plan.endOffset == 26, "sealed input end");
        for (int partition = 0; partition < 2; partition++) {
            checkRestore(() -> new Q8ReplaySource.Persons(PLAN, 1), Person.CODER, partition, 26);
            checkRestore(() -> new Q8ReplaySource.Auctions(PLAN, 1), Auction.CODER, partition, 29);
        }
        System.out.println("Q8 release schedule and checkpoint/restore sequences passed for both partitions of both sources");
    }

    private static <T> void checkRestore(Supplier<Q8ReplaySource<T>> factory, Coder<T> coder, int partition, int end)
            throws Exception {
        Q8ReplaySource<T> full = factory.get();
        initialize(full, partition, new ArrayList<>(), false);
        List<T> reference = collect(full, end);
        check(reference.size() == end, "uninterrupted input length");

        Q8ReplaySource<T> first = factory.get();
        List<String> state = new ArrayList<>();
        var activeGauges = initialize(first, partition, state, false);
        checkBacklog(activeGauges, end);
        List<T> replay = collect(first, 6);
        checkBacklog(activeGauges, end - 6);
        FunctionSnapshotContext snapshot = proxy(FunctionSnapshotContext.class, (method, args) -> {
            if (method.equals("getCheckpointId")) return CHECKPOINT;
            throw new AssertionError(method);
        });
        first.snapshotState(snapshot);
        first.notifyCheckpointComplete(CHECKPOINT);
        check(state.size() == 1 && state.get(0).endsWith("/6"), "exclusive saved offset");

        Q8ReplaySource<T> restored = factory.get();
        var restoredGauges = initialize(restored, partition, state, true);
        checkBacklog(restoredGauges, end - 6);
        replay.addAll(collect(restored, end));
        checkBacklog(restoredGauges, 0);
        check(replay.size() == reference.size(), "restored input sequence length");
        for (int i = 0; i < reference.size(); i++) {
            check(Arrays.equals(CoderUtils.encodeToByteArray(coder, replay.get(i)),
                    CoderUtils.encodeToByteArray(coder, reference.get(i))), "restored input bytes at offset " + i);
        }
        try {
            initialize(factory.get(), 1 - partition, state, true);
            throw new AssertionError("restore accepted a different source partition");
        } catch (IllegalStateException expected) {
            check(expected.getMessage().contains("partition"), "unexpected restore error");
        }
    }

    private static void checkBacklog(Map<String, Gauge<?>> gauges, int expected) {
        check(gauges.get("q8Backlog").getValue().equals((long) expected), "replay backlog");
        if ("1".equals(System.getenv("FLINK_Q8_REPORT_PENDING_RECORDS"))) {
            check(gauges.get("pendingRecords").getValue().equals((long) expected), "autoscaler source lag");
        } else {
            check(!gauges.containsKey("pendingRecords"), "unexpected autoscaler lag metric");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Map<String, Gauge<?>> initialize(Q8ReplaySource<T> source, int partition,
                                                        List<String> data, boolean restore) throws Exception {
        var gauges = new HashMap<String, Gauge<?>>();
        var metrics = proxy(OperatorMetricGroup.class, (method, args) -> {
            if (method.equals("gauge")) {
                gauges.put(args[0].toString(), (Gauge<?>) args[1]);
                return args[1];
            }
            throw new AssertionError(method);
        });
        source.setRuntimeContext(proxy(RuntimeContext.class, (method, args) -> {
            if (method.equals("getIndexOfThisSubtask")) return partition;
            if (method.equals("getNumberOfParallelSubtasks")) return 2;
            if (method.equals("getMetricGroup")) return metrics;
            throw new AssertionError(method);
        }));
        ListState<String> state = proxy(ListState.class, (method, args) -> {
            if (method.equals("get")) return new ArrayList<>(data);
            if (method.equals("update")) {
                data.clear();
                data.addAll((List<String>) args[0]);
                return null;
            }
            throw new AssertionError(method);
        });
        OperatorStateStore store = proxy(OperatorStateStore.class, (method, args) -> {
            if (method.equals("getListState")) return state;
            throw new AssertionError(method);
        });
        source.initializeState(proxy(FunctionInitializationContext.class, (method, args) -> {
            if (method.equals("isRestored")) return restore;
            if (method.equals("getRestoredCheckpointId")) return restore ? OptionalLong.of(CHECKPOINT) : OptionalLong.empty();
            if (method.equals("getOperatorStateStore")) return store;
            throw new AssertionError(method);
        }));
        source.open(new Configuration());
        return gauges;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> collect(Q8ReplaySource<T> source, int limit) throws Exception {
        List<T> events = new ArrayList<>();
        Object lock = new Object();
        SourceContext<T> context = proxy(SourceFunction.SourceContext.class, (method, args) -> {
            if (method.equals("getCheckpointLock")) return lock;
            if (method.equals("collect")) {
                check(Thread.holdsLock(lock), "source emission outside checkpoint lock");
                events.add((T) args[0]);
                if (events.size() == limit) source.cancel();
                return null;
            }
            throw new AssertionError(method);
        });
        source.run(context);
        return events;
    }

    private static <T> T proxy(Class<T> type, BiFunction<String, Object[], Object> invoke) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (object, method, args) -> invoke.apply(method.getName(), args)));
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
