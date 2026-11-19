package be.uclouvain.gepiciad.sources;

import java.io.Serializable;

/** A bounded sequence of open-loop release rates for one Q8 source partition. */
final class Q8InputPlan implements Serializable {
    private static final long serialVersionUID = 1L;
    final String definition;
    final long releaseMillis;
    final long endOffset;
    private final long[] stageMillis;
    private final int[] stageRates;

    Q8InputPlan(String definition, long releaseMillis, boolean persons) {
        if (releaseMillis <= 0) {
            throw new IllegalArgumentException("q8-release-epoch-ms must be a positive Unix timestamp");
        }
        this.definition = definition;
        this.releaseMillis = releaseMillis;
        String[] stages = definition.split(",", -1);
        stageMillis = new long[stages.length];
        stageRates = new int[stages.length];
        long total = 0;
        long totalMillis = 0;
        for (int i = 0; i < stages.length; i++) {
            String[] parts = stages[i].split(":", -1);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Expected seconds:personRate:auctionRate per Q8 stage");
            }
            long seconds = Long.parseLong(parts[0]);
            int personRate = Integer.parseInt(parts[1]);
            int auctionRate = Integer.parseInt(parts[2]);
            if (seconds <= 0 || personRate < 0 || auctionRate < 0) {
                throw new IllegalArgumentException("Q8 stages need a positive duration and nonnegative rates");
            }
            stageRates[i] = persons ? personRate : auctionRate;
            stageMillis[i] = Math.multiplyExact(1000L, seconds);
            totalMillis = Math.addExact(totalMillis, stageMillis[i]);
            total = Math.addExact(total, Math.multiplyExact(seconds, stageRates[i]));
        }
        Math.addExact(releaseMillis, totalMillis);
        endOffset = total;
    }

    long offered(long nowMillis) {
        long elapsed = Math.max(0L, nowMillis - releaseMillis);
        long count = 0;
        for (int i = 0; i < stageMillis.length; i++) {
            long inStage = Math.min(elapsed, stageMillis[i]);
            count += inStage / 1000 * stageRates[i] + inStage % 1000 * stageRates[i] / 1000;
            elapsed -= inStage;
            if (elapsed == 0) {
                break;
            }
        }
        return count;
    }
}
