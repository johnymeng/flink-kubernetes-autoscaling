package org.apache.flink.a4s.core;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Stack distance histogram in QuickMRC units or, with a SHARDS header, in bytes. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StackDistanceHistogram implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER =
            JacksonMapperFactory.createObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    private final long[] bucketCounts;
    private final long completeMisses;
    private final int numPartitions;
    private final long bucketSizeScaling;
    private final long cacheItemSizeBytes;
    @Nullable private final ShardsHeader shards;

    public StackDistanceHistogram(
            long[] bucketCounts,
            long completeMisses,
            int numPartitions,
            long bucketSizeScaling,
            long cacheItemSizeBytes) {
        this(
                bucketCounts,
                completeMisses,
                numPartitions,
                bucketSizeScaling,
                cacheItemSizeBytes,
                null);
    }

    @JsonCreator
    public StackDistanceHistogram(
            @JsonProperty("bucketCounts") long[] bucketCounts,
            @JsonProperty(value = "completeMisses", required = true) long completeMisses,
            @JsonProperty("numPartitions") int numPartitions,
            @JsonProperty("bucketSizeScaling") long bucketSizeScaling,
            @JsonProperty("cacheItemSizeBytes") long cacheItemSizeBytes,
            @JsonProperty("shards") @Nullable ShardsHeader shards) {
        validateCounts(bucketCounts);
        validateCompleteMisses(completeMisses);
        validateBucketSizeScaling(bucketSizeScaling);
        validateCacheItemSizeBytes(cacheItemSizeBytes);
        this.bucketCounts = Arrays.copyOf(bucketCounts, bucketCounts.length);
        this.completeMisses = completeMisses;
        this.numPartitions = numPartitions;
        this.bucketSizeScaling = bucketSizeScaling;
        this.cacheItemSizeBytes = cacheItemSizeBytes;
        this.shards = shards;
        if (shards != null
                && (bucketSizeScaling != 1
                        || cacheItemSizeBytes != shards.binBytes
                        || bucketCounts.length != 1 + (shards.maxDistanceBytes - 1) / shards.binBytes
                        || getTotalFrequency() != shards.sampledRefs)) {
            throw new IllegalArgumentException("SHARDS histogram does not match its header");
        }
    }

    /**
     * Parses the array returned by {@code LRUCache#getShardsMrcHistogram()}: {@link
     * ShardsHeader#LENGTH} header fields, the finite bins, then the infinite bin.
     */
    public static StackDistanceHistogram fromShardsMrcArray(long[] raw) {
        if (raw == null || raw.length < ShardsHeader.LENGTH + 1) {
            throw new IllegalArgumentException("SHARDS histogram array is too short");
        }
        ShardsHeader header = ShardsHeader.fromArray(raw);
        return new StackDistanceHistogram(
                Arrays.copyOfRange(raw, ShardsHeader.LENGTH, raw.length - 1),
                raw[raw.length - 1],
                1,
                1L,
                header.getBinBytes(),
                header);
    }

    public String toMetricString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize stack distance histogram", e);
        }
    }

    public static StackDistanceHistogram fromMetricString(String serializedHistogram) {
        if (serializedHistogram == null || serializedHistogram.trim().isEmpty()) {
            throw new IllegalArgumentException("Serialized histogram must not be null or empty");
        }

        try {
            StackDistanceHistogram histogram =
                    OBJECT_MAPPER.readValue(serializedHistogram, StackDistanceHistogram.class);
            if (histogram == null) {
                throw new IllegalArgumentException("Invalid histogram payload: empty object");
            }
            return histogram;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid histogram payload: expected JSON StackDistanceHistogram object", e);
        }
    }

    private static void validateCounts(long[] counts) {
        if (counts == null) {
            throw new IllegalArgumentException("Bucket counts must not be null");
        }
        for (long count : counts) {
            if (count < 0L) {
                throw new IllegalArgumentException("Bucket counts must be non-negative");
            }
        }
    }

    private static void validateBucketSizeScaling(long bucketSizeScaling) {
        if (bucketSizeScaling <= 0L) {
            throw new IllegalArgumentException("bucketSizeScaling must be > 0");
        }
    }

    private static void validateCompleteMisses(long completeMisses) {
        if (completeMisses < 0L) {
            throw new IllegalArgumentException("completeMisses must be >= 0");
        }
    }

    private static void validateCacheItemSizeBytes(long cacheItemSizeBytes) {
        if (cacheItemSizeBytes <= 0L) {
            throw new IllegalArgumentException("cacheItemSizeBytes must be > 0");
        }
    }

    public long[] getBucketCounts() {
        return Arrays.copyOf(bucketCounts, bucketCounts.length);
    }

    public int getNumBuckets() {
        return bucketCounts.length;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public long getBucketSizeScaling() {
        return bucketSizeScaling;
    }

    public long getCacheItemSizeBytes() {
        return cacheItemSizeBytes;
    }

    @JsonProperty("shards")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public ShardsHeader getShards() {
        return shards;
    }

    /** 1 for a QuickMRC histogram, 2 for a SHARDS histogram. */
    @JsonIgnore
    public int getVersion() {
        return shards == null ? 1 : 2;
    }

    public long getCompleteMisses() {
        return completeMisses;
    }

    public long getHistogramFingerprint() {
        long h = 0xcbf29ce484222325L;
        final long prime = 0x100000001b3L;
        for (long count : bucketCounts) {
            h ^= count;
            h *= prime;
        }
        h ^= completeMisses;
        h *= prime;
        h ^= (long) numPartitions;
        h *= prime;
        h ^= bucketSizeScaling;
        h *= prime;
        h ^= cacheItemSizeBytes;
        h *= prime;
        return h;
    }

    public static StackDistanceHistogram merge(List<StackDistanceHistogram> histograms) {
        if (histograms == null || histograms.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty list of histograms");
        }

        if (histograms.stream().anyMatch(histogram -> histogram.shards != null)) {
            throw new IllegalArgumentException("Use mergeShards for SHARDS histograms");
        }
        if (histograms.size() == 1) {
            return histograms.get(0);
        }

        int maxFiniteBuckets = 0;
        for (StackDistanceHistogram histogram : histograms) {
            maxFiniteBuckets = Math.max(maxFiniteBuckets, histogram.getNumBuckets());
        }

        long[] mergedFiniteCounts = new long[maxFiniteBuckets];
        long mergedCompleteMisses = 0L;
        long mergedBucketSizeScaling = histograms.get(0).getBucketSizeScaling();
        long mergedCacheItemSizeBytes = histograms.get(0).getCacheItemSizeBytes();

        int totalPartitions = 0;
        for (StackDistanceHistogram histogram : histograms) {
            if (histogram.getBucketSizeScaling() != mergedBucketSizeScaling) {
                throw new IllegalArgumentException(
                        "Cannot merge histograms with different bucketSizeScaling values");
            }
            if (histogram.getCacheItemSizeBytes() != mergedCacheItemSizeBytes) {
                throw new IllegalArgumentException(
                        "Cannot merge histograms with different cacheItemSizeBytes values");
            }
            for (int i = 0; i < histogram.getNumBuckets(); i++) {
                mergedFiniteCounts[i] =
                        Math.addExact(mergedFiniteCounts[i], histogram.getFrequency(i));
            }
            mergedCompleteMisses =
                    Math.addExact(mergedCompleteMisses, histogram.getCompleteMisses());
            totalPartitions = Math.addExact(totalPartitions, histogram.getNumPartitions());
        }

        return new StackDistanceHistogram(
                mergedFiniteCounts,
                mergedCompleteMisses,
                totalPartitions,
                mergedBucketSizeScaling,
                mergedCacheItemSizeBytes);
    }

    /** Merges one SHARDS histogram per subtask of a fully observed vertex. */
    public static StackDistanceHistogram mergeShards(
            List<StackDistanceHistogram> histograms, int vertexParallelism) {
        if (histograms.isEmpty() || histograms.size() != vertexParallelism) {
            throw new IllegalArgumentException("SHARDS requires one histogram per subtask");
        }
        long[] counts = new long[histograms.get(0).getNumBuckets()];
        long completeMisses = 0L;
        ShardsHeader header = null;
        for (StackDistanceHistogram histogram : histograms) {
            if (histogram.shards == null || histogram.numPartitions != 1) {
                throw new IllegalArgumentException("Expected a single-partition SHARDS histogram");
            }
            header = header == null ? histogram.shards : header.add(histogram.shards);
            for (int i = 0; i < counts.length; i++) {
                counts[i] = Math.addExact(counts[i], histogram.bucketCounts[i]);
            }
            completeMisses = Math.addExact(completeMisses, histogram.completeMisses);
        }
        return new StackDistanceHistogram(
                counts, completeMisses, vertexParallelism, 1L, header.binBytes, header);
    }

    public long getTotalFrequency() {
        long total = completeMisses;
        for (long count : this.bucketCounts) {
            total = Math.addExact(total, count);
        }
        return total;
    }

    public long getFrequency(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= getNumBuckets()) {
            return 0L;
        }
        return bucketCounts[bucketIndex];
    }

    @Override
    public String toString() {
        return String.format(
                "StackDistanceHistogram{numBuckets=%d, completeMisses=%d, numPartitions=%d, bucketSizeScaling=%d, cacheItemSizeBytes=%d, totalFrequency=%d}",
                getNumBuckets(),
                completeMisses,
                numPartitions,
                bucketSizeScaling,
                cacheItemSizeBytes,
                getTotalFrequency());
    }

    /**
     * Header of a SHARDS histogram, as returned by {@code LRUCache#getShardsMrcHistogram()}.
     * Counters are cumulative since the cache was created and never reset.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardsHeader implements Serializable {
        private static final long serialVersionUID = 1L;

        /** Number of header fields before the bins in the native array. */
        public static final int LENGTH = 10;

        private final double samplingRate;
        private final long hashSeed;
        private final long binBytes;
        private final long maxDistanceBytes;
        private final long totalRefs;
        private final long sampledRefs;
        private final long evictedByCap;
        private final long trackedEntries;
        private final long trackedBytes;
        private final long estimatorNanos;

        @JsonCreator
        public ShardsHeader(
                @JsonProperty("samplingRate") double samplingRate,
                @JsonProperty("hashSeed") long hashSeed,
                @JsonProperty("binBytes") long binBytes,
                @JsonProperty("maxDistanceBytes") long maxDistanceBytes,
                @JsonProperty("totalRefs") long totalRefs,
                @JsonProperty("sampledRefs") long sampledRefs,
                @JsonProperty("evictedByCap") long evictedByCap,
                @JsonProperty("trackedEntries") long trackedEntries,
                @JsonProperty("trackedBytes") long trackedBytes,
                @JsonProperty("estimatorNanos") long estimatorNanos) {
            if (!(samplingRate > 0.0 && samplingRate <= 1.0)
                    || binBytes <= 0L
                    || maxDistanceBytes <= 0L
                    || totalRefs < 0
                    || sampledRefs < 0
                    || evictedByCap < 0
                    || trackedEntries < 0
                    || trackedBytes < 0
                    || estimatorNanos < 0) {
                throw new IllegalArgumentException("Invalid SHARDS histogram header");
            }
            this.samplingRate = samplingRate;
            this.hashSeed = hashSeed;
            this.binBytes = binBytes;
            this.maxDistanceBytes = maxDistanceBytes;
            this.totalRefs = totalRefs;
            this.sampledRefs = sampledRefs;
            this.evictedByCap = evictedByCap;
            this.trackedEntries = trackedEntries;
            this.trackedBytes = trackedBytes;
            this.estimatorNanos = estimatorNanos;
        }

        static ShardsHeader fromArray(long[] raw) {
            return new ShardsHeader(
                    Double.longBitsToDouble(raw[0]),
                    raw[1],
                    raw[2],
                    raw[3],
                    raw[4],
                    raw[5],
                    raw[6],
                    raw[7],
                    raw[8],
                    raw[9]);
        }

        /** Sums the counters of two headers with the same estimator configuration. */
        ShardsHeader add(ShardsHeader other) {
            if (samplingRate != other.samplingRate
                    || hashSeed != other.hashSeed
                    || binBytes != other.binBytes
                    || maxDistanceBytes != other.maxDistanceBytes) {
                throw new IllegalArgumentException(
                        "Cannot merge SHARDS histograms with different estimator settings");
            }
            return new ShardsHeader(
                    samplingRate,
                    hashSeed,
                    binBytes,
                    maxDistanceBytes,
                    Math.addExact(totalRefs, other.totalRefs),
                    Math.addExact(sampledRefs, other.sampledRefs),
                    Math.addExact(evictedByCap, other.evictedByCap),
                    Math.addExact(trackedEntries, other.trackedEntries),
                    Math.addExact(trackedBytes, other.trackedBytes),
                    Math.addExact(estimatorNanos, other.estimatorNanos));
        }

        public double getSamplingRate() {
            return samplingRate;
        }

        public long getHashSeed() {
            return hashSeed;
        }

        public long getBinBytes() {
            return binBytes;
        }

        public long getMaxDistanceBytes() {
            return maxDistanceBytes;
        }

        public long getTotalRefs() {
            return totalRefs;
        }

        public long getSampledRefs() {
            return sampledRefs;
        }

        public long getEvictedByCap() {
            return evictedByCap;
        }

        public long getTrackedEntries() {
            return trackedEntries;
        }

        public long getTrackedBytes() {
            return trackedBytes;
        }

        public long getEstimatorNanos() {
            return estimatorNanos;
        }
    }
}
