/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheDecision;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheEvaluation;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheMode;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.FixedCosts;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.ObservationEvidence;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.a4s.A4SAlignedCacheWindow;
import org.apache.flink.autoscaler.a4s.CacheCostSample;
import org.apache.flink.autoscaler.a4s.MemoryParallelismCurve;
import org.apache.flink.autoscaler.a4s.OnlineCacheCost;
import org.apache.flink.autoscaler.a4s.QuickMrcCostProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.BACKLOG_PROCESSING_LAG_THRESHOLD;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.*;

/** Job scaling evaluator for autoscaler. */
public class ScalingMetricEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricEvaluator.class);
    private static final double MAX_CACHE_COST_HOLDOUT_ERROR = 0.25;
    private static final double BYTES_PER_MIB = 1024.0 * 1024.0;
    private static final Duration MIN_CACHE_COST_WINDOW =
            Duration.ofSeconds(OnlineCacheCost.MIN_WINDOW_SECONDS);
    private static final Duration MAX_CACHE_COST_WINDOW =
            Duration.ofSeconds(OnlineCacheCost.MAX_WINDOW_SECONDS);

    public EvaluatedMetrics evaluate(
            Configuration conf, CollectedMetricHistory collectedMetrics, Duration restartTime) {
        LOG.debug("Restart time used in metrics evaluation: {}", restartTime);
        var scalingOutput = new HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>();
        var memoryParallelismCurves = new HashMap<JobVertexID, MemoryParallelismCurve>();
        var cacheCostFailures = new HashMap<JobVertexID, OnlineCacheCost.Reason>();
        var metricsHistory = collectedMetrics.getMetricHistory();
        var topology = collectedMetrics.getJobTopology();
        CollectedMetrics latestCollectedMetrics = metricsHistory.get(metricsHistory.lastKey());
        Map<JobVertexID, MissRateCurve> collectedMissRateCurves =
                latestCollectedMetrics.getMissRateCurves() == null
                        ? Map.of()
                        : latestCollectedMetrics.getMissRateCurves();
        var result = new EvaluatedMetrics(scalingOutput, null, memoryParallelismCurves,
                collectedMissRateCurves, cacheCostFailures);
        var decisions = result.getCacheEvaluation().getVertices();
        double hitLatencySec = conf.getDouble(AutoScalerOptions.A4S_HIT_LATENCY_SEC);
        double missLatencySec = conf.getDouble(AutoScalerOptions.A4S_MISS_LATENCY_SEC);
        var calibratedCacheCosts = conf.get(AutoScalerOptions.A4S_CALIBRATED_CACHE_COSTS);
        boolean onlineCacheCosts = conf.get(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED);
        boolean strictA4s = conf.get(AutoScalerOptions.A4S_ENABLED)
                && conf.get(AutoScalerOptions.JUSTIN_ENABLED);
        result.getCacheEvaluation().setCachePlacementEnabled(strictA4s);
        boolean shadow = conf.get(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_SHADOW);
        var exemptVertices = conf.get(AutoScalerOptions.A4S_CACHE_METRICS_EXEMPT_VERTICES);
        var fixedCosts = new FixedCosts(hitLatencySec, missLatencySec);
        int vertexMinParallelism = conf.get(AutoScalerOptions.VERTEX_MIN_PARALLELISM);
        int vertexMaxParallelism = conf.get(AutoScalerOptions.VERTEX_MAX_PARALLELISM);

        boolean processingBacklog = isProcessingBacklog(topology, metricsHistory, conf);

        for (var vertex : topology.getVerticesInTopologicalOrder()) {
            scalingOutput.put(
                    vertex,
                    evaluateMetrics(
                            conf,
                            scalingOutput,
                            metricsHistory,
                            topology,
                            vertex,
                            processingBacklog,
                            restartTime));

            // This relies on target data rate first being calculated       
            LOG.debug("A4S: Evaluating metrics for vertex {}", vertex);
            MissRateCurve missRateCurve = collectedMissRateCurves.get(vertex);
            var batch = cacheObservation(latestCollectedMetrics, vertex);
            int parallelism = topology.get(vertex).getParallelism();
            var observationStatus = OnlineCacheCost.observationStatus(batch, parallelism);
            if (missRateCurve == null || missRateCurve.getPoints().isEmpty()) {
                LOG.debug("A4S: No usable miss rate curve found for vertex {}, skipping", vertex);
                if ((strictA4s || onlineCacheCosts) && !exemptVertices.contains(vertex.toString())
                        && !topology.getFinishedVertices().contains(vertex)) {
                    var status = observationStatus == CacheObservation.Status.VALID
                            ? CacheObservation.Status.INCOMPLETE : observationStatus;
                    decisions.put(vertex, new CacheDecision(
                            onlineCacheCosts && shadow ? CacheMode.ONLINE_SHADOW : CacheMode.INVALID,
                            null, ObservationEvidence.capture(parallelism, status, batch,
                                    cacheCostSamples(latestCollectedMetrics, vertex)),
                            null, null, OnlineCacheCost.Reason.MISSING_OBSERVATIONS, null, null, null,
                            null, null, null));
                }
                continue;
            }
            var points = missRateCurve.getPoints();
            long observedMinBytes = points.get(0).getCacheSizeBytes();
            long observedMaxBytes = points.get(points.size() - 1).getCacheSizeBytes();
            var metrics = scalingOutput.get(vertex);
            double targetRate = strictA4s || onlineCacheCosts
                    ? AutoScalerUtils.getTargetProcessingCapacity(
                            metrics, conf, conf.get(TARGET_UTILIZATION), true, restartTime)
                    : metrics.get(TARGET_DATA_RATE).getAverage();
            String calibratedCosts = calibratedCacheCosts.get(vertex.toHexString());
            CacheMode configuredPolicy =
                    calibratedCosts == null ? CacheMode.LEGACY_FIXED : CacheMode.OFFLINE_CALIBRATED;
            if (onlineCacheCosts) {
                var fit = selectOnlineFit(metricsHistory, vertex, parallelism);
                OnlineCacheCost.Reason reason = fit.getReason();
                Double fixedBaseline = shadow
                                && conf.get(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_USE_FIXED_BASELINE)
                        ? hitLatencySec : null;
                QuickMrcCostProjection.Result projection = null;
                QuickMrcCostProjection.Recommendation recommendation = null;
                if (reason == OnlineCacheCost.Reason.QUALIFIED) {
                    projection = QuickMrcCostProjection.project(
                            fit.getFit(), fit.getAlignedCurve(), targetRate, fixedBaseline);
                    recommendation = projection.getRecommendation();
                    if (recommendation == null) {
                        reason = !shadow && QuickMrcCostProjection
                                        .cannotMeetDemandAtCurrentParallelism(fit.getFit(), targetRate)
                                ? OnlineCacheCost.Reason.INSUFFICIENT_CURRENT_PARALLELISM
                                : OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED;
                    }
                }
                CacheMode mode = onlineMode(shadow, reason);
                var observation = ObservationEvidence.capture(parallelism, observationStatus, batch,
                        cacheCostSamples(latestCollectedMetrics, vertex), fit.getFailingSubtask());
                ObservationEvidence fitStart = null;
                if (fit.getAttemptedWindow() != null) {
                    var first = metricsHistory.get(fit.getAttemptedWindow().getStart());
                    var startBatch = cacheObservation(first, vertex);
                    fitStart = ObservationEvidence.capture(parallelism,
                            OnlineCacheCost.observationStatus(startBatch, parallelism),
                            startBatch, cacheCostSamples(first, vertex), fit.getFailingSubtask());
                }
                decisions.put(vertex, new CacheDecision(mode,
                        shadow ? configuredPolicy : recommendation == null ? null : mode,
                        observation, fitStart, fit, reason, finite(targetRate), observedMinBytes,
                        observedMaxBytes, projection,
                        shadow && configuredPolicy == CacheMode.LEGACY_FIXED ? fixedCosts : null,
                        fixedBaseline));
                if (!shadow) {
                    if (recommendation == null) {
                        cacheCostFailures.put(vertex, reason);
                    } else {
                        memoryParallelismCurves.put(vertex,
                                MemoryParallelismCurve.builder().targetThroughput(targetRate)
                                        .addPoint(parallelism,
                                                recommendation.getTotalNativeCacheBytes() / BYTES_PER_MIB)
                                        .build());
                    }
                    continue;
                }
            } else {
                var observation = ObservationEvidence.capture(parallelism, observationStatus, batch,
                        cacheCostSamples(latestCollectedMetrics, vertex));
                decisions.put(vertex, new CacheDecision(configuredPolicy, configuredPolicy,
                        observation, null, null, null, finite(targetRate), observedMinBytes,
                        observedMaxBytes, null, calibratedCosts == null ? fixedCosts : null, null));
            }

            if (calibratedCosts == null) {
                var vertexInfo = topology.get(vertex);
                int currentParallelism = vertexInfo.getParallelism();
                int minParallelism = Math.max(1, Math.min(currentParallelism, vertexMinParallelism));
                int maxParallelism =
                        Math.min(vertexInfo.getMaxParallelism(),
                                Math.max(currentParallelism, vertexMaxParallelism));
                MemoryParallelismCurve curve =
                        MemoryParallelismCurve.fromMissRateCurve(
                                targetRate,
                                missLatencySec,
                                hitLatencySec,
                                minParallelism,
                                maxParallelism,
                                missRateCurve);
                if (curve.getPoints().isEmpty()) {
                    cacheCostFailures.put(vertex, OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED);
                    if (!onlineCacheCosts) {
                        var decision = decisions.get(vertex);
                        decisions.put(vertex, new CacheDecision(configuredPolicy, null,
                                decision.getObservation(), null, null,
                                OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED, finite(targetRate),
                                observedMinBytes, observedMaxBytes, null, fixedCosts, null));
                    }
                } else {
                    memoryParallelismCurves.put(vertex, curve);
                }
            } else {
                MemoryParallelismCurve curve =
                        calibratedCurve(
                                vertex,
                                calibratedCosts,
                                targetRate,
                                vertexMinParallelism,
                                vertexMaxParallelism,
                                missRateCurve);
                memoryParallelismCurves.put(
                        vertex, requireFeasibleCurve(vertex, targetRate, curve));
            }
        }

        result.setGlobalMetrics(evaluateGlobalMetrics(metricsHistory));
        return result;
    }

    @Nullable
    private static Double finite(double value) {
        return Double.isFinite(value) ? value : null;
    }

    private static CacheMode onlineMode(boolean shadow, OnlineCacheCost.Reason reason) {
        if (shadow) {
            return CacheMode.ONLINE_SHADOW;
        }
        switch (reason) {
            case QUALIFIED:
                return CacheMode.ONLINE_QUALIFIED;
            case INSUFFICIENT_CURRENT_PARALLELISM:
                return CacheMode.ONLINE_OVERLOAD;
            default:
                return CacheMode.INVALID;
        }
    }

    static CacheEvaluation observationHold(
            CollectedMetricHistory collected, Map<JobVertexID, CacheObservation.Status> holds) {
        var decision = new CacheEvaluation();
        decision.getObservationHolds().putAll(holds);
        var history = collected.getMetricHistory();
        var latest = history.isEmpty() ? null : history.get(history.lastKey());
        var topology = collected.getJobTopology();
        for (var vertex : topology.getVerticesInTopologicalOrder()) {
            var batch = latest == null ? null : cacheObservation(latest, vertex);
            if (batch == null && !holds.containsKey(vertex)) {
                continue;
            }
            int parallelism = topology.get(vertex).getParallelism();
            var status = holds.getOrDefault(vertex, OnlineCacheCost.observationStatus(batch, parallelism));
            var observation = ObservationEvidence.capture(parallelism, status, batch,
                    latest == null ? List.of() : cacheCostSamples(latest, vertex));
            var reason = status == CacheObservation.Status.VALID
                    ? OnlineCacheCost.Reason.INSUFFICIENT_HISTORY
                    : status == CacheObservation.Status.COUNTER_RESET
                            ? OnlineCacheCost.Reason.COUNTER_RESET : OnlineCacheCost.Reason.MISSING_OBSERVATIONS;
            decision.getVertices().put(vertex, new CacheDecision(
                    CacheMode.INVALID, null, observation, null, null, reason,
                    null, null, null, null, null, null));
        }
        return decision;
    }

    private static MemoryParallelismCurve requireFeasibleCurve(
            JobVertexID vertex, double targetRate, MemoryParallelismCurve curve) {
        if (curve.getPoints().isEmpty()) {
            throw new IllegalStateException(
                    "No feasible calibrated A4S curve for vertex "
                            + vertex + " at " + targetRate + " records/s");
        }
        return curve;
    }

    @VisibleForTesting
    static OnlineCacheCost.FitResult selectOnlineFit(
            SortedMap<Instant, CollectedMetrics> history, JobVertexID vertex, int parallelism) {
        Instant end = history.lastKey();
        var current = cacheCostSamples(history.get(end), vertex);
        if (current.size() != parallelism) {
            return OnlineCacheCost.FitResult.rejected(OnlineCacheCost.Reason.MISSING_TIMING)
                    .withObservationStatus(
                            OnlineCacheCost.observationStatus(cacheObservation(history.get(end), vertex), parallelism));
        }
        var candidates = fitStartTimes(history, end);
        OnlineCacheCost.FitResult latestRejection = null;
        for (int i = candidates.size() - 1; i >= 0; i--) {
            Instant start = candidates.get(i);
            var fit = fitBetween(history, vertex, start, end);
            if (fit.getFit() == null) {
                if (latestRejection == null) {
                    latestRejection = fit;
                }
                continue;
            }
            var priorStarts = fitStartTimes(history, start);
            OnlineCacheCost.FitResult priorRejection = null;
            for (int n = priorStarts.size() - 1; n >= 0; n--) {
                var prior = fitBetween(history, vertex, priorStarts.get(n), start);
                if (prior.getFit() == null) {
                    if (priorRejection == null) {
                        priorRejection = prior;
                    }
                    continue;
                }
                double error = prior.getFit().holdoutRelativeError(fit.getFit());
                var contextStatus = Double.isFinite(error) ? CacheObservation.Status.VALID
                        : prior.getFit().holdoutContextStatus(fit.getFit());
                boolean comparable = contextStatus == CacheObservation.Status.VALID;
                boolean stableMix = comparable && prior.getFit().hasStableAccessMix(fit.getFit());
                var holdout = new OnlineCacheCost.Holdout(prior.getAttemptedWindow(),
                        prior.getFit().getWindowEndMillis(), finite(error),
                        comparable ? stableMix : null,
                        comparable ? null : OnlineCacheCost.Reason.CONTEXT_CHANGED,
                        comparable ? null : contextStatus);
                if (error > MAX_CACHE_COST_HOLDOUT_ERROR
                        || !stableMix) {
                    return fit.withReason(OnlineCacheCost.Reason.PHASE_DRIFT).withHoldout(holdout);
                }
                return fit.withHoldout(holdout);
            }
            return fit.withReason(OnlineCacheCost.Reason.MISSING_HOLDOUT)
                    .withHoldout(new OnlineCacheCost.Holdout(
                            priorRejection == null ? null : priorRejection.getAttemptedWindow(),
                            null, null, null,
                            priorRejection == null ? OnlineCacheCost.Reason.INSUFFICIENT_HISTORY
                                    : priorRejection.getReason(),
                            priorRejection == null ? null : priorRejection.getObservationStatus()));
        }
        return latestRejection == null
                ? OnlineCacheCost.FitResult.rejected(OnlineCacheCost.Reason.INSUFFICIENT_HISTORY)
                        .withObservationStatus(OnlineCacheCost.observationStatus(
                                cacheObservation(history.get(end), vertex), parallelism))
                : latestRejection;
    }

    private static List<Instant> fitStartTimes(
            SortedMap<Instant, CollectedMetrics> history, Instant end) {
        Instant earliest = end.minus(MAX_CACHE_COST_WINDOW);
        Instant mostRecent = end.minus(MIN_CACHE_COST_WINDOW);
        List<Instant> candidates = new ArrayList<>();
        for (Instant timestamp : history.subMap(earliest, end).keySet()) {
            if (timestamp.isAfter(mostRecent)) {
                break;
            }
            candidates.add(timestamp);
        }
        return candidates;
    }

    private static OnlineCacheCost.FitResult fitBetween(
            SortedMap<Instant, CollectedMetrics> history, JobVertexID vertex, Instant start, Instant end) {
        var before = history.get(start);
        var after = history.get(end);
        var startBatch = cacheObservation(before, vertex);
        var window = new OnlineCacheCost.FitWindow(start, end);
        var initialSamples = cacheCostSamples(before, vertex);
        var previous = initialSamples;
        for (var entry : history.subMap(start, end).values()) {
            var batch = cacheObservation(entry, vertex);
            if (batch != null && (batch.status == CacheObservation.Status.COUNTER_RESET
                    || batch.status == CacheObservation.Status.CACHE_CHANGED
                    || batch.status == CacheObservation.Status.SHARED_CACHE)) {
                return OnlineCacheCost.FitResult.rejected(
                                batch.status == CacheObservation.Status.COUNTER_RESET
                                        ? OnlineCacheCost.Reason.COUNTER_RESET
                                        : OnlineCacheCost.Reason.CONTEXT_CHANGED)
                        .withObservationStatus(batch.status).withAttemptedWindow(window);
            }
            if (startBatch != null && batch != null && batch.status == CacheObservation.Status.VALID
                    && batch.expectedSubtasks > 0 && batch.subtasks.size() == batch.expectedSubtasks
                    && batch.expectedSubtasks != startBatch.expectedSubtasks) {
                return OnlineCacheCost.FitResult.rejected(OnlineCacheCost.Reason.CONTEXT_CHANGED)
                        .withObservationStatus(CacheObservation.Status.CACHE_CHANGED)
                        .withAttemptedWindow(window);
            }
            var samples = cacheCostSamples(entry, vertex);
            if (!samples.isEmpty() && samples.size() == previous.size()) {
                var discontinuity = OnlineCacheCost.findDiscontinuity(previous, samples);
                if (discontinuity.isPresent()) {
                    return discontinuity.get().withAttemptedWindow(window);
                }
                previous = samples;
            }
        }
        var current = cacheCostSamples(after, vertex);
        if (previous != initialSamples
                && current.size() == previous.size()) {
            var discontinuity = OnlineCacheCost.findDiscontinuity(previous, current);
            if (discontinuity.isPresent()) {
                return discontinuity.get().withAttemptedWindow(window);
            }
        }
        var endBatch = cacheObservation(after, vertex);
        var fit = OnlineCacheCost.fitQuickMrc(startBatch, initialSamples, endBatch, current);
        if (fit.getFit() == null) {
            return fit.withAttemptedWindow(window);
        }
        var aligned = A4SAlignedCacheWindow.between(startBatch, initialSamples, endBatch, current);
        return (aligned.batch == null
                        ? OnlineCacheCost.FitResult.rejected(OnlineCacheCost.Reason.UNBOUND_TIMING)
                                .withObservationStatus(aligned.status)
                        : fit.withAlignedCurve(aligned.batch))
                .withAttemptedWindow(window);
    }

    private static CacheObservationBatch cacheObservation(CollectedMetrics metrics, JobVertexID vertex) {
        var observations = metrics.getCacheObservations();
        return observations == null ? null : observations.get(vertex);
    }

    private static List<CacheCostSample> cacheCostSamples(
            CollectedMetrics metrics, JobVertexID vertex) {
        var samples = metrics.getCacheCostSamples();
        return samples == null ? List.of() : samples.getOrDefault(vertex, List.of());
    }

    private static MemoryParallelismCurve calibratedCurve(
            JobVertexID vertex,
            String costs,
            double targetRate,
            int minParallelism,
            int maxParallelism,
            MissRateCurve missRateCurve) {
        String[] values = costs.split("/", -1);
        String error =
                "Invalid " + AutoScalerOptions.A4S_CALIBRATED_CACHE_COSTS.key() + " for vertex "
                        + vertex + ": expected positive finite zeroMissCost/missPenalty";
        if (values.length != 2) {
            throw new IllegalArgumentException(error);
        }
        try {
            return MemoryParallelismCurve.fromCalibratedMissRateCurve(
                    targetRate,
                    Double.parseDouble(values[0]),
                    Double.parseDouble(values[1]),
                    minParallelism,
                    maxParallelism,
                    missRateCurve);
        } catch (IllegalArgumentException invalidCosts) {
            throw new IllegalArgumentException(error, invalidCosts);
        }
    }

    @VisibleForTesting
    protected static boolean isProcessingBacklog(
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            Configuration conf) {
        var lastMetrics = metricsHistory.get(metricsHistory.lastKey()).getVertexMetrics();
        return topology.getVerticesInTopologicalOrder().stream()
                .filter(topology::isSource)
                .anyMatch(
                        vertex -> {
                            double lag = lastMetrics.get(vertex).getOrDefault(LAG, 0.0);
                            double inputRateAvg =
                                    getRate(ScalingMetric.NUM_RECORDS_IN, vertex, metricsHistory);
                            if (Double.isNaN(inputRateAvg)) {
                                return false;
                            }
                            double lagSeconds = lag / inputRateAvg;
                            if (lagSeconds
                                    > conf.get(BACKLOG_PROCESSING_LAG_THRESHOLD).toSeconds()) {
                                LOG.info("Currently processing backlog at source {}", vertex);
                                return true;
                            } else {
                                return false;
                            }
                        });
    }

    @Nonnull
    private Map<ScalingMetric, EvaluatedScalingMetric> evaluateMetrics(
            Configuration conf,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> scalingOutput,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobTopology topology,
            JobVertexID vertex,
            boolean processingBacklog,
            Duration restartTime) {

        var latestVertexMetrics =
                metricsHistory.get(metricsHistory.lastKey()).getVertexMetrics().get(vertex);

        var vertexInfo = topology.get(vertex);

        double inputRateAvg = getRate(ScalingMetric.NUM_RECORDS_IN, vertex, metricsHistory);

        var evaluatedMetrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        computeTargetDataRate(
                topology,
                vertex,
                conf,
                inputRateAvg,
                scalingOutput,
                metricsHistory,
                latestVertexMetrics,
                evaluatedMetrics);

        double busyTimeAvg =
                computeBusyTimeAvg(conf, metricsHistory, vertex, vertexInfo.getParallelism());

        evaluatedMetrics.put(INPUT_RATE, EvaluatedScalingMetric.avg(inputRateAvg));
        evaluatedMetrics.put(
                TRUE_PROCESSING_RATE,
                EvaluatedScalingMetric.avg(
                        computeTrueProcessingRate(
                                busyTimeAvg, inputRateAvg, metricsHistory, vertex, conf)));

        evaluatedMetrics.put(LOAD, EvaluatedScalingMetric.avg(busyTimeAvg / 1000.));

        Optional.ofNullable(latestVertexMetrics.get(LAG))
                .ifPresent(l -> evaluatedMetrics.put(LAG, EvaluatedScalingMetric.of(l)));

        evaluatedMetrics.put(PARALLELISM, EvaluatedScalingMetric.of(vertexInfo.getParallelism()));

        evaluatedMetrics.put(
                MAX_PARALLELISM, EvaluatedScalingMetric.of(vertexInfo.getMaxParallelism()));

        evaluatedMetrics.put(
                NUM_SOURCE_PARTITIONS,
                EvaluatedScalingMetric.of(vertexInfo.getNumSourcePartitions()));

        if (latestVertexMetrics.containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE)) {
            evaluatedMetrics.put(
                    ROCKS_DB_BLOCK_CACHE_HIT_RATE,
                    new EvaluatedScalingMetric(
                            latestVertexMetrics.get(ROCKS_DB_BLOCK_CACHE_HIT_RATE),
                            getAverage(ROCKS_DB_BLOCK_CACHE_HIT_RATE, vertex, metricsHistory)
                    )
            );
        }

        computeProcessingRateThresholds(evaluatedMetrics, conf, processingBacklog, restartTime);
        return evaluatedMetrics;
    }

    /**
     * Compute the average busy time for the given vertex for the current metric window. Depending
     * on the {@link MetricAggregator} chosen we use two different mechanisms:
     *
     * <ol>
     *   <li>For AVG aggregator we compute from accumulated busy time to get the most precise metric
     *   <li>or MAX/MIN aggregators we have to average over the point-in-time MAX/MIN values
     *       collected over the metric windows. These are stored in the LOAD metric.
     * </ol>
     *
     * @param conf
     * @param metricsHistory
     * @param vertex
     * @param parallelism
     * @return Average busy time in the current metric window
     */
    @VisibleForTesting
    protected static double computeBusyTimeAvg(
            Configuration conf,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID vertex,
            int parallelism) {
        if (conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR) == MetricAggregator.AVG) {
            return getRate(ScalingMetric.ACCUMULATED_BUSY_TIME, vertex, metricsHistory)
                    / parallelism;
        } else {
            return getAverage(LOAD, vertex, metricsHistory) * 1000;
        }
    }

    /**
     * Compute the true processing rate for the given vertex for the current metric window. The
     * computation takes into account both observed (during catchup) and busy time based processing
     * rate and selects the right metric depending on the config.
     *
     * @param busyTimeAvg
     * @param inputRateAvg
     * @param metricsHistory
     * @param vertex
     * @param conf
     * @return Average true processing rate over metric window.
     */
    protected static double computeTrueProcessingRate(
            double busyTimeAvg,
            double inputRateAvg,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID vertex,
            Configuration conf) {

        var busyTimeTpr = computeTprFromBusyTime(busyTimeAvg, inputRateAvg);
        var observedTprAvg =
                getAverage(
                        OBSERVED_TPR,
                        vertex,
                        metricsHistory,
                        conf.get(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS));

        var tprMetric = selectTprMetric(vertex, conf, busyTimeTpr, observedTprAvg);
        return tprMetric == OBSERVED_TPR ? observedTprAvg : busyTimeTpr;
    }

    private static double computeTprFromBusyTime(double busyMsPerSecond, double rate) {
        if (rate == 0) {
            // Nothing is coming in, we assume infinite processing power
            // until we can sample the true processing rate (i.e. data flows).
            return Double.POSITIVE_INFINITY;
        }
        return rate / (busyMsPerSecond / 1000);
    }

    private static ScalingMetric selectTprMetric(
            JobVertexID jobVertexID,
            Configuration conf,
            double busyTimeTprAvg,
            double observedTprAvg) {

        if (Double.isNaN(observedTprAvg)) {
            return TRUE_PROCESSING_RATE;
        }

        if (Double.isInfinite(busyTimeTprAvg) || Double.isNaN(busyTimeTprAvg)) {
            return OBSERVED_TPR;
        }

        double switchThreshold =
                conf.get(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD);
        // If we could measure the observed tpr we decide whether to switch to using it
        // instead of busy time based on the error / difference between the two
        if (busyTimeTprAvg > observedTprAvg * (1 + switchThreshold)) {
            LOG.debug(
                    "Using observed tpr {} for {} as busy time based seems too large ({})",
                    observedTprAvg,
                    jobVertexID,
                    busyTimeTprAvg);
            return OBSERVED_TPR;
        } else {
            LOG.debug("Using busy time based tpr {} for {}.", busyTimeTprAvg, jobVertexID);
            return TRUE_PROCESSING_RATE;
        }
    }

    @VisibleForTesting
    protected static void computeProcessingRateThresholds(
            Map<ScalingMetric, EvaluatedScalingMetric> metrics,
            Configuration conf,
            boolean processingBacklog,
            Duration restartTime) {

        double utilizationBoundary = conf.getDouble(TARGET_UTILIZATION_BOUNDARY);
        double targetUtilization = conf.get(TARGET_UTILIZATION);

        double upperUtilization;
        double lowerUtilization;

        if (processingBacklog) {
            // When we are processing backlog we allow max utilization and we do not trigger scale
            // down on under utilization to avoid creating more lag.
            upperUtilization = 1.0;
            lowerUtilization = 0.0;
        } else {
            upperUtilization = targetUtilization + utilizationBoundary;
            lowerUtilization = targetUtilization - utilizationBoundary;
        }

        double scaleUpThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(
                        metrics, conf, upperUtilization, false, restartTime);

        double scaleDownThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(
                        metrics, conf, lowerUtilization, true, restartTime);

        metrics.put(SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleUpThreshold));
        metrics.put(SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleDownThreshold));
    }

    private void computeTargetDataRate(
            JobTopology topology,
            JobVertexID vertex,
            Configuration conf,
            double inputRate,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> alreadyEvaluated,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            Map<ScalingMetric, Double> latestVertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> out) {

        if (topology.isSource(vertex)) {
            double catchUpTargetSec = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();

            double lagRate = getRate(LAG, vertex, metricsHistory);
            double ingestionDataRate = Math.max(0, inputRate + lagRate);
            if (Double.isNaN(ingestionDataRate)) {
                throw new RuntimeException(
                        "Cannot evaluate metrics without ingestion rate information");
            }

            out.put(TARGET_DATA_RATE, EvaluatedScalingMetric.avg(ingestionDataRate));

            double lag = latestVertexMetrics.getOrDefault(LAG, 0.);
            double catchUpInputRate = catchUpTargetSec == 0 ? 0 : lag / catchUpTargetSec;
            if (catchUpInputRate > 0) {
                LOG.debug(
                        "Extra backlog processing input rate for {} is {}",
                        vertex,
                        catchUpInputRate);
            }
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchUpInputRate));
        } else {
            var inputs = topology.get(vertex).getInputs().keySet();
            double sumAvgTargetRate = 0;
            double sumCatchUpDataRate = 0;
            for (var inputVertex : inputs) {
                var inputEvaluatedMetrics = alreadyEvaluated.get(inputVertex);
                var inputTargetRate = inputEvaluatedMetrics.get(TARGET_DATA_RATE);
                var outputRatio =
                        computeEdgeOutputRatio(inputVertex, vertex, topology, metricsHistory);
                LOG.debug(
                        "Computed output ratio for edge ({} -> {}) : {}",
                        inputVertex,
                        vertex,
                        outputRatio);
                sumAvgTargetRate += inputTargetRate.getAverage() * outputRatio;
                sumCatchUpDataRate +=
                        inputEvaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent() * outputRatio;
            }
            out.put(TARGET_DATA_RATE, EvaluatedScalingMetric.avg(sumAvgTargetRate));
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(sumCatchUpDataRate));
        }
    }

    @VisibleForTesting
    protected static Map<ScalingMetric, EvaluatedScalingMetric> evaluateGlobalMetrics(
            SortedMap<Instant, CollectedMetrics> metricHistory) {
        var latest = metricHistory.get(metricHistory.lastKey()).getGlobalMetrics();
        var out = new HashMap<ScalingMetric, EvaluatedScalingMetric>();

        var gcPressure = latest.getOrDefault(GC_PRESSURE, Double.NaN);
        out.put(GC_PRESSURE, EvaluatedScalingMetric.of(gcPressure));

        populateMetric(HEAP_MAX_USAGE_RATIO, metricHistory, out);
        populateMetric(HEAP_MEMORY_USED, metricHistory, out);
        populateMetric(MANAGED_MEMORY_USED, metricHistory, out);
        populateMetric(METASPACE_MEMORY_USED, metricHistory, out);

        out.put(
                NUM_TASK_SLOTS_USED,
                EvaluatedScalingMetric.of(latest.getOrDefault(NUM_TASK_SLOTS_USED, Double.NaN)));

        return out;
    }

    private static void populateMetric(
            ScalingMetric scalingMetric,
            SortedMap<Instant, CollectedMetrics> metricHistory,
            Map<ScalingMetric, EvaluatedScalingMetric> out) {
        var latestMetrics = metricHistory.get(metricHistory.lastKey()).getGlobalMetrics();

        var latestObservation = latestMetrics.getOrDefault(scalingMetric, Double.NaN);
        double value = getAverageGlobalMetric(scalingMetric, metricHistory);

        out.put(scalingMetric, new EvaluatedScalingMetric(latestObservation, value));
    }

    private static double getAverageGlobalMetric(
            ScalingMetric metric, SortedMap<Instant, CollectedMetrics> metricsHistory) {
        return getAverage(metric, null, metricsHistory);
    }

    public static double getAverage(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {
        return getAverage(metric, jobVertexId, metricsHistory, 1);
    }

    /**
     * Compute per second rate for the given accumulated metric over the metric window.
     *
     * @param metric
     * @param jobVertexId
     * @param metricsHistory
     * @return Per second rate or Double.NaN if we don't have at least 2 observations.
     */
    public static double getRate(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {

        Instant firstTs = null;
        double first = Double.NaN;

        Instant lastTs = null;
        double last = Double.NaN;

        for (var entry : metricsHistory.entrySet()) {
            double value =
                    entry.getValue()
                            .getVertexMetrics()
                            .get(jobVertexId)
                            .getOrDefault(metric, Double.NaN);
            if (!Double.isNaN(value)) {
                if (Double.isNaN(first)) {
                    first = value;
                    firstTs = entry.getKey();
                } else {
                    last = value;
                    lastTs = entry.getKey();
                }
            }
        }
        if (Double.isNaN(last)) {
            return Double.NaN;
        }

        return 1000 * (last - first) / Duration.between(firstTs, lastTs).toMillis();
    }

    public static double getAverage(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            int minElements) {

        double sum = 0;
        int n = 0;
        boolean anyInfinite = false;
        for (var collectedMetrics : metricsHistory.values()) {
            var metrics =
                    jobVertexId != null
                            ? collectedMetrics.getVertexMetrics().get(jobVertexId)
                            : collectedMetrics.getGlobalMetrics();
            double num = metrics.getOrDefault(metric, Double.NaN);
            if (Double.isNaN(num)) {
                continue;
            }
            if (Double.isInfinite(num)) {
                anyInfinite = true;
                continue;
            }

            sum += num;
            n++;
        }
        if (n == 0) {
            return anyInfinite ? Double.POSITIVE_INFINITY : Double.NaN;
        }
        return n < minElements ? Double.NaN : sum / n;
    }

    /**
     * Compute the In/Out ratio between the (from, to) vertices. The rate estimates the number of
     * output records produced to the downstream vertex for every input received for the upstream
     * vertex. For example output ratio 2.0 means that we produce approximately 2 outputs to the
     * "to" vertex for every 1 input received in the "from" vertex.
     *
     * @param from Upstream vertex
     * @param to Downstream vertex
     * @param topology
     * @param metricsHistory
     * @return Output ratio
     */
    @VisibleForTesting
    protected static double computeEdgeOutputRatio(
            JobVertexID from,
            JobVertexID to,
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {

        double inputRate = getRate(ScalingMetric.NUM_RECORDS_IN, from, metricsHistory);

        double outputRatio = 0;
        // If the input rate is zero, we also need to flatten the output rate.
        // Otherwise, the OUTPUT_RATIO would be outrageously large, leading to
        // a rapid scale up.
        if (inputRate > 0) {
            double outputRate = computeEdgeDataRate(topology, metricsHistory, from, to);
            if (outputRate > 0) {
                outputRatio = outputRate / inputRate;
            }
        }
        return outputRatio;
    }

    /**
     * Compute how many records flow between two job vertices in the pipeline. Since Flink does not
     * expose any output / data rate metric on an edge level we have to compute this from the vertex
     * level input/output metrics.
     *
     * @param topology
     * @param metricsHistory
     * @param from Upstream vertex
     * @param to Downstream vertex
     * @return Records per second data rate between the two vertices
     */
    @VisibleForTesting
    protected static double computeEdgeDataRate(
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID from,
            JobVertexID to) {

        var toVertexInputs = topology.get(to).getInputs().keySet();
        // Case 1: Downstream vertex has single input (from) so we can use the most reliable num
        // records in
        if (toVertexInputs.size() == 1) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate for single input downstream task", from, to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory);
        }

        // Case 2: Downstream vertex has only inputs from upstream vertices which don't have other
        // outputs
        double inputRateFromOtherVertices = 0;
        for (JobVertexID input : toVertexInputs) {
            if (input.equals(from)) {
                // Exclude source edge because we only want to consider other input edges
                continue;
            }
            if (topology.get(input).getOutputs().size() == 1) {
                inputRateFromOtherVertices +=
                        getRate(ScalingMetric.NUM_RECORDS_OUT, input, metricsHistory);
            } else {
                // Output vertex has multiple outputs, cannot use this information...
                inputRateFromOtherVertices = Double.NaN;
                break;
            }
        }
        if (!Double.isNaN(inputRateFromOtherVertices)) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate by subtracting upstream input rates",
                    from,
                    to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory)
                    - inputRateFromOtherVertices;
        }

        // Case 3: We fall back simply to num records out, this is the least reliable
        LOG.debug(
                "Computing edge ({}, {}) data rate by falling back to from num records out",
                from,
                to);
        return getRate(ScalingMetric.NUM_RECORDS_OUT, from, metricsHistory);
    }
}
