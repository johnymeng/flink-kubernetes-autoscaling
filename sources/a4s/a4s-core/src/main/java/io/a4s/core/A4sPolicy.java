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

package io.a4s.core;

import io.a4s.api.EvaluationRequest;
import io.a4s.api.MigrationEstimate;
import io.a4s.api.OperatorConfiguration;
import io.a4s.api.OperatorDecision;
import io.a4s.api.OperatorMetrics;
import io.a4s.api.PlatformCapabilities;
import io.a4s.api.PolicyConfiguration;
import io.a4s.api.PolicyConfiguration.Mode;
import io.a4s.api.ScalingPlan;
import io.a4s.api.ScalingPlan.Action;
import io.a4s.api.Worker;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class A4sPolicy implements ScalingPolicy {
    public static final String MODEL_VERSION = "fixed-latency-v1";
    private static final long BYTES_PER_MIB = 1024L * 1024L;
    private final MigrationCostModel migrationModel;
    private final String modelVersion;
    private final Function<PolicyConfiguration, PerformanceModel> performanceModels;

    public A4sPolicy() {
        this((current, candidate) -> Optional.empty());
    }

    public A4sPolicy(MigrationCostModel migrationModel) {
        this(migrationModel, MODEL_VERSION, FixedLatencyModel::new);
    }

    public A4sPolicy(
            MigrationCostModel migrationModel,
            String modelVersion,
            Function<PolicyConfiguration, PerformanceModel> performanceModels) {
        this.migrationModel = Objects.requireNonNull(migrationModel);
        this.modelVersion = Objects.requireNonNull(modelVersion);
        this.performanceModels = Objects.requireNonNull(performanceModels);
    }

    @Override
    public ScalingPlan evaluate(EvaluationRequest request) {
        var snapshot = request.snapshot;
        var hold = snapshotHold(request);
        if (hold != null) {
            return hold;
        }

        var model = performanceModels.apply(request.policy);
        var frontiers = new ArrayList<Frontier>(snapshot.operators.size());
        var selected = new ArrayList<OperatorDecision>(snapshot.operators.size());
        for (var operator : snapshot.operators) {
            String issue = operatorMetricsIssue(request, operator);
            if (issue != null) {
                return hold(request, Action.INSUFFICIENT_METRICS, issue, 0);
            }
            var frontier = new Frontier(request, operator, model);
            if (frontier.curve != null && !frontier.curve.hasMeasurements()) {
                return hold(
                        request,
                        Action.INSUFFICIENT_METRICS,
                        "Missing MRC for " + operator.operatorId,
                        0);
            }
            var first = frontier.atOrAbove(operator.startingParallelism);
            if (first == null) {
                return hold(
                        request,
                        Action.INFEASIBLE_CONFIGURATION,
                        "No supported configuration for " + operator.operatorId,
                        0);
            }
            frontiers.add(frontier);
            selected.add(first);
        }

        for (int attempt = 1; attempt <= request.policy.maxAttempts; attempt++) {
            if (withinBudgets(request, selected)) {
                var decisions = new LinkedHashMap<String, OperatorDecision>();
                for (int i = 0; i < selected.size(); i++) {
                    decisions.put(snapshot.operators.get(i).operatorId, selected.get(i));
                }
                return recommend(request, decisions, attempt);
            }
            if (attempt == request.policy.maxAttempts || !advance(frontiers, selected)) {
                return hold(
                        request,
                        Action.INFEASIBLE_CONFIGURATION,
                        "Could not verify a placement within resource budgets",
                        attempt);
            }
        }
        throw new IllegalStateException("Invalid attempt limit");
    }

    private ScalingPlan snapshotHold(EvaluationRequest request) {
        var snapshot = request.snapshot;
        var constraints = request.constraints;
        long expiry = expiry(request);
        if (!snapshot.metricsComplete || request.evaluatedAtMillis > expiry) {
            return hold(
                    request,
                    Action.INSUFFICIENT_METRICS,
                    "Incomplete or expired measurement window",
                    0);
        }
        if (snapshot.reconfigurationInProgress || request.previousState.pendingDecisionId != null) {
            return hold(request, Action.NO_CHANGE, "Previous reconfiguration is unresolved", 0);
        }
        if (snapshot.lastReconfigurationMillis != null
                && request.evaluatedAtMillis - snapshot.lastReconfigurationMillis
                        < constraints.cooldownMillis) {
            return hold(request, Action.NO_CHANGE, "Reconfiguration cooldown", 0);
        }
        if (constraints.requiresPlacement
                || constraints.latencyObjectiveMillis != null
                || constraints.backlogRecoveryMillis != null) {
            return hold(
                    request, Action.UNSUPPORTED_ACTION, "Requested objective has no v1 model", 0);
        }
        return null;
    }

    private static String operatorMetricsIssue(
            EvaluationRequest request, OperatorMetrics operator) {
        if (operator.cacheSensitive && operator.targetRecordsPerSecond == null) {
            return "Missing demand for " + operator.operatorId;
        }
        if (!hasUsableCapacityAxis(request, operator)) {
            return "Actual cache capacity is unavailable for " + operator.operatorId;
        }
        var histogram = operator.histogram;
        if (histogram != null
                && (histogram.startMillis < request.snapshot.windowStartMillis
                        || histogram.endMillis != request.snapshot.windowEndMillis)) {
            return "Histogram does not cover the current measurement end for "
                    + operator.operatorId;
        }
        return null;
    }

    private static boolean hasUsableCapacityAxis(
            EvaluationRequest request, OperatorMetrics operator) {
        return !operator.cacheSensitive
                || request.policy.mode == Mode.LEGACY_SLOT_RESERVATION
                || operator.missRateCurve.isEmpty()
                || operator.capacityAxis == OperatorMetrics.CapacityAxis.MEASURED_CACHE_BYTES;
    }

    private ScalingPlan recommend(
            EvaluationRequest request, Map<String, OperatorDecision> decisions, int attempts) {
        var capabilities = request.snapshot.capabilities;
        boolean changes = false;
        boolean preserveState = false;
        for (var operator : request.snapshot.operators) {
            var current = operator.current;
            var target = decisions.get(operator.operatorId).target;
            boolean horizontal = current.parallelism != target.parallelism;
            boolean cache = !Objects.equals(current.cachePerTaskBytes, target.cachePerTaskBytes);
            boolean managed =
                    !Objects.equals(
                            current.managedMemoryPerTaskBytes, target.managedMemoryPerTaskBytes);
            if (horizontal && !capabilities.supports(PlatformCapabilities.Action.PARALLELISM)
                    || cache && !capabilities.supports(PlatformCapabilities.Action.CACHE_PER_TASK)
                    || managed
                            && !capabilities.supports(
                                    PlatformCapabilities.Action.MANAGED_MEMORY_PER_TASK)) {
                return hold(
                        request,
                        Action.UNSUPPORTED_ACTION,
                        "Required scaling action is unsupported",
                        attempts);
            }
            if (horizontal || cache || managed) {
                changes = true;
                preserveState |= !Boolean.FALSE.equals(operator.stateful);
            }
        }
        if (!changes) {
            return hold(
                    request,
                    Action.NO_CHANGE,
                    "Current allocation matches the recommendation",
                    attempts);
        }
        if (preserveState
                && !capabilities.supports(PlatformCapabilities.Action.STATEFUL_RECOVERY)) {
            return hold(
                    request,
                    Action.UNSUPPORTED_ACTION,
                    "State-preserving execution is unsupported",
                    attempts);
        }
        MigrationEstimate migration =
                migrationModel.predict(request.snapshot, Map.copyOf(decisions)).orElse(null);
        Long limit = request.constraints.maxDowntimeMillis;
        if (limit != null
                && (migration == null
                        || migration.downtimeMillis == null
                        || migration.downtimeMillis > limit)) {
            return hold(
                    request,
                    Action.INFEASIBLE_CONFIGURATION,
                    "Downtime objective cannot be verified",
                    attempts);
        }
        return plan(
                request,
                Action.RESCALE,
                decisions,
                migration,
                preserveState,
                attempts,
                "Measured support and configured resource bounds");
    }

    private static boolean withinBudgets(
            EvaluationRequest request, List<OperatorDecision> selected) {
        long remainingCache =
                request.constraints.totalCacheBudgetBytes == null
                        ? Long.MAX_VALUE
                        : request.constraints.totalCacheBudgetBytes;
        boolean limitCache = request.constraints.totalCacheBudgetBytes != null;
        var cpuBudget = request.constraints.cpuBudgetCores;
        BigDecimal remainingCpu = cpuBudget == null ? null : BigDecimal.valueOf(cpuBudget);
        for (var decision : selected) {
            var target = decision.target;
            if (!withinManagedBudget(request, decision)) {
                return false;
            }
            if (target.cachePerTaskBytes != null
                    && (target.cachePerTaskBytes < request.constraints.minCachePerTaskBytes
                            || target.cachePerTaskBytes
                                    > request.constraints.maxCachePerTaskBytes)) {
                return false;
            }
            if (limitCache) {
                if (decision.targetTotalCacheBytes == null
                        || decision.targetTotalCacheBytes > remainingCache) {
                    return false;
                }
                remainingCache -= decision.targetTotalCacheBytes;
            }
            if (remainingCpu != null) {
                if (target.cpuCoresPerTask == null) {
                    return false;
                }
                remainingCpu =
                        remainingCpu.subtract(
                                BigDecimal.valueOf(target.cpuCoresPerTask)
                                        .multiply(BigDecimal.valueOf(target.parallelism)));
                if (remainingCpu.signum() < 0) {
                    return false;
                }
            }
        }
        return withinWorkerCapacity(request, selected);
    }

    private static boolean withinManagedBudget(
            EvaluationRequest request, OperatorDecision decision) {
        var max = request.constraints.maxManagedMemoryPerTaskBytes;
        if (max == null) {
            return true;
        }
        boolean legacy = request.policy.mode == Mode.LEGACY_SLOT_RESERVATION;
        var minimum = decision.modeledMinimumTotalCacheBytes;
        if (legacy && minimum == null) {
            return true; // Legacy limit governs changed managed reservations only.
        }
        var managed = decision.target.managedMemoryPerTaskBytes;
        return managed != null
                && managed <= max
                && (!legacy || minimum <= max - request.policy.managedMemoryBaseBytes);
    }

    private static boolean withinWorkerCapacity(
            EvaluationRequest request, List<OperatorDecision> selected) {
        var workers = request.snapshot.workers;
        if (workers.isEmpty()) {
            return true;
        }
        var tasks = new ArrayList<TaskDemand>(selected.size());
        for (int i = 0; i < selected.size(); i++) {
            var operator = request.snapshot.operators.get(i);
            var target = selected.get(i).target;
            if (operator.telemetry == null
                    || operator.telemetry.allocatedMemoryPerTaskBytes == null
                    || target.cpuCoresPerTask == null) {
                return false;
            }
            long perTaskBytes = operator.telemetry.allocatedMemoryPerTaskBytes;
            // Credit no reductions, and reserve for both increases if the platform has not
            // declared whether its managed and cache allocations overlap.
            long managedGrowth =
                    growth(
                            operator.current.managedMemoryPerTaskBytes,
                            target.managedMemoryPerTaskBytes);
            long cacheGrowth = growth(operator.current.cachePerTaskBytes, target.cachePerTaskBytes);
            if (managedGrowth > Long.MAX_VALUE - perTaskBytes
                    || cacheGrowth > Long.MAX_VALUE - perTaskBytes - managedGrowth) {
                return false;
            }
            tasks.add(
                    new TaskDemand(
                            perTaskBytes + managedGrowth + cacheGrowth,
                            BigDecimal.valueOf(target.cpuCoresPerTask),
                            target.parallelism));
        }
        tasks.sort(
                Comparator.comparingLong((TaskDemand task) -> task.bytes)
                        .thenComparing(task -> task.cpuCores)
                        .reversed());
        var balances =
                workers.stream()
                        .sorted(Comparator.comparing(worker -> worker.workerId))
                        .map(WorkerBalance::new)
                        .collect(Collectors.toList());
        // Produce a placement witness. A greedy packing can decline a feasible plan,
        // but it cannot approve a plan whose tasks do not fit together.
        for (var task : tasks) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("A4S evaluation interrupted");
            }
            int remaining = task.count;
            for (var worker : balances) {
                remaining -= worker.reserve(task, remaining);
                if (remaining == 0) {
                    break;
                }
            }
            if (remaining > 0) {
                return false;
            }
        }
        return true;
    }

    private static final class TaskDemand {
        private final long bytes;
        private final BigDecimal cpuCores;
        private final int count;

        private TaskDemand(long bytes, BigDecimal cpuCores, int count) {
            this.bytes = bytes;
            this.cpuCores = cpuCores;
            this.count = count;
        }
    }

    private static final class WorkerBalance {
        private long bytesLeft;
        private BigDecimal cpuLeft;

        private WorkerBalance(Worker worker) {
            bytesLeft = worker.allocatableBytes;
            cpuLeft = BigDecimal.valueOf(worker.allocatableCpuCores);
        }

        private int reserve(TaskDemand task, int count) {
            // Pack identical subtasks in bulk even for high parallelism.
            int fit = task.bytes == 0 ? count : (int) Math.min(count, bytesLeft / task.bytes);
            if (fit == 0) {
                return 0;
            }
            var cpu = task.cpuCores.multiply(BigDecimal.valueOf(fit));
            if (cpu.compareTo(cpuLeft) > 0) {
                fit = cpuLeft.divideToIntegralValue(task.cpuCores).intValueExact();
                cpu = task.cpuCores.multiply(BigDecimal.valueOf(fit));
            }
            bytesLeft -= task.bytes * fit;
            cpuLeft = cpuLeft.subtract(cpu);
            return fit;
        }
    }

    private static long growth(Long current, Long target) {
        return target == null ? 0 : Math.max(0, target - (current == null ? 0 : current));
    }

    private static boolean advance(List<Frontier> frontiers, List<OperatorDecision> selected) {
        int bestIndex = -1;
        OperatorDecision best = null;
        long bestDelta = Long.MAX_VALUE;
        for (int i = 0; i < selected.size(); i++) {
            var current = selected.get(i);
            if (frontiers.get(i).curve == null) {
                continue;
            }
            var next = frontiers.get(i).atOrAbove(current.target.parallelism + 1);
            if (next == null) {
                continue;
            }
            long delta = next.modeledMinimumTotalCacheBytes - current.modeledMinimumTotalCacheBytes;
            if (best == null || delta < bestDelta) {
                bestIndex = i;
                best = next;
                bestDelta = delta;
            }
        }
        if (bestIndex < 0) {
            return false;
        }
        selected.set(bestIndex, best);
        return true;
    }

    /**
     * Validate an actionable response against the original request before platform revalidation.
     */
    public boolean isApplicable(EvaluationRequest request, ScalingPlan plan, long nowMillis) {
        var snapshot = request.snapshot;
        if (plan.action != Action.RESCALE
                || plan.schemaVersion != request.schemaVersion
                || !plan.snapshotId.equals(snapshot.snapshotId)
                || !plan.jobId.equals(snapshot.jobId)
                || !plan.executionGeneration.equals(snapshot.executionGeneration)
                || !plan.decisionId.equals(decisionId(request))
                || !modelVersion.equals(plan.modelVersion)
                || plan.expiresAtMillis != expiry(request)
                || nowMillis > plan.expiresAtMillis
                || plan.attempts < 1
                || plan.attempts > request.policy.maxAttempts
                || plan.policyState.version != request.previousState.version
                || !Objects.equals(
                        plan.policyState.pendingDecisionId, request.previousState.pendingDecisionId)
                || snapshotHold(request) != null
                || plan.operators.size() != snapshot.operators.size()) {
            return false;
        }

        var selected = new ArrayList<OperatorDecision>(snapshot.operators.size());
        var model = performanceModels.apply(request.policy);
        for (var operator : snapshot.operators) {
            var decision = plan.operators.get(operator.operatorId);
            var current = plan.currentConfiguration.get(operator.operatorId);
            if (decision == null
                    || current == null
                    || !sameAllocation(operator.current, current)
                    || operatorMetricsIssue(request, operator) != null) {
                return false;
            }
            int parallelism = decision.target.parallelism;
            if (parallelism < Math.max(operator.startingParallelism, operator.minParallelism)
                    || parallelism > operator.maxParallelism
                    || !operator.permittedParallelisms.isEmpty()
                            && java.util.Collections.binarySearch(
                                            operator.permittedParallelisms, parallelism)
                                    < 0) {
                return false;
            }
            var frontier = new Frontier(request, operator, model);
            var expected =
                    frontier.curve == null
                            ? frontier.atOrAbove(operator.startingParallelism)
                            : frontier.candidate(parallelism);
            if (expected == null
                    || !sameAllocation(expected.target, decision.target)
                    || !Objects.equals(
                            expected.modeledMinimumTotalCacheBytes,
                            decision.modeledMinimumTotalCacheBytes)
                    || !Objects.equals(
                            expected.predictedRecordsPerSecond,
                            decision.predictedRecordsPerSecond)) {
                return false;
            }
            selected.add(decision);
        }
        var checked = recommend(request, plan.operators, plan.attempts);
        return withinBudgets(request, selected)
                && checked.action == Action.RESCALE
                && plan.requiresStatePreservation == checked.requiresStatePreservation;
    }

    private static boolean sameAllocation(
            OperatorConfiguration first, OperatorConfiguration second) {
        return first.parallelism == second.parallelism
                && Objects.equals(first.cachePerTaskBytes, second.cachePerTaskBytes)
                && Objects.equals(first.managedMemoryPerTaskBytes, second.managedMemoryPerTaskBytes)
                && Objects.equals(first.cpuCoresPerTask, second.cpuCoresPerTask);
    }

    public static long expiry(EvaluationRequest request) {
        long end = request.snapshot.windowEndMillis;
        long lifetime = request.constraints.maxSnapshotAgeMillis;
        return lifetime > Long.MAX_VALUE - end ? Long.MAX_VALUE : end + lifetime;
    }

    public static String decisionId(EvaluationRequest request) {
        var snapshot = request.snapshot;
        String key =
                snapshot.jobId.length()
                        + ":"
                        + snapshot.jobId
                        + snapshot.executionGeneration.length()
                        + ":"
                        + snapshot.executionGeneration
                        + snapshot.snapshotId;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private ScalingPlan hold(
            EvaluationRequest request, Action action, String rationale, int attempts) {
        return plan(request, action, Map.of(), null, false, attempts, rationale);
    }

    private ScalingPlan plan(
            EvaluationRequest request,
            Action action,
            Map<String, OperatorDecision> decisions,
            MigrationEstimate migration,
            boolean preserveState,
            int attempts,
            String rationale) {
        var snapshot = request.snapshot;
        var current = new LinkedHashMap<String, OperatorConfiguration>();
        snapshot.operators.forEach(operator -> current.put(operator.operatorId, operator.current));
        return new ScalingPlan(
                request.schemaVersion,
                decisionId(request),
                snapshot.snapshotId,
                snapshot.jobId,
                snapshot.executionGeneration,
                action,
                current,
                decisions,
                migration,
                rationale,
                modelVersion,
                expiry(request),
                preserveState,
                attempts,
                request.previousState);
    }

    private static final class Frontier {
        private final EvaluationRequest request;
        private final OperatorMetrics operator;
        private final MemoryParallelismCurve curve;

        private Frontier(
                EvaluationRequest request, OperatorMetrics operator, PerformanceModel model) {
            this.request = request;
            this.operator = operator;
            this.curve =
                    operator.cacheSensitive ? new MemoryParallelismCurve(operator, model) : null;
        }

        private OperatorDecision atOrAbove(int startingParallelism) {
            int start = Math.max(startingParallelism, operator.minParallelism);
            if (!operator.permittedParallelisms.isEmpty()) {
                int index =
                        java.util.Collections.binarySearch(operator.permittedParallelisms, start);
                for (int i = index >= 0 ? index : -index - 1;
                        i < operator.permittedParallelisms.size();
                        i++) {
                    var candidate = candidate(operator.permittedParallelisms.get(i));
                    if (candidate != null) {
                        return candidate;
                    }
                }
            } else {
                for (int parallelism = start;
                        parallelism <= operator.maxParallelism;
                        parallelism++) {
                    var candidate = candidate(parallelism);
                    if (candidate != null) {
                        return candidate;
                    }
                }
            }
            return null;
        }

        private OperatorDecision candidate(int parallelism) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("A4S evaluation interrupted");
            }
            var current = operator.current;
            Long minimum = null;
            Double throughput = null;
            Long cache = current.cachePerTaskBytes;
            Long managed = current.managedMemoryPerTaskBytes;
            if (curve != null) {
                var required = curve.minimumCacheBytes(parallelism);
                if (required.isEmpty() || required.getAsLong() < 0) {
                    return null;
                }
                minimum = required.getAsLong();
                if (request.policy.mode == Mode.LEGACY_SLOT_RESERVATION) {
                    cache = null;
                    managed = legacyManagedMemoryBytes(request, minimum);
                    if (managed == null) {
                        return null;
                    }
                    // This reservation convention supplies no physical cache capacity guarantee.
                } else {
                    long perTask = minimum / parallelism + (minimum % parallelism == 0 ? 0 : 1);
                    cache = Math.max(perTask, request.constraints.minCachePerTaskBytes);
                }
            }
            if (cache != null && cache > Long.MAX_VALUE / parallelism) {
                return null;
            }
            var target =
                    new OperatorConfiguration(parallelism, cache, managed, current.cpuCoresPerTask);
            if (curve != null && request.policy.mode == Mode.CACHE) {
                var prediction = curve.predict(target);
                if (prediction.isEmpty()) {
                    return null;
                }
                double rate = prediction.getAsDouble();
                if (!Double.isFinite(rate) || rate < operator.targetRecordsPerSecond) {
                    return null;
                }
                throughput = rate;
            }
            return new OperatorDecision(
                    target, target.totalCacheBytes(), minimum, throughput, null);
        }
    }

    private static Long legacyManagedMemoryBytes(EvaluationRequest request, long requiredBytes) {
        long base = request.policy.managedMemoryBaseBytes;
        Long override = request.policy.managedMemoryOverrideBytes;
        if (override != null) {
            return override;
        }
        if (requiredBytes > Long.MAX_VALUE - base) {
            return null;
        }
        long raw = requiredBytes + base;
        if (raw == 0) {
            return null;
        }
        long mib = raw / BYTES_PER_MIB + (raw % BYTES_PER_MIB != 0 ? 1 : 0);
        return mib > Long.MAX_VALUE / BYTES_PER_MIB ? null : mib * BYTES_PER_MIB;
    }
}
