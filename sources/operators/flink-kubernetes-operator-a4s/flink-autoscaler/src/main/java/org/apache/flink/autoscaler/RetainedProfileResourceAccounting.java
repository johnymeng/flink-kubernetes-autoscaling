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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsInfo;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Accounts for explicit, per-subtask profiles against the deployed fine-grained worker geometry. */
final class RetainedProfileResourceAccounting {

    private RetainedProfileResourceAccounting() {}

    static Optional<Allocation> estimate(
            JobID job, WorkerSnapshot snapshot, Map<ResourceProfile, Long> running,
            Map<ResourceProfile, Long> requested) {
        if (snapshot.workers.isEmpty() || running.isEmpty()
                || !running.keySet().equals(requested.keySet())) {
            return Optional.empty();
        }
        try {
            var config = new Configuration(snapshot.deployed);
            if (!isNativeApplication(config, snapshot.workers)
                    || !config.get(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT)
                    || config.get(ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM) != 0) {
                return Optional.empty();
            }

            // Same CPU fallback as KubernetesWorkerResourceSpecFactory.
            var cpu = TaskExecutorProcessUtils.getCpuCoresWithFallback(
                    config, config.getDouble("kubernetes.taskmanager.cpu.amount", -1));
            config.set(TaskManagerOptions.CPU_CORES, cpu.getValue().doubleValue());
            var defaultSpec = TaskExecutorProcessUtils.processSpecFromConfig(config);
            var process = TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                    config, WorkerResourceSpec.fromTaskExecutorProcessSpec(defaultSpec));
            var capacity = ResourceProfile.newBuilder()
                    .setCpuCores(cpu)
                    .setTaskHeapMemory(process.getTaskHeapSize())
                    .setTaskOffHeapMemory(process.getTaskOffHeapSize())
                    .setManagedMemory(process.getManagedMemorySize())
                    .setNetworkMemory(process.getNetworkMemSize())
                    .build();
            MemorySize processMemory = process.getTotalProcessMemorySize();
            if (!process.getExtendedResources().isEmpty()
                    || cpu.getValue().movePointRight(3).stripTrailingZeros().scale() > 0
                    || processMemory.getBytes() <= 0 || !wholeMebibytes(processMemory)
                    || !representableMebibytes(capacity.getTaskHeapMemory())
                    || !representableMebibytes(capacity.getTaskOffHeapMemory())
                    || !representableMebibytes(capacity.getManagedMemory())
                    || !representableMebibytes(capacity.getNetworkMemory())
                    || !matchesWorkers(job, capacity, processMemory, snapshot.workers, running)) {
                return Optional.empty();
            }

            int currentWorkers = snapshot.workers.size();
            long needed = estimateRequestedWorkers(capacity, running, requested, currentWorkers);
            if (needed > Integer.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(new Allocation(currentWorkers, (int) needed, cpu.getValue(), processMemory));
        } catch (IllegalConfigurationException | IllegalArgumentException | IllegalStateException
                | ArithmeticException e) {
            return Optional.empty();
        }
    }

    private static boolean isNativeApplication(
            Configuration deployed, List<TaskManagerDetailsInfo> workers) {
        String target = deployed.get(DeploymentOptions.TARGET);
        if ("kubernetes-application".equals(target)) {
            return true;
        }
        // ApplicationClusterEntryPoint installs the embedded executor inside the JobManager.
        // Confirm the native Kubernetes RM's cluster-specific pod IDs before accepting it.
        String clusterId = deployed.getString("kubernetes.cluster-id", "");
        String namespace = deployed.getString("kubernetes.namespace", "");
        if (!"embedded".equals(target) || clusterId.isBlank() || namespace.isBlank()) {
            return false;
        }
        var workerId = Pattern.compile(Pattern.quote(clusterId) + "-taskmanager-[1-9][0-9]*-[1-9][0-9]*");
        return workers.stream().allMatch(
                worker -> workerId.matcher(worker.getResourceId().getResourceIdString()).matches());
    }

    private static boolean wholeMebibytes(MemorySize size) {
        return size.getBytes() % (1L << 20) == 0 && representableMebibytes(size);
    }

    private static boolean representableMebibytes(MemorySize size) {
        // TaskManager REST reports slot resource dimensions in whole MiB.
        return (size.getBytes() >> 20) <= Integer.MAX_VALUE;
    }

    private static boolean matchesWorkers(
            JobID job, ResourceProfile capacity, MemorySize processMemory,
            List<TaskManagerDetailsInfo> workers, Map<ResourceProfile, Long> running) {
        var identities = new HashSet<>();
        var profiles = new HashMap<ResourceProfileInfo, ResourceProfile>();
        var occupied = new HashMap<ResourceProfile, Long>();
        running.keySet().forEach(
                profile -> profiles.put(ResourceProfileInfo.fromResrouceProfile(profile), profile));
        if (profiles.size() != running.size()) {
            return false;
        }
        var expectedTotal = ResourceProfileInfo.fromResrouceProfile(capacity);
        for (var worker : workers) {
            var memory = worker.getMemoryConfiguration();
            if (!identities.add(worker.getResourceId()) || worker.getBlocked()
                    || !expectedTotal.equals(worker.getTotalResource())
                    || !Objects.equals(memory.getTotalProcessMemory(), processMemory.getBytes())
                    || !Objects.equals(memory.getTaskHeap(), capacity.getTaskHeapMemory().getBytes())
                    || !Objects.equals(memory.getTaskOffHeap(), capacity.getTaskOffHeapMemory().getBytes())
                    || !Objects.equals(memory.getManagedMemoryTotal(), capacity.getManagedMemory().getBytes())
                    || !Objects.equals(memory.getNetworkMemory(), capacity.getNetworkMemory().getBytes())) {
                return false;
            }
            var free = capacity;
            for (var slot : worker.getAllocatedSlots()) {
                var profile = profiles.get(slot.getResource());
                if (!job.equals(slot.getJobId()) || profile == null
                        || !free.allFieldsNoLessThan(profile)) {
                    return false;
                }
                free = free.subtract(profile);
                occupied.merge(profile, 1L, Math::addExact);
            }
            // Compare the free DTO against independently floored bytes after subtraction.
            // Retained profiles are whole MiB. Any hidden extra allocation rounded out of the
            // slot DTOs can consume at most the worker's byte remainder in each dimension,
            // or the reported free MiB would be lower. That remainder cannot fit another
            // whole-MiB request or change the worker-count bound.
            if (!ResourceProfileInfo.fromResrouceProfile(free).equals(worker.getFreeResource())) {
                return false;
            }
        }
        return occupied.equals(running);
    }

    static long estimateRequestedWorkers(
            ResourceProfile capacity, Map<ResourceProfile, Long> running,
            Map<ResourceProfile, Long> requested, int currentWorkers) {
        long guaranteedCopiesPerWorker = Long.MAX_VALUE;
        long totalRequested = 0;
        for (var requirement : requested.entrySet()) {
            var profile = requirement.getKey();
            long count = requirement.getValue();
            long alreadyAllocated = running.get(profile);
            long perWorker = copiesPerWorker(capacity, profile);
            if (perWorker <= 0 || alreadyAllocated <= 0 || count < alreadyAllocated) {
                throw new IllegalArgumentException("Retained profile cannot be accounted for");
            }
            guaranteedCopiesPerWorker = Math.min(guaranteedCopiesPerWorker, perWorker);
            totalRequested = Math.addExact(totalRequested, count);
            if (totalRequested > Integer.MAX_VALUE) {
                throw new ArithmeticException("Too many requested slots");
            }
        }
        // k of every single profile fits, so k times the largest request in each dimension
        // fits. Any k-profile mixture therefore fits. Each old or pending worker with fewer
        // than k slots can accept the next request. The fine-grained allocator only opens
        // another worker once it cannot match on any of them. This bounds the worker count
        // even when the native restart releases and repacks every slot in a different order.
        return Math.max(currentWorkers, ceilDivide(totalRequested, guaranteedCopiesPerWorker));
    }

    private static long ceilDivide(long count, long perWorker) {
        return count / perWorker + (count % perWorker == 0 ? 0 : 1);
    }

    private static long copiesPerWorker(ResourceProfile capacity, ResourceProfile profile) {
        if (profile.getCpuCores().getValue().signum() <= 0) {
            return 0;
        }
        long cpuCopies = capacity.getCpuCores().getValue()
                .divideToIntegralValue(profile.getCpuCores().getValue())
                .min(BigDecimal.valueOf(Integer.MAX_VALUE)).longValueExact();
        return Math.min(cpuCopies, Math.min(
                Math.min(copies(capacity.getTaskHeapMemory(), profile.getTaskHeapMemory()),
                        copies(capacity.getTaskOffHeapMemory(), profile.getTaskOffHeapMemory())),
                Math.min(copies(capacity.getManagedMemory(), profile.getManagedMemory()),
                        copies(capacity.getNetworkMemory(), profile.getNetworkMemory()))));
    }

    private static long copies(MemorySize capacity, MemorySize requirement) {
        return requirement.getBytes() == 0
                ? Long.MAX_VALUE : capacity.getBytes() / requirement.getBytes();
    }

    static final class WorkerSnapshot {
        private final Configuration deployed;
        private final List<TaskManagerDetailsInfo> workers;

        WorkerSnapshot(Configuration deployed, List<TaskManagerDetailsInfo> workers) {
            this.deployed = deployed;
            this.workers = workers;
        }
    }

    static final class Allocation {
        final int currentWorkers;
        final int requestedWorkers;
        final BigDecimal cpu;
        final MemorySize processMemory;

        private Allocation(
                int currentWorkers, int requestedWorkers, BigDecimal cpu, MemorySize processMemory) {
            this.currentWorkers = currentWorkers;
            this.requestedWorkers = requestedWorkers;
            this.cpu = cpu;
            this.processMemory = processMemory;
        }
    }
}
