package org.apache.flink.runtime.jobgraph.justin;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class JustinVertexResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_NAME_PARALLELISM = "parallelism";
    private static final String FIELD_NAME_RESOURCE_PROFILE = "resourceProfile";
    private static final String FIELD_NAME_APPLIED_RESOURCE_PROFILE = "appliedResourceProfile";
    private static final String FIELD_NAME_DEFAULT_SLOT = "defaultSlot";

    public static class Parallelism implements Serializable {

        private static final String FIELD_NAME_LOWER_BOUND = "lowerBound";
        private static final String FIELD_NAME_UPPER_BOUND = "upperBound";

        @JsonProperty(FIELD_NAME_LOWER_BOUND)
        private final int lowerBound;

        @JsonProperty(FIELD_NAME_UPPER_BOUND)
        private final int upperBound;

        @JsonCreator
        public Parallelism(
                @JsonProperty(FIELD_NAME_LOWER_BOUND) int lowerBound,
                @JsonProperty(FIELD_NAME_UPPER_BOUND) int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public int getLowerBound() {
            return lowerBound;
        }

        public int getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Parallelism that = (Parallelism) o;
            return lowerBound == that.lowerBound && upperBound == that.upperBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowerBound, upperBound);
        }

        @Override
        public String toString() {
            return "Parallelism{" + "lowerBound=" + lowerBound + ", upperBound=" + upperBound + '}';
        }
    }

    @JsonProperty(FIELD_NAME_PARALLELISM)
    private final Parallelism parallelism;

    @JsonProperty(FIELD_NAME_RESOURCE_PROFILE)
    private final ResourceProfile resourceProfile;

    @Nullable private final ResourceProfile appliedResourceProfile;
    private final boolean defaultSlot;

    public JustinVertexResourceRequirements(
            Parallelism parallelism, ResourceProfile resourceProfile) {
        this(parallelism, resourceProfile, null);
    }

    public JustinVertexResourceRequirements(
            Parallelism parallelism,
            ResourceProfile resourceProfile,
            @Nullable ResourceProfile appliedResourceProfile) {
        this(parallelism, resourceProfile, appliedResourceProfile, false);
    }

    @JsonCreator
    public JustinVertexResourceRequirements(
            @JsonProperty(FIELD_NAME_PARALLELISM) Parallelism parallelism,
            @JsonProperty(FIELD_NAME_RESOURCE_PROFILE) ResourceProfile resourceProfile,
            @JsonProperty(FIELD_NAME_APPLIED_RESOURCE_PROFILE) @Nullable
                    ResourceProfile appliedResourceProfile,
            @JsonProperty(FIELD_NAME_DEFAULT_SLOT) @Nullable Boolean defaultSlot) {
        this.parallelism = checkNotNull(parallelism);
        this.resourceProfile = resourceProfile;
        this.appliedResourceProfile = appliedResourceProfile;
        this.defaultSlot = Boolean.TRUE.equals(defaultSlot);
    }

    public Parallelism getParallelism() {
        return parallelism;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    /** The scheduler should allocate a default slot. The numeric profile is the legacy GET value. */
    @JsonProperty(FIELD_NAME_DEFAULT_SLOT)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isDefaultSlot() {
        return defaultSlot;
    }

    /** Slot profile confirmed for all running subtasks, when available on a GET response. */
    @Nullable
    @JsonProperty(FIELD_NAME_APPLIED_RESOURCE_PROFILE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ResourceProfile getAppliedResourceProfile() {
        return appliedResourceProfile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JustinVertexResourceRequirements that = (JustinVertexResourceRequirements) o;
        return defaultSlot == that.defaultSlot
                && parallelism.equals(that.parallelism)
                && Objects.equals(resourceProfile, that.resourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parallelism, resourceProfile, defaultSlot);
    }

    @Override
    public String toString() {
        return "JustinVertexResourceRequirements{"
                + "parallelism="
                + parallelism
                + ", resourceProfile="
                + resourceProfile
                + '}';
    }
}
