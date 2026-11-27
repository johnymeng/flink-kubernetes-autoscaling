package org.apache.flink.autoscaler.utils.justin;


import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeySerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAnyGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAnySetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Body for change job requests. */
@FlinkJsonSchema.AdditionalFields(type = JustinVertexResourceRequirements.class)
public class JustinResourceRequirementsBody implements RequestBody, ResponseBody {

    @Nullable
    @JsonProperty("executionToken")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String executionToken;

    @Nullable
    @JsonProperty("statefulRescaleAvailable")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean statefulRescaleAvailable;

    @JsonAnySetter
    @JsonAnyGetter
    @JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
    @JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class)
    private final Map<JobVertexID, JustinVertexResourceRequirements> justinVertexResourceRequirements;

    public JustinResourceRequirementsBody() {
        this(null);
    }

    public JustinResourceRequirementsBody(
            @Nullable JustinResourceRequirements justinVertexResourceRequirements) {
        this(justinVertexResourceRequirements, null);
    }

    public JustinResourceRequirementsBody(
            @Nullable JustinResourceRequirements justinVertexResourceRequirements,
            @Nullable String executionToken) {
        this(justinVertexResourceRequirements, executionToken, null);
    }

    public JustinResourceRequirementsBody(
            @Nullable JustinResourceRequirements justinVertexResourceRequirements,
            @Nullable String executionToken,
            @Nullable Boolean statefulRescaleAvailable) {
        this.executionToken = executionToken;
        this.statefulRescaleAvailable = statefulRescaleAvailable;
        if (justinVertexResourceRequirements != null) {
            this.justinVertexResourceRequirements = justinVertexResourceRequirements.getJobVertexParallelisms();
        } else {
            this.justinVertexResourceRequirements = new HashMap<>();
        }
    }

    @JsonIgnore
    public Optional<JustinResourceRequirements> asJustinResourceRequirements() {
        if (justinVertexResourceRequirements.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new JustinResourceRequirements(justinVertexResourceRequirements));
    }

    @JsonIgnore
    public Optional<String> executionToken() {
        return Optional.ofNullable(executionToken);
    }

    @JsonIgnore
    public boolean hasStatefulRescaleSupport() {
        return Boolean.TRUE.equals(statefulRescaleAvailable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JustinResourceRequirementsBody that = (JustinResourceRequirementsBody) o;
        return Objects.equals(
                        justinVertexResourceRequirements, that.justinVertexResourceRequirements)
                && Objects.equals(executionToken, that.executionToken)
                && Objects.equals(statefulRescaleAvailable, that.statefulRescaleAvailable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                justinVertexResourceRequirements, executionToken, statefulRescaleAvailable);
    }
}
