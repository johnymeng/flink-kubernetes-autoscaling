/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.rest.handler.job.justin;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobgraph.justin.JustinVertexResourceRequirements;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsMessageParameters.IncludeAppliedProfileParameter;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsMessageParameters.IncludeDefaultSlotParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler for reading current {@link org.apache.flink.runtime.jobgraph.JobResourceRequirements
 * resource requirements} of a given job.
 */
public class JustinResourceRequirementsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                JustinResourceRequirementsBody,
                JustinResourceRequirementsMessageParameters> {
    public JustinResourceRequirementsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                JustinResourceRequirementsHeaders.INSTANCE);
    }

    @Override
    protected CompletableFuture<JustinResourceRequirementsBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final boolean includeApplied =
                request.getQueryParameter(IncludeAppliedProfileParameter.class).contains(true);
        final boolean includeDefault =
                request.getQueryParameter(IncludeDefaultSlotParameter.class).contains(true);
        return gateway.requestJustinResourceRequirements(jobId)
                .thenApply(
                        requirements ->
                                new JustinResourceRequirementsBody(
                                        requestedFields(
                                                requirements, includeApplied, includeDefault)));
    }

    private static JustinResourceRequirements requestedFields(
            JustinResourceRequirements requirements, boolean includeApplied, boolean includeDefault) {
        var projected = new HashMap<JobVertexID, JustinVertexResourceRequirements>();
        requirements
                .getJobVertexParallelisms()
                .forEach(
                        (vertexId, requirement) ->
                                projected.put(
                                        vertexId,
                                        new JustinVertexResourceRequirements(
                                                requirement.getParallelism(),
                                                requirement.getResourceProfile(),
                                                includeApplied
                                                        ? requirement.getAppliedResourceProfile()
                                                        : null,
                                                includeDefault && requirement.isDefaultSlot())));
        return new JustinResourceRequirements(projected);
    }
}
