package org.apache.flink.autoscaler.utils.justin;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Headers for REST request to get details on job's resources. */
public class JustinResourceRequirementsHeaders
        implements RuntimeMessageHeaders<
        EmptyRequestBody, JustinResourceRequirementsBody, JobMessageParameters> {

    public static final JustinResourceRequirementsHeaders INSTANCE =
            new JustinResourceRequirementsHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/justin";

    public JustinResourceRequirementsBody readAppliedRequirements(
            RestClusterClient<?> client, JobID jobId, Duration timeout, boolean includeToken)
            throws Exception {
        var parameters = getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(jobId);
        parameters.includeAppliedProfile.resolve(List.of(true));
        if (includeToken) {
            parameters.includeExecutionToken.resolve(List.of(true));
        }
        return client.sendRequest(this, parameters, EmptyRequestBody.getInstance())
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<JustinResourceRequirementsBody> getResponseClass() {
        return JustinResourceRequirementsBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Request details on the job's resource requirements.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public JustinResourceRequirementsMessageParameters getUnresolvedMessageParameters() {
        return new JustinResourceRequirementsMessageParameters();
    }
}
