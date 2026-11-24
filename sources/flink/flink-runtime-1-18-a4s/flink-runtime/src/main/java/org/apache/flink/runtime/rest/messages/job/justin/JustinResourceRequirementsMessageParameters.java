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

package org.apache.flink.runtime.rest.messages.job.justin;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Arrays;
import java.util.Collection;

/** Parameters for reading Justin resource requirements. */
public class JustinResourceRequirementsMessageParameters extends JobMessageParameters {

    public final IncludeAppliedProfileParameter includeAppliedProfile =
            new IncludeAppliedProfileParameter();
    public final IncludeDefaultSlotParameter includeDefaultSlot = new IncludeDefaultSlotParameter();

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Arrays.asList(includeAppliedProfile, includeDefaultSlot);
    }

    /** Opt-in for returning resource profiles confirmed by the scheduler. */
    public static final class IncludeAppliedProfileParameter extends BooleanParameter {
        public IncludeAppliedProfileParameter() {
            super("include-applied-profile");
        }

        @Override
        public String getDescription() {
            return "Whether to include confirmed applied slot profiles in the response.";
        }
    }

    /** Opt-in for distinguishing the scheduler default from an explicit slot reservation. */
    public static final class IncludeDefaultSlotParameter extends BooleanParameter {
        public IncludeDefaultSlotParameter() {
            super("include-default-slot");
        }

        @Override
        public String getDescription() {
            return "Whether to identify vertices requesting a default slot.";
        }
    }

    /** Optional Boolean query parameter for resource profile metadata. */
    private abstract static class BooleanParameter extends MessageQueryParameter<Boolean> {
        BooleanParameter(String key) {
            super(key, MessageParameterRequisiteness.OPTIONAL);
        }

        @Override
        public Boolean convertStringToValue(String value) {
            return Boolean.valueOf(value);
        }

        @Override
        public String convertValueToString(Boolean value) {
            return value.toString();
        }
    }
}
