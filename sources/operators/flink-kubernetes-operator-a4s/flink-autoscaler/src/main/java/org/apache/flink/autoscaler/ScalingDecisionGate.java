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

/** Prevents a new scaling decision while an earlier decision is being realized. */
@FunctionalInterface
public interface ScalingDecisionGate<Context> {

    boolean blocksNewDecision(Context context) throws Exception;

    /** Records a failure that occurred while applying an allowed scaling decision. */
    default void handleScalingFailure(Context context, Throwable failure) throws Exception {}

    /** Returns whether reconciliation must stop before applying deployment-spec changes. */
    default boolean blocksScalingApplication(Context context) throws Exception {
        return false;
    }

    static <Context> ScalingDecisionGate<Context> open() {
        return context -> false;
    }
}
