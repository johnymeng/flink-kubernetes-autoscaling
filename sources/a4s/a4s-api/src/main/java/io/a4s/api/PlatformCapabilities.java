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

package io.a4s.api;

import java.beans.ConstructorProperties;
import java.util.Map;

/** Action support as discovered or declared by the platform adapter. */
public final class PlatformCapabilities {
    public final Map<Action, Support> actions;

    @ConstructorProperties({"actions"})
    public PlatformCapabilities(Map<Action, Support> actions) {
        actions = Map.copyOf(actions);
        this.actions = actions;
    }

    public enum Action {
        PARALLELISM,
        MANAGED_MEMORY_PER_TASK,
        CACHE_PER_TASK,
        CACHE_DYNAMIC_RESIZE,
        OPERATOR_MEMORY,
        SLOT_MEMORY,
        PLACEMENT,
        STATEFUL_RECOVERY,
        IN_PLACE_RECONFIGURATION,
        REMOTE_STATE,
        ASYNC_STATE_ACCESS,
        WORKER_PROVISIONING,
        WORKER_DEPROVISIONING
    }

    public enum Support {
        UNSUPPORTED,
        WITH_RESTART,
        WITHOUT_RESTART
    }

    public boolean supports(Action action) {
        return actions.getOrDefault(action, Support.UNSUPPORTED) != Support.UNSUPPORTED;
    }
}
