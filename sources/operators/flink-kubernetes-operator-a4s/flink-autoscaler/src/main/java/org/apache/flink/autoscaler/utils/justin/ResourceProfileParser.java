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

package org.apache.flink.autoscaler.utils.justin;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

/** Parses the persisted resource profile overrides used by the Justin scaling clients. */
public final class ResourceProfileParser {

    private ResourceProfileParser() {}

    public static ResourceProfile parse(String override) {
        String[] parts = override.split("[{}]+");
        if (parts.length != 2 || !parts[0].equals(ResourceProfile.class.getSimpleName())) {
            return ResourceProfile.UNKNOWN;
        }

        String[] fields = parts[1].split(", ");
        if (fields.length < 5) {
            return ResourceProfile.UNKNOWN;
        }

        return ResourceProfile.newBuilder()
                .setCpuCores(Double.parseDouble(valueOf(fields[0])))
                .setTaskHeapMemory(parseMemory(fields[1]))
                .setTaskOffHeapMemory(parseMemory(fields[2]))
                .setManagedMemory(parseMemory(fields[3]))
                .setNetworkMemory(parseMemory(fields[4]))
                .build();
    }

    private static MemorySize parseMemory(String field) {
        String value = valueOf(field);
        int exactBytes = value.indexOf('(');
        // MemorySize prints its rounded display value followed by the exact byte count.
        return MemorySize.parse(
                exactBytes < 0 ? value : value.substring(exactBytes + 1, value.length() - 1));
    }

    private static String valueOf(String field) {
        return field.substring(field.indexOf('=') + 1);
    }
}
