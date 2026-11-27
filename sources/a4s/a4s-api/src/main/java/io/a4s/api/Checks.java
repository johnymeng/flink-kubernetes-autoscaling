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

final class Checks {
    private Checks() {}

    static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }

    static void identity(String value, String name) {
        require(value != null && !value.isBlank() && value.length() <= 256, "Invalid " + name);
    }

    static void nonnegative(Long value, String name) {
        require(value == null || value >= 0, "Invalid " + name);
    }

    static void nonnegative(Double value, String name) {
        require(value == null || (Double.isFinite(value) && value >= 0), "Invalid " + name);
    }

    static void fraction(Double value, String name) {
        nonnegative(value, name);
        require(value == null || value <= 1, "Invalid " + name);
    }
}
