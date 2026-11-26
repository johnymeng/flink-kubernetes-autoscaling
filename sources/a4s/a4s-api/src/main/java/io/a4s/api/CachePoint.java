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

/** One measured point on an operator-wide miss-rate curve. */
public final class CachePoint {
    public final long capacityBytes;
    public final double missRate;

    @ConstructorProperties({"capacityBytes", "missRate"})
    public CachePoint(long capacityBytes, double missRate) {
        Checks.require(capacityBytes >= 0, "Negative cache capacity");
        Checks.require(
                Double.isFinite(missRate) && missRate >= 0 && missRate <= 1,
                "Miss rate must be in [0, 1]");
        this.capacityBytes = capacityBytes;
        this.missRate = missRate;
    }
}
