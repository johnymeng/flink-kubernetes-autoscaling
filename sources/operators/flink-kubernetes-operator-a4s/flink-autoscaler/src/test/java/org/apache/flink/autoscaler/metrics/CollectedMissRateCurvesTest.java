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


package org.apache.flink.autoscaler.metrics;

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.autoscaler.utils.a4s.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Verifies the input boundary used by cache placement and persisted metric histories. */
class CollectedMissRateCurvesTest {
    @Test
    void operatorPreservesAnAbsentWireCurve() throws Exception {
        var mapper = RestMapperUtils.getStrictObjectMapper();
        assertNull(mapper.readValue("{\"scaledMrc\":null}", A4SAggregatedMetricsResponseBody.class)
                .getScaledMrc());
    }

    @Test
    void newAndRestoredCollectionsKeepOnlyObservedCurves() throws Exception {
        var source = new JobVertexID();
        var join = new JobVertexID();
        var empty = new MissRateCurve(Collections.emptyList());
        var measured = new MissRateCurve(Collections.singletonList(new MissRateCurve.Point(4096, 0.5)));
        var values = Map.of(source, empty, join, measured);
        var expected = Map.of(join, measured);

        assertEquals(expected, new CollectedMetrics(Map.of(), Map.of(), values).getMissRateCurves());

        var mapper = new ObjectMapper(new YAMLFactory()).registerModule(new AutoScalerSerDeModule());
        var restored =
                mapper.readValue(
                        mapper.writeValueAsString(Map.of("missRateCurves", values)),
                        CollectedMetrics.class);
        assertEquals(Set.of(join), restored.getMissRateCurves().keySet());
        var points = restored.getMissRateCurves().get(join).getPoints();
        assertEquals(1, points.size());
        assertEquals(4096L, points.get(0).getCacheSizeBytes());
        assertEquals(0.5, points.get(0).getMissRate());
    }
}
