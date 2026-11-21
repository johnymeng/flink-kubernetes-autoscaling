package org.apache.flink.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MissRateCurveTest {
    @Test
    void testScaledMRC_horizontalScalingLogic() {
        int partitions = 2;
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {5L, 3L, 2L}, 0L, partitions, 1L, 4096L);

        MissRateCurve scaled = MissRateCurve.fromStackDistanceHistogram(histogram);

        List<MissRateCurve.Point> expectedScaled =
                Arrays.asList(
                        new MissRateCurve.Point(8192L, 0.5),
                        new MissRateCurve.Point(16384L, 0.2),
                        new MissRateCurve.Point(24576L, 0.0));

        for (int i = 0; i < expectedScaled.size(); i++) {
            assertEquals(
                    expectedScaled.get(i).getCacheSizeBytes(),
                    scaled.getPoints().get(i).getCacheSizeBytes());
            assertEquals(
                    expectedScaled.get(i).getMissRate(),
                    scaled.getPoints().get(i).getMissRate(),
                    0.0001);
        }
    }

    @Test
    void rejectsCapacityCoordinateOverflow() {
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {1}, 0, 2, Long.MAX_VALUE, 4096);
        assertThrows(ArithmeticException.class, () -> MissRateCurve.fromStackDistanceHistogram(histogram));
    }

    @Test
    void supportedInverseKeepsInclusivePlateausAndExactZero() {
        List<MissRateCurve.Point> points = Arrays.asList(
                new MissRateCurve.Point(4096, 0.3),
                new MissRateCurve.Point(8192, 0.3),
                new MissRateCurve.Point(12288, 0.0));
        MissRateCurve curve = new MissRateCurve(points, MissRateCurve.CapacityAxis.ATTESTED_NATIVE_BYTES);
        assertEquals(4096L, curve.supportedMemoryBytesForMissRate(0.3).get().longValue());
        assertEquals(12288L, curve.supportedMemoryBytesForMissRate(0).get().longValue());
        assertEquals(java.util.Optional.empty(), curve.supportedMemoryBytesForMissRate(1));
        assertEquals(java.util.Optional.empty(),
                new MissRateCurve(points).supportedMemoryBytesForMissRate(0.3));
        MissRateCurve malformed = new MissRateCurve(Arrays.asList(
                points.get(0), points.get(2), points.get(1)),
                MissRateCurve.CapacityAxis.ATTESTED_NATIVE_BYTES);
        assertEquals(java.util.Optional.empty(), malformed.supportedMemoryBytesForMissRate(0.3));
    }

    @Test
    void testLeastMemoryBytesForMissRate_simpleCase() {
        MissRateCurve mrc =
                new MissRateCurve(
                        Arrays.asList(
                                new MissRateCurve.Point(100L, 0.5),
                                new MissRateCurve.Point(200L, 0.2),
                                new MissRateCurve.Point(300L, 0.1)));

        assertEquals(100L, mrc.leastMemoryBytesForMissRate(0.5));
        assertEquals(200L, mrc.leastMemoryBytesForMissRate(0.2));
        assertEquals(300L, mrc.leastMemoryBytesForMissRate(0.1));
    }

    @Test
    void testLeastMemoryBytesForMissRate_plateauCase() {
        MissRateCurve mrc =
                new MissRateCurve(
                        Arrays.asList(
                                new MissRateCurve.Point(100L, 0.6),
                                new MissRateCurve.Point(200L, 0.5),
                                new MissRateCurve.Point(300L, 0.4),
                                new MissRateCurve.Point(400L, 0.38),
                                new MissRateCurve.Point(500L, 0.37)));

        assertEquals(500L, mrc.leastMemoryBytesForMissRate(0.3));
    }

    @Test
    void testLeastMemoryBytesForMissRate_noPointFoundOnePoint() {
        MissRateCurve mrc = new MissRateCurve(Arrays.asList(new MissRateCurve.Point(100L, 0.5)));

        assertEquals(100L, mrc.leastMemoryBytesForMissRate(0.3));
    }

    @Test
    void testLeastMemoryBytesForMissRate_noPointFoundTwoPoints() {
        MissRateCurve mrc =
                new MissRateCurve(
                        Arrays.asList(new MissRateCurve.Point(100L, 0.5), new MissRateCurve.Point(200L, 0.49)));

        assertEquals(200L, mrc.leastMemoryBytesForMissRate(0.3));
    }
}
