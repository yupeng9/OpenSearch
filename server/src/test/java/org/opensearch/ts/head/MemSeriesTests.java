/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.model.Labels;

public class MemSeriesTests extends OpenSearchTestCase {

    public void testAppendPreprocessorWithSmallSamplesPerChunkNotAllowed() {
        // Create a MemSeries with a small samplesPerChunk value (less than 4)
        Labels labels = Labels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, false);

        // Create ChunkOptions with samplesPerChunk = 3 (less than 4)
        ChunkOptions options = new ChunkOptions(1000, 3);

        long timestamp1 = 1000L;
        double value1 = 10.0;
        assertThrows(AssertionError.class, () -> series.append(timestamp1, value1, options));
    }

    /**
     * Test that verifies the normal case when ChunkOptions.samplesPerChunk >= 4,
     * where the nextAt computation should happen at 25% of samplesPerChunk.
     */
    public void testAppendPreprocessorWithNormalSamplesPerChunk() {
        // Create a MemSeries
        Labels labels = Labels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, false);

        // Create ChunkOptions with samplesPerChunk = 8 (greater than 4)
        ChunkOptions options = new ChunkOptions(1000, 8);

        // Append samples up to 25% of samplesPerChunk (2 samples)
        long timestamp1 = 1000L;
        double value1 = 10.0;
        boolean created1 = series.append(timestamp1, value1, options);

        long timestamp2 = 2000L;
        double value2 = 20.0;
        boolean created2 = series.append(timestamp2, value2, options);

        // Verify that the first two appends didn't create a new chunk
        assertTrue(created1);
        assertFalse(created2);
        assertEquals(timestamp1, series.getHeadChunk().getMinTimestamp());
        assertEquals(timestamp2, series.getHeadChunk().getMaxTimestamp());
        assertEquals(2, series.getHeadChunk().getChunk().numSamples());

        // Append more samples to reach samplesPerChunk
        for (int i = 3; i <= 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            boolean created = series.append(timestamp, value, options);

            // The 8th sample should create a new chunk because we reached samplesPerChunk
            if (i == 8) {
                assertTrue(created);
                assertEquals(8000L, series.getHeadChunk().getMinTimestamp());
                assertEquals(8000L, series.getHeadChunk().getMaxTimestamp());
                assertEquals(1, series.getHeadChunk().getChunk().numSamples());
            } else {
                assertFalse(created);
            }
        }
    }
}
