/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.test.OpenSearchTestCase;

public class XORChunkTests extends OpenSearchTestCase {

    public void testXorRead() throws Exception {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        
        for (long i = 0; i < 120_000; i += 1000) {
            double value = i + (double)i/10 + (double)i/100 + (double)i/1000;
            appender.append(i, value);
        }

        ChunkIterator iterator = chunk.iterator(null);
        int count = 0;
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            long expectedTimestamp = count * 1000L;
            double expectedValue = expectedTimestamp + (double)expectedTimestamp/10 + (double)expectedTimestamp/100 + (double)expectedTimestamp/1000;
            
            assertEquals("Timestamp mismatch at index " + count, expectedTimestamp, tv.timestamp());
            assertEquals("Value mismatch at index " + count, expectedValue, tv.value(), 1e-10);
            
            count++;
        }
        
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testAppenderStateRestoration() throws Exception {
        // Test that appender() correctly restores state from existing chunk data
        XORChunk chunk = new XORChunk();
        ChunkAppender appender1 = chunk.appender();
        
        // Add initial samples
        appender1.append(1000, 10.0);
        appender1.append(2000, 20.0);
        appender1.append(3000, 30.0);
        
        // Get a new appender - should restore state from existing data
        ChunkAppender appender2 = chunk.appender();
        
        // Add more samples with the new appender
        appender2.append(4000, 40.0);
        appender2.append(5000, 50.0);
        
        // Verify all samples are readable
        ChunkIterator iterator = chunk.iterator(null);
        double[] expectedValues = {10.0, 20.0, 30.0, 40.0, 50.0};
        long[] expectedTimestamps = {1000, 2000, 3000, 4000, 5000};
        
        int count = 0;
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            
            assertTrue("Too many samples read: " + count, count < expectedValues.length);
            
            assertEquals("Sample " + count + " timestamp mismatch", 
                expectedTimestamps[count], tv.timestamp());
            
            assertEquals("Sample " + count + " value mismatch", 
                expectedValues[count], tv.value(), 0.001);
            
            count++;
        }
        
        assertNull("Iterator should not have errors", iterator.error());
        assertEquals("Expected 5 samples", 5, count);
    }
} 