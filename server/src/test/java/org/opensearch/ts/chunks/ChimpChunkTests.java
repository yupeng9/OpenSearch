/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.test.OpenSearchTestCase;

public class ChimpChunkTests extends OpenSearchTestCase {

    public void testChimpCompressionAndDecompression() {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        // Add some sample data
        long baseTime = 1000000000L; // Base timestamp
        double[] values = {1.0, 1.1, 1.2, 1.15, 1.25, 1.3, 1.35, 1.4, 1.45, 1.5};
        
        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 1000, values[i]);
        }
        
        // Verify we can read the data back correctly
        ChunkIterator iterator = chunk.iterator(null);
        int count = 0;
        
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 1000, tv.timestamp());
            assertEquals("Value mismatch at index " + count, values[count], tv.value(), 0.000001);
            count++;
        }
        
        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertEquals("Chunk sample count mismatch", values.length, chunk.numSamples());
        assertEquals("Encoding should be CHIMP", Encoding.CHIMP, chunk.encoding());
        
        // Verify no errors occurred
        assertNull("Iterator should not have errors", iterator.error());
    }
    
    public void testChimpWithRepeatedValues() {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        // Add data with repeated values to test compression efficiency
        long baseTime = 2000000000L;
        double[] values = {5.0, 5.0, 5.1, 5.1, 5.1, 5.2, 5.2, 5.0, 5.0, 5.3};
        
        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 2000, values[i]);
        }
        
        // Verify decompression
        ChunkIterator iterator = chunk.iterator(null);
        int count = 0;
        
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 2000, tv.timestamp());
            assertEquals("Value mismatch at index " + count, values[count], tv.value(), 0.000001);
            count++;
        }
        
        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }
    
    public void testChimpEmptyChunk() {
        ChimpChunk chunk = new ChimpChunk();
        
        assertEquals("Empty chunk should have 0 samples", 0, chunk.numSamples());
        assertEquals("Encoding should be CHIMP", Encoding.CHIMP, chunk.encoding());
        
        ChunkIterator iterator = chunk.iterator(null);
        assertEquals("Empty chunk iterator should return NONE", 
                    ChunkIterator.ValueType.NONE, iterator.next());
        assertNull("Iterator should not have errors", iterator.error());
    }
    
    public void testChimpSingleValue() {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        appender.append(1000L, 42.0);
        
        assertEquals("Single value chunk should have 1 sample", 1, chunk.numSamples());
        
        ChunkIterator iterator = chunk.iterator(null);
        assertEquals("Should have one value", ChunkIterator.ValueType.FLOAT, iterator.next());
        
        ChunkIterator.TimestampValue tv = iterator.at();
        assertEquals("Timestamp should match", 1000L, tv.timestamp());
        assertEquals("Value should match", 42.0, tv.value(), 0.000001);
        
        assertEquals("Should be end of data", ChunkIterator.ValueType.NONE, iterator.next());
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testChimpRead() throws Exception {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        // Test with large scale data similar to XOR test
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
        assertEquals("Expected 120 samples", 120, count);
        assertEquals("Chunk sample count should match", 120, chunk.numSamples());
    }

    public void testChimpAppenderStateRestoration() throws Exception {
        // Test that appender() correctly restores state from existing chunk data
        ChimpChunk chunk = new ChimpChunk();
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
        assertEquals("Chunk sample count should match", 5, chunk.numSamples());
    }

    public void testChimpLargeValues() {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        // Test with large floating point values to stress the compression
        long baseTime = 1000000000L;
        double[] values = {
            1.7976931348623157e+308,  // Near Double.MAX_VALUE
            -1.7976931348623157e+308, // Near -Double.MAX_VALUE  
            4.9e-324,                 // Near Double.MIN_VALUE
            -4.9e-324,                // Near -Double.MIN_VALUE
            0.0,                      // Zero
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            1234567890.123456789,     // Large precise value
            -9876543210.987654321,    // Large negative precise value
            Math.PI,                  // Irrational number
            Math.E                    // Another irrational number
        };
        
        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 1000, values[i]);
        }
        
        // Verify all values can be read back correctly
        ChunkIterator iterator = chunk.iterator(null);
        int count = 0;
        
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 1000, tv.timestamp());
            
            // For special values, use exact equality
            if (Double.isInfinite(values[count]) || Double.isNaN(values[count])) {
                assertEquals("Special value mismatch at index " + count, values[count], tv.value(), 0.0);
            } else {
                assertEquals("Value mismatch at index " + count, values[count], tv.value(), Math.abs(values[count]) * 1e-15);
            }
            count++;
        }
        
        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testChimpTimestampJumps() {
        ChimpChunk chunk = new ChimpChunk();
        ChunkAppender appender = chunk.appender();
        
        // Test with irregular timestamp patterns to stress timestamp compression
        long[] timestamps = {
            1000L,        // First value
            2000L,        // +1000 (normal delta)
            3000L,        // +1000 (same delta) 
            10000L,       // +7000 (large jump)
            10100L,       // +100 (small delta)
            10050L,       // -50 (negative delta - should be rare but supported)
            50000L,       // +39950 (very large jump)
            50001L,       // +1 (very small delta)
            50002L,       // +1 (same small delta)
            100000L       // +49998 (another large jump)
        };
        
        double baseValue = 100.0;
        for (int i = 0; i < timestamps.length; i++) {
            appender.append(timestamps[i], baseValue + i);
        }
        
        // Verify all timestamps are preserved correctly
        ChunkIterator iterator = chunk.iterator(null);
        int count = 0;
        
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, timestamps[count], tv.timestamp());
            assertEquals("Value mismatch at index " + count, baseValue + count, tv.value(), 0.001);
            count++;
        }
        
        assertEquals("Expected " + timestamps.length + " samples but got " + count, timestamps.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }
} 