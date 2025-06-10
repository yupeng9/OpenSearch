/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * Iterator interface for reading data from chunks
 */
public interface ChunkIterator {
    /**
     * Represents the type of value returned by a chunk iterator
     */
    enum ValueType {
        NONE,
        FLOAT
    }
    
    /**
     * Advances the iterator to the next value and returns the type of value found
     * @return ValueType.FLOAT if a value is available, ValueType.NONE if no more values
     */
    ChunkIterator.ValueType next();
    
    /**
     * Returns the current timestamp and value
     * @return TimestampValue containing both timestamp and value
     */
    TimestampValue at();
    
    /**
     * Returns any error that occurred during iteration
     * @return error or null if no error
     */
    Exception error();
    
    /**
     * Record to hold timestamp and value pair
     */
    record TimestampValue(long timestamp, double value) {}
} 