/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitStream;
import org.opensearch.ts.utils.TimestampEncoder;

/**
 * Base class for compression-based appenders (XOR, CHIMP, etc.)
 * Handles common timestamp encoding logic and state management.
 * Subclasses only need to implement value-specific compression algorithms.
 */
public abstract class CompressionAppender implements ChunkAppender {
    
    protected CompressionChunk chunk;
    protected long lastTimestamp;
    protected double lastValue;
    protected long timeDelta;
    
    // State for managing first and second values
    protected boolean isFirst = true;
    protected boolean isSecond = false;

    public CompressionAppender() {
        // Default constructor for new appenders
    }

    public CompressionAppender(CompressionChunk chunk, long lastTimestamp, double lastValue, long timeDelta) {
        this.chunk = chunk;
        this.lastTimestamp = lastTimestamp;
        this.lastValue = lastValue;
        this.timeDelta = timeDelta;
        
        // Determine state based on existing samples in chunk
        int numSamples = chunk.numSamples();
        this.isFirst = (numSamples == 0);
        this.isSecond = (numSamples == 1);
    }

    @Override
    public final void append(long timestamp, double value) {
        if (chunk == null) {
            throw new IllegalStateException("Appender not properly initialized with chunk");
        }

        BitStream bitStream = chunk.getBitStream();
        
        if (isFirst) {
            // First value: store timestamp and value directly
            bitStream.writeVarint(timestamp);
            bitStream.writeBits(Double.doubleToRawLongBits(value), 64);
            
            // Update sample count
            updateSampleCount(1);
            
            lastTimestamp = timestamp;
            lastValue = value;
            onFirstValue(value);
            isFirst = false;
            isSecond = true;
            return;
        }
        
        if (isSecond) {
            // Second value: write timestamp delta and value
            timeDelta = timestamp - lastTimestamp;
            bitStream.writeUvarint(timeDelta);
            writeValue(value);
            isSecond = false;
        } else {
            // Subsequent values: use delta-of-delta for timestamp
            long tDelta = timestamp - lastTimestamp;
            long deltaOfDelta = tDelta - timeDelta;
            TimestampEncoder.writeTimestampDelta(bitStream, deltaOfDelta);
            writeValue(value);
            timeDelta = tDelta;
        }
        
        lastTimestamp = timestamp;
        lastValue = value;
        
        // Update sample count
        updateSampleCount(getSampleCount() + 1);
    }
    
    /**
     * Get current sample count from chunk header
     */
    protected final int getSampleCount() {
        return chunk.numSamples();
    }
    
    /**
     * Update sample count in chunk header
     */
    protected final void updateSampleCount(int count) {
        chunk.getBitStream().updateShortAt(0, (short) count);
    }
    
    // Abstract methods that subclasses must implement for algorithm-specific logic
    
    /**
     * Called when the first value is written to initialize algorithm-specific state
     */
    protected abstract void onFirstValue(double value);
    
    /**
     * Write a value with compression
     */
    protected abstract void writeValue(double value);
}