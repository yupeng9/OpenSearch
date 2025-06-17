/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitReader;
import org.opensearch.ts.utils.TimestampEncoder;

import java.nio.ByteBuffer;

/**
 * Base class for compression-based iterators (XOR, CHIMP, etc.)
 * Handles common timestamp decoding logic and state management.
 * Subclasses only need to implement value-specific decompression algorithms.
 */
public abstract class CompressionIterator implements ChunkIterator {
    
    protected BitReader bitReader;
    protected int totalSamples;
    protected int samplesRead;
    
    // Current state
    protected long currentTimestamp;
    protected double currentValue;
    protected long timeDelta;
    
    protected Exception error;
    
    // State flags
    protected boolean isFirst = true;
    protected boolean isSecond = false;

    public CompressionIterator() {
        // Default constructor
    }

    public CompressionIterator(byte[] data) {
        reset(data);
    }

    public void reset(byte[] data) {
        this.samplesRead = 0;
        this.error = null;
        this.isFirst = true;
        this.isSecond = false;
        this.currentTimestamp = 0;
        this.currentValue = 0.0;
        this.timeDelta = 0;
        
        // Initialize algorithm-specific state
        resetAlgorithmState();
        
        try {
            if (data.length >= 2) {
                this.totalSamples = ByteBuffer.wrap(data, 0, 2).getShort() & 0xFFFF;
                // Skip the first 2 bytes (sample count) and create bit reader from remaining data
                this.bitReader = new BitReader(java.util.Arrays.copyOfRange(data, 2, data.length));
            } else {
                this.totalSamples = 0;
                this.bitReader = new BitReader(new byte[0]);
            }
        } catch (Exception e) {
            this.error = e;
            this.totalSamples = 0;
        }
    }

    @Override
    public final ChunkIterator.ValueType next() {
        if (error != null || samplesRead >= totalSamples) {
            return ChunkIterator.ValueType.NONE;
        }
        
        try {
            if (isFirst) {
                // First value: read timestamp and value directly
                currentTimestamp = bitReader.readVarint();
                currentValue = Double.longBitsToDouble(bitReader.readBits(64));
                
                onFirstValueRead(currentValue);
                isFirst = false;
                isSecond = true;
                samplesRead++;
                return ChunkIterator.ValueType.FLOAT;
            }
            
            if (isSecond) {
                // Second value: read timestamp delta and value
                timeDelta = bitReader.readUvarint();
                currentTimestamp += timeDelta;
                readValue();
                isSecond = false;
            } else {
                // Subsequent values: read delta-of-delta for timestamp
                long deltaOfDelta = TimestampEncoder.readTimestampDelta(bitReader);
                timeDelta = timeDelta + deltaOfDelta;
                currentTimestamp += timeDelta;
                readValue();
            }
            
            samplesRead++;
            return ChunkIterator.ValueType.FLOAT;
            
        } catch (Exception e) {
            this.error = e;
            return ChunkIterator.ValueType.NONE;
        }
    }

    @Override
    public final TimestampValue at() {
        return new TimestampValue(currentTimestamp, currentValue);
    }

    @Override
    public final Exception error() {
        return error;
    }
    
    // Abstract methods that subclasses must implement for algorithm-specific logic
    
    /**
     * Reset algorithm-specific state when iterator is reset
     */
    protected abstract void resetAlgorithmState();
    
    /**
     * Called when the first value is read to initialize algorithm-specific state
     */
    protected abstract void onFirstValueRead(double value);
    
    /**
     * Read a value with decompression
     */
    protected abstract void readValue();
} 