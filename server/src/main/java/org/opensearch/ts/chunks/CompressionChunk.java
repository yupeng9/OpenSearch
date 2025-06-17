/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitStream;

import java.nio.ByteBuffer;

/**
 * Base class for compression-based chunks (XOR, CHIMP, etc.)
 * Provides common functionality for BitStream-based chunk implementations.
 */
public abstract class CompressionChunk implements Chunk {
    private static final int CHUNK_COMPACT_CAPACITY_THRESHOLD = 32;
    
    protected BitStream bitStream;

    /**
     * Constructor for new chunk
     */
    public CompressionChunk() {
        this.bitStream = new BitStream();
        // Initialize with 2-byte header for sample count
        bitStream.writeBits(0, 16); // Initial sample count = 0
    }

    /**
     * Constructor for existing chunk from bytes
     */
    public CompressionChunk(byte[] bytes) {
        this.bitStream = new BitStream(bytes);
    }

    @Override
    public byte[] bytes() {
        return bitStream.toByteArray();
    }

    @Override
    public int bytesSize() {
        return bitStream.size();
    }

    @Override
    public int numSamples() {
        byte[] data = bitStream.toByteArray();
        if (data.length >= 2) {
            ByteBuffer buffer = ByteBuffer.wrap(data, 0, 2);
            return buffer.getShort() & 0xFFFF; // Convert to unsigned
        }
        return 0;
    }

    @Override
    public void compact() {
        byte[] currentBytes = bytes();
        if (currentBytes.length > bitStream.size() + CHUNK_COMPACT_CAPACITY_THRESHOLD) {
            // Create new BitStream with compacted data
            this.bitStream = new BitStream(java.util.Arrays.copyOf(currentBytes, bitStream.size()));
        }
    }

    /**
     * Provides access to the underlying BitStream
     */
    public BitStream getBitStream() {
        return bitStream;
    }

    // Abstract methods that subclasses must implement
    @Override
    public abstract Encoding encoding();

    @Override
    public abstract ChunkAppender appender();

    @Override
    public abstract ChunkIterator iterator(ChunkIterator iterator);
} 