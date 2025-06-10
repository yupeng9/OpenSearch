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

public class XORChunk implements Chunk {
    private static final int CHUNK_COMPACT_CAPACITY_THRESHOLD = 32;
    
    private BitStream bitStream;

    public XORChunk() {
        this.bitStream = new BitStream();
        // Initialize with 2-byte header for sample count
        bitStream.writeBits(0, 16); // Initial sample count = 0
    }

    public XORChunk(byte[] bytes) {
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
    public Encoding encoding() {
        return Encoding.XOR;
    }

    @Override
    public ChunkAppender appender() {
        XORIterator it = new XORIterator(bytes());

        // To get an appender we must know the state it would have if we had
        // appended all existing data from scratch.
        // We iterate through the end and populate via the iterator's state.
        while (it.next() != ChunkIterator.ValueType.NONE) {
        }
        if (it.error() != null) {
            throw new RuntimeException("Error reading existing chunk data", it.error());
        }

        XORAppender a = new XORAppender(
            this,
            it.currentTimestamp,
            it.currentValue,
            it.timeDelta,
            it.leading,
            it.trailing
        );
        if (it.totalSamples == 0) {
            a.setLeading((byte) 0xff);
        }
        return a;
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

    @Override
    public ChunkIterator iterator(ChunkIterator iterator) {
        if (iterator instanceof XORIterator) {
            XORIterator xorIterator = (XORIterator) iterator;
            xorIterator.reset(bytes());
            return xorIterator;
        }
        return new XORIterator(bytes());
    }

    BitStream getBitStream() {
        return bitStream;
    }
}
