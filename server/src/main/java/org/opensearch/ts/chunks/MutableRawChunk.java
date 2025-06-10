/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitStream;

/**
 * A raw chunk without any encoding optimization for testing purpose.
 */
public class MutableRawChunk implements RawChunk {
    private BitStream bitStream;

    /**
     * Creates an empty RawChunk. Used for creating new chunks in memory.
     */
    public MutableRawChunk() {
        this.bitStream = new BitStream();
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
    public ChunkAppender appender() {
        return new RawChunkAppender();
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ChunkIterator iterator(ChunkIterator iterator) {
        throw new UnsupportedOperationException("Raw chunk iterator not implemented yet");
    }

    public class RawChunkAppender implements ChunkAppender {
        @Override
        public void append(long timestamp, double value) {
            bitStream.writeLong(timestamp);
            bitStream.writeDouble(value);
        }
    }
}
