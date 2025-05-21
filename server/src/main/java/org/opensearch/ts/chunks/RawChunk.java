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
public class RawChunk implements Chunk {
    // a sample is 8-byte timestamp + 8-byte value
    public static final int SAMPLE_SIZE = 16;

    private BitStream bitStream;

    public RawChunk() {
        this.bitStream = new BitStream();
    }

    @Override
    public byte[] bytes() {
        return bitStream.toByteArray();
    }

    @Override
    public Encoding encoding() {
        return Encoding.RAW;
    }

    @Override
    public ChunkAppender appender() {
        return new RawChunkAppender();
    }

    @Override
    public int numSamples() {
        // TODO: consider header size
        return bitStream.size() / SAMPLE_SIZE;
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public class RawChunkAppender implements ChunkAppender {
        @Override
        public void append(long timestamp, double value) {
            bitStream.writeLong(timestamp);
            bitStream.writeDouble(value);
        }
    }
}
