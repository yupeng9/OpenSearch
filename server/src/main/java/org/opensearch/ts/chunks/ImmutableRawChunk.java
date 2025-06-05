/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * ImmutableRawChunk is non-appendable, read-only chunk that holds raw bytes.
 */
public class ImmutableRawChunk implements RawChunk {
    private final byte[] bytes;

    public ImmutableRawChunk(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] bytes() {
        return bytes;
    }

    @Override
    public int bytesSize() {
        return bytes.length;
    }

    @Override
    public ChunkAppender appender() {
        throw new UnsupportedOperationException("Immutable chunk does not support appending");
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
