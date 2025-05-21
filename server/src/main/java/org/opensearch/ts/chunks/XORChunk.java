/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

public class XORChunk implements Chunk {
    @Override
    public byte[] bytes() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Encoding encoding() {
        return Encoding.XOR;
    }

    @Override
    public ChunkAppender appender() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int numSamples() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
