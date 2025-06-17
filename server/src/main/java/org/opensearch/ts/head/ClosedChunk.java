/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.ImmutableRawChunk;

public class ClosedChunk implements HeadChunk {

    private final long minTimestamp;
    private final long maxTimestamp;
    private final Chunk chunk;
    private final byte[] uuid;

    public ClosedChunk(long minTimestamp, long maxTimestamp, byte[] bytes, Encoding encoding, byte[] uuid) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.chunk = switch (encoding) {
            case RAW -> new ImmutableRawChunk(bytes);
            case XOR -> throw new UnsupportedOperationException("XOR encoding not yet supported");
        };
        this.uuid = uuid;
    }

    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public byte[] getChunkUuid() {
        return uuid;
    }

    public Chunk getChunk() {
        return chunk;
    }
}
