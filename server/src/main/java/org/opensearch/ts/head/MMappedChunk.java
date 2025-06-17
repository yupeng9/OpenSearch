/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;

/**
 * MMapChunk represents a memory-mapped chunk in the head block.
 */
public class MMappedChunk implements HeadChunk {
    private final ChunkDiskMapper.ChunkRef chunkRef;
    private final long minTimestamp;
    private final long maxTimestamp;

    public MMappedChunk(ChunkDiskMapper.ChunkRef chunkRef, long minTimestamp, long maxTimestamp) {
        this.chunkRef = chunkRef;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public byte[] getChunkUuid() {
        throw new UnsupportedOperationException("class will be removed");
    }

    public int getFileIndex() {
        return chunkRef.fileIndex();
    }

    public Chunk getChunk(ChunkDiskMapper chunkDiskMapper) {
        return chunkDiskMapper.chunkFor(chunkRef);
    }

    @Override
    public Chunk getChunk() {
        // MMappedChunk will be removed in a future cleanup commit
        throw new UnsupportedOperationException("Use getChunk(ChunkDiskMapper) instead to retrieve the chunk from disk.");
    }
}
