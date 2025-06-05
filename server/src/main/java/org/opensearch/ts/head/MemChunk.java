/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;

/*
 * MemChunk represents a chunk in the head block. Chunk may be in memory, or mmapped.
 */
public class MemChunk implements HeadChunk {
    private Chunk chunk;
    private long minTimestamp;
    private long maxTimestamp;
    // link to the previous chunk on the linked list
    private MemChunk prev;

    public MemChunk(long minTimestamp, long maxTimestamp, MemChunk prev) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.prev = prev;
    }

    // Returns the length of the memChunk list, including this element
    public int len() {
        int count = 0;
        MemChunk elem = this;
        while (elem != null) {
            count++;
            elem = elem.prev;
        }
        return count;
    }

    // Returns the oldest element in the list (tail of the linked list)
    public MemChunk oldest() {
        MemChunk elem = this;
        while (elem.prev != null) {
            elem = elem.prev;
        }
        return elem;
    }

    // Returns the memChunk that is `offset` elements before this one
    public MemChunk atOffset(int offset) {
        if (offset < 0) return null;
        MemChunk elem = this;
        for (int i = 0; i < offset; i++) {
            if (elem == null) return null;
            elem = elem.prev;
        }
        return elem;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setChunk(Chunk chunk) {
        this.chunk = chunk;
    }

    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long timestamp) {
        this.minTimestamp = timestamp;
    }

    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long timestamp) {
        this.maxTimestamp = timestamp;
    }

    public MemChunk getPrev() {
        return prev;
    }

    public void truncatePrev() {
        this.prev = null;
    }
}
