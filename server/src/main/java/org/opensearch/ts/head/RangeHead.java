/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.block.BlockReader;
import org.opensearch.ts.chunks.ChunkReader;

/**
 * RangeHead allows querying Head as a block within a range.
 */
public class RangeHead implements BlockReader {
    private final Head head;
    private final long minTime;
    private final long maxTime;

    public RangeHead(Head head, long minTime, long maxTime) {
        this.head = head;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public ChunkReader chunks() {
        return head.chunksRange(minTime, maxTime);
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public String toString() {
        return "RangeHead{" +
            "head=" + head +
            ", minTime=" + minTime +
            ", maxTime=" + maxTime +
            '}';
    }
}
