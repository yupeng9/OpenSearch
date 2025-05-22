/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkReader;
import org.opensearch.ts.chunks.Meta;

public class HeadChunkReader implements ChunkReader {
    private Head head;
    private long minTime;
    private long maxTime;

    public HeadChunkReader(Head head, long minTime, long maxTime) {
        this.head = head;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public Chunk readChunk(Meta meta) {
        // TODO: unpack chunk id
        long seriesId = meta.getChunkRef();
        int chunkId = 0;
        MemSeries series = head.getStripeSeries().getById(seriesId);
        if (series == null) {
            return null;
        }

        return head.chunkFromSeries(series, chunkId, minTime, maxTime);
    }

    @Override
    public void close() {

    }
}
