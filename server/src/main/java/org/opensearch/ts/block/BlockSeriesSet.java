/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.block;

import org.opensearch.ts.chunks.ChunkReader;
import org.opensearch.ts.model.Series;
import org.opensearch.ts.model.SeriesSet;

/**
 * BlockSeriesSet allows to iterate over all series in the single block.
 * Iterated series are trimmed with given min and max time
 */
public class BlockSeriesSet implements SeriesSet {
    ChunkReader chunks;
    long minTime;
    long maxTime;

    public BlockSeriesSet(ChunkReader chunks, long minTime, long maxTime) {
        this.chunks = chunks;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public Series next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }
}
