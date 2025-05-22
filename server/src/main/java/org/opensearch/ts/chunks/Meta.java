/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

public class Meta {
    long chunkRef;
    Chunk chunk;

    // Time range the data covers.
    long minTime;
    // When maxTime == Long.MAX_VALUE the chunk is still open and being appended to.
    long maxTime;

    public Meta(long chunkRef, Chunk chunk, long minTime, long maxTime) {
        this.chunkRef = chunkRef;
        this.chunk = chunk;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    public long getChunkRef() {
        return chunkRef;
    }
}
