/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.RawChunk;
import org.opensearch.ts.model.Labels;

/**
 * In-memory representation of a series.
 */
public class MemSeries {
    private long reference;
    // labels of the series
    private Labels labels;

    // A linked list of chunks in memory being built or to be mmapped. This points to the most recent chunk.
    private MemChunk headChunk;

    // timestamp at which to cut the next chunk
    private long nextAt;

    // last seen value, used for checking duplicates
    private double lastValue;

    private ChunkAppender chunkAppender;

    private boolean pendingCommit;

    public MemSeries(long reference, Labels labels, boolean pendingCommit) {
        this.reference = reference;
        this.labels = labels;
        this.pendingCommit = pendingCommit;
    }

    public Labels getLabels() {
        return labels;
    }

    public long getReference() {
        return reference;
    }

    public void commit(){
        pendingCommit = false;
    }

    private boolean appendPreprocessor(long timestamp, Encoding encoding, ChunkOptions options) {
        boolean created =false;
        if(headChunk==null){
            headChunk = createHeadChunk(timestamp, encoding, options.chunkRange());
            created = true;
        }

        MemChunk chunk = headChunk;

        int numSample = chunk.getChunk().numSamples();
        if(numSample==0){
            // a new chunk
            chunk.setMinTime(timestamp);
            this.nextAt = rangeForTimestamp(timestamp, options.chunkRange());
        }
        return created;
    }

    private MemChunk createHeadChunk(long minTime,Encoding encoding, long chunkRange){
        MemChunk chunk = new MemChunk(minTime, Long.MIN_VALUE, headChunk);
        this.headChunk = chunk;
        // TODO: support other encoding
        assert encoding == Encoding.RAW;
        chunk.setChunk(new RawChunk());
        this.nextAt = rangeForTimestamp(minTime, chunkRange);
        this.chunkAppender = chunk.getChunk().appender();
        return chunk;
    }

    public boolean append(long timestamp, double value, ChunkOptions options) {
        boolean created = appendPreprocessor(timestamp, Encoding.RAW, options);
        chunkAppender.append(timestamp, value);
        headChunk.setMaxTime(timestamp);
        this.lastValue = value;
        return created;
    }

    private long rangeForTimestamp(long t, long chunkRange) {
        return (t/chunkRange)*chunkRange+chunkRange;
    }

    /**
     * returns the chunk for the given id from memory. if the chunk is on disk, then it needs to mmap it.
     */
    public MemChunk getChunk(int chunkId) {
        // TODO: support the mmapped chunk
        // TODO: support the offset mgmt for head
        return headChunk.atOffset(chunkId);
    }
}
