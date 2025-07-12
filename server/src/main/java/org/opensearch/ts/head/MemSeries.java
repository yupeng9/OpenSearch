/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.XORChunk;
import org.opensearch.ts.model.Labels;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory representation of a series.
 */
public class MemSeries {

    // TODO arbitrary limit for now (see https://uber.slack.com/archives/C08TEM5GWFP/p1749084948078729)
    public static final int MAX_SERIALIZED_SIZE = 16 * 1024; // 16KB
    public static final int SERIALIZED_META_SIZE = Long.BYTES; // non labels bytes, used for serialization

    private final long reference;

    // labels of the series (TODO do we need to keep this in memory?)
    private final Labels labels;

    // Lock should be held when using any vars defined below this point
    private final ReentrantLock seriesLock = new ReentrantLock();

    // A linked list of chunks in memory being built or to be mmapped. This points to the most recent chunk.
    private MemChunk headChunk;

    // Max timestamp of the head chunk, used for checking ooo/duplicates
    private long maxTimestamp;

    // Max timestamp of a mmapped head chunk, used to skip samples during translog replay
    private long maxMmapTimestamp;

    // timestamp at which to cut the next chunk
    private long nextAt;

    // last seen value, used for checking duplicates
    private double lastValue;

    private ChunkAppender chunkAppender;

    private boolean pendingCommit;

    // Previously empty in a GC cycle, may be removed in the next GC cycle
    private boolean pendingGC;

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

    public void commit() {
        pendingCommit = false;
    }

    private boolean appendPreprocessor(long timestamp, Encoding encoding, ChunkOptions options) {
        boolean created = false;
        if (headChunk == null) {
            // TODO: ooo handling, do not create if ooo sample
            pendingGC = false; // ensure the series will not be concurrently removed if this is the first sample in a long time
            headChunk = createHeadChunk(timestamp, encoding, options.chunkRange());
            created = true;
        }

        MemChunk chunk = headChunk;

        int numSamples = chunk.getChunk().numSamples();
        if (numSamples == 0) {
            // a new chunk
            chunk.setMinTimestamp(timestamp);
            this.nextAt = rangeForTimestamp(timestamp, options.chunkRange());
        }

        // If we reach 25% of chunk's target sample count, try to tighten nextAt. Can help with cleaning stale series (churn)
        if (numSamples == options.samplesPerChunk() / 4) {
            assert options.samplesPerChunk() >= 4 : "ChunkOptions.samplesPerChunk must be >= 4";
            this.nextAt = computeChunkEndTime(chunk.getMinTimestamp(), chunk.getMaxTimestamp(), this.nextAt, 4);
        }

        // Cut if the timestamp is larger than the nextAt or if we have too many samples
        if (timestamp >= this.nextAt || numSamples >= options.samplesPerChunk() * 2) {
            createHeadChunk(timestamp, encoding, options.chunkRange());
            created = true;
        }
        return created;
    }

    /**
     * Estimate end timestamp based on beginning timestamp, current timestamp, and upper bound end timestamp. Assumes this
     * is called when chunk is 1 / ratio full.
     * @return the estimated end timestamp for the chunk, strictly less than or equal to nextAt
     */
    private long computeChunkEndTime(long minTimestamp, long maxTimestamp, long nextAt, int ratio) {
        double n = (double) (nextAt - minTimestamp) / ((double) (maxTimestamp - minTimestamp + 1) * ratio);
        if (n <= 1) {
            return maxTimestamp;
        }
        return (long) (minTimestamp + (nextAt - minTimestamp) / Math.floor(n));
    }

    private MemChunk createHeadChunk(long minTime, Encoding encoding, long chunkRange) {
        MemChunk chunk = new MemChunk(minTime, Long.MIN_VALUE, headChunk);

        seriesLock.lock();
        try {
            this.headChunk = chunk;
        } finally {
            seriesLock.unlock();
        }
        // TODO: support other encoding
        assert encoding == Encoding.XOR;
        chunk.setChunk(new XORChunk());
        this.nextAt = rangeForTimestamp(minTime, chunkRange);
        this.chunkAppender = chunk.getChunk().appender();
        return chunk;
    }

    /**
     * Append an in order sample to the series. The series lock should be held when calling this method
     */
    public boolean append(long timestamp, double value, ChunkOptions options) {
        boolean created = appendPreprocessor(timestamp, Encoding.XOR, options);
        chunkAppender.append(timestamp, value);
        headChunk.setMaxTimestamp(timestamp);
        this.lastValue = value;
        this.pendingGC = false;
        return created;
    }

    public boolean isOOO(long t) {
        return headChunk != null && t < headChunk.getMaxTimestamp(); // TODO this is leaky, allows block overlap etc
    }

    /**
     * Calculates the end timestamp for the given timestamp based on the chunk range.
     */
    private long rangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange + chunkRange;
    }

    public MemChunk getHeadChunk() {
        return headChunk;
    }

    public boolean getPendingGC() {
        return pendingGC;
    }

    public void setPendingGC(boolean pendingGC) {
        this.pendingGC = pendingGC;
    }

    public long getMaxMmapTimestamp() {
        return maxMmapTimestamp;
    }

    public void setMaxMmapTimestamp(long maxMmapTimestamp) {
        this.maxMmapTimestamp = maxMmapTimestamp;
    }

    public List<MemChunk> getClosableChunks() {
        lock();
        try {
            List<MemChunk> closableChunks = new ArrayList<>();
            MemChunk headChunk = this.headChunk;
            if (headChunk == null || headChunk.getPrev() == null) {
                return closableChunks; // nothing to map
            }

            for (int i = headChunk.len() - 1; i > 0; i--) {
                closableChunks.add(headChunk.atOffset(i));
                // todo: if curr time is significantly larger than headChunk.nextAt, we may seal the head chunk and mmap it too (e.g. series churn)
            }

            return closableChunks;
        } finally {
            unlock();
        }
    }

    public void dropClosedChunks(Set<MemChunk> closedChunks) {
        lock();
        try {
            MemChunk curr = headChunk;
            while (curr != null) {
                if (closedChunks.contains(curr)) {
                    if (curr.getPrev() != null) {
                        curr.getPrev().setNext(curr.getNext());
                    }

                    if (curr.getNext() != null) {
                        curr.getNext().setPrev(curr.getPrev());
                    }

                    if (curr == headChunk) {
                        headChunk = curr.getPrev();
                    }
                }
                curr = curr.getPrev();
            }
        } finally {
            unlock();
        }
    }

    public void lock() {
        seriesLock.lock();
    }

    public void unlock() {
        seriesLock.unlock();
    }
}
