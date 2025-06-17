/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.MutableRawChunk;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    // list of mmapped chunks, that are still part of the head block.
    private final List<MMappedChunk> mmappedChunks = new ArrayList<>();

    // Atomically update headChunk list and mmappedChunks when mmapping or truncating
    private final ReentrantReadWriteLock chunkListsLock = new ReentrantReadWriteLock();

    // TODO update during truncation
    private long firstChunkId;

    // A linked list of chunks in memory being built or to be mmapped. This points to the most recent chunk.
    private MemChunk headChunk;
    private final ReentrantLock headChunkWriteLock = new ReentrantLock();

    // Max timestamp of the head chunk, used for checking ooo/duplicates
    private long maxTimestamp;

    // Max time of MMAP'd chunk, used during WAL replay
    private long mmapMaxTimestamp;

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

    public List<MMappedChunk> getMMappedChunks() {
        return mmappedChunks;
    }

    public void commit() {
        pendingCommit = false;
    }

    // TODO: locking? can multiple threads write?
    private boolean appendPreprocessor(long timestamp, Encoding encoding, ChunkOptions options) {
        boolean created = false;
        if (headChunk == null) {
            // TODO: ooo handling, do not create if ooo sample (implied, memseries contains mmap chunks only)
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

        headChunkWriteLock.lock();
        try {
            this.headChunk = chunk;
        } finally {
            headChunkWriteLock.unlock();
        }
        // TODO: support other encoding
        assert encoding == Encoding.RAW;
        chunk.setChunk(new MutableRawChunk());
        this.nextAt = rangeForTimestamp(minTime, chunkRange);
        this.chunkAppender = chunk.getChunk().appender();
        return chunk;
    }

    public boolean append(long timestamp, double value, ChunkOptions options) {
        boolean created = appendPreprocessor(timestamp, Encoding.RAW, options);
        chunkAppender.append(timestamp, value);
        headChunk.setMaxTimestamp(timestamp);
        this.lastValue = value;
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

    /**
     * returns the chunk for the given id from memory. if the chunk is on disk, then it needs to mmap it.
     */
    public HeadChunk getChunk(long chunkId) {
        chunkListsLock.readLock().lock();
        try {
            int chunkIndex = Math.toIntExact(chunkId - firstChunkId);
            int headChunksLen = headChunk == null ? 0 : headChunk.len();

            if (chunkIndex < 0 || chunkIndex > mmappedChunks.size() + headChunksLen - 1) {
                throw new IndexOutOfBoundsException("Chunk ID " + chunkId + " is out of bounds. Valid range: [" + firstChunkId + ", " + (
                    firstChunkId + mmappedChunks.size() + headChunksLen - 1) + "]");
            }

            if (chunkIndex < mmappedChunks.size()) {
                return mmappedChunks.get(chunkIndex);
            }

            int headChunksIndex = chunkIndex - mmappedChunks.size();
            int headChunksOffset = headChunksLen - 1 - headChunksIndex; // offset is from head to tail, reverse the index
            return headChunk.atOffset(headChunksOffset);
        } finally {
            chunkListsLock.readLock().unlock();
        }
    }

    public MemChunk getHeadChunk() {
        return headChunk;
    }

    public void setHeadChunk(MemChunk chunk) {
        this.headChunk = chunk;
    }

    public List<MemChunk> getClosableChunks() {
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
    }

    /**
     * MMap all but the head chunk, and possibly the head chunk if the series is inactive. Returns the number of chunks mapped
     */
    public int mmapChunks(ChunkDiskMapper chunkDiskMapper) {
        if (headChunk == null || headChunk.getPrev() == null) {
            return 0; // nothing to map
        }

        List<MMappedChunk> newMMappedChunks = new ArrayList<>();
        long newMMapMaxTimestamp = mmapMaxTimestamp;

        int count = 0;
        // mmap chunks from oldest to newest, skipping the current headChunk
        for (int i = headChunk.len() - 1; i > 0; i--) {
            MemChunk chunk = headChunk.atOffset(i);
            ChunkDiskMapper.ChunkRef chunkRef = chunkDiskMapper.writeChunk(reference, chunk);

            MMappedChunk mmappedChunk = new MMappedChunk(chunkRef, chunk.getMinTimestamp(), chunk.getMaxTimestamp());
            newMMappedChunks.add(mmappedChunk);
            if (chunk.getMaxTimestamp() > newMMapMaxTimestamp) {
                newMMapMaxTimestamp = chunk.getMaxTimestamp();
            }
            count++;
        }

        // TODO: if t is significantly larger than headChunk.nextAt, we may seal the head chunk and mmap it too (e.g. series churn)

        //
        chunkListsLock.writeLock().lock();
        try {
            // replace the old mmapped chunks with the new ones
            mmappedChunks.addAll(newMMappedChunks);
            mmapMaxTimestamp = newMMapMaxTimestamp;

            // drop memchunks that are now mmapped
            headChunk.truncatePrev();
        } finally {
            chunkListsLock.writeLock().unlock();
        }

        return count;
    }

    /**
     * Truncate mmapped chunks that contain data strictly before minTimestamp, and returns the smallest file index of the remaining mmapped chunks
     */
    public int truncateBefore(long minTimestamp) {
        int minFileIndex = 0;
        chunkListsLock.writeLock().lock();
        try {
            int i = 0;
            // mmappedChunks are not necessarily ordered (may include ooo chunks, so check all)
            while (i < mmappedChunks.size()) {
                MMappedChunk chunk = mmappedChunks.get(i);
                if (chunk.getMaxTimestamp() >= minTimestamp) {
                    minFileIndex = Math.min(minFileIndex, chunk.getFileIndex());
                    i++;
                    continue;
                }
                mmappedChunks.remove(i);
            }
        } finally {
            chunkListsLock.writeLock().unlock();
        }
        return minFileIndex;
    }

    /**
     * Encode the series as a byte[]. Takes a byte[] as input which the serialized data is written to. Returns the number of bytes written.
     */
    public int encodeForSnapshot(byte[] buffer) throws IOException {
        int pos = labels.bytes(buffer);
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer, pos, Long.BYTES);
        out.writeLong(reference);
        return out.getPosition();
    }

    /**
     * Create a MemSeries from the given byte arrays representing labels and metadata.
     */
    public static MemSeries decodeFromSnapshot(byte[] labelsBytes, byte[] metaBytes) {
        assert metaBytes.length == SERIALIZED_META_SIZE : "Meta bytes must be of size " + SERIALIZED_META_SIZE;
        Labels labels = Labels.fromSerializedBytes(labelsBytes);
        ByteArrayDataInput meta = new ByteArrayDataInput(metaBytes);
        long reference = meta.readLong();
        return new MemSeries(reference, labels, false);
    }

    public void lockHeadChunk() {
        headChunkWriteLock.lock();
    }

    public void unlockHeadChunk() {
        headChunkWriteLock.unlock();
    }
}
