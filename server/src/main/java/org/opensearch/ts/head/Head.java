/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.core.index.shard.ShardId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ts.block.BlockReader;
import org.opensearch.ts.head.index.chunk.ClosedChunkIndex;
import org.opensearch.ts.head.index.live.LiveSeriesIndex;
import org.opensearch.ts.chunks.ChunkReader;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.model.SeriesSet;
import org.opensearch.ts.query.Querier;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Head implements BlockReader {
    private static final Logger log = LogManager.getLogger(Head.class);

    private final ShardId shardId;
    private final Path headDir;
    private final LiveSeriesIndex liveSeriesIndex;
    private final StripeSeries stripeSeries;
    private final AtomicLong seriesId = new AtomicLong(0);
    private final AtomicLong numSeries = new AtomicLong(0);
    private long minTime;
    private long maxTime;
    private ClosedChunkIndex closedChunkIndex;
    private ClosedChunkIndex newClosedChunkIndex;

    public Head(Path dir, ShardId shardId) {
        this.shardId = shardId;
        minTime = Long.MAX_VALUE;
        maxTime = Long.MIN_VALUE;
        stripeSeries = new StripeSeries();

        headDir = Paths.get(dir.toString(), "headDir");
        try {
            Files.createDirectories(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create head directory: " + headDir, e);
        }

        liveSeriesIndex = new LiveSeriesIndex(shardId);
        try {
            closedChunkIndex = new ClosedChunkIndex(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ClosedChunkIndex", e);
        }
    }

    // For testing purposes, create a head with a default shardId
    public Head(Path dir) {
        this(dir, new ShardId("dummy_metrics", "dummy_metrics_index_uuid", 0));
    }

    /**
     * @return the minimum valid timestamp appends so that the samples stay ahead of the blocks and head compaction window
     */
    public long appendableMinValidTime() {
        // TODO: support compaction
        return minTime;
    }

    public HeadAppender newAppender() {
        long minValidTimestamp = appendableMinValidTime();
        return new HeadAppender(this, minValidTimestamp, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    public StripeSeries getStripeSeries() {
        return stripeSeries;
    }

    public void initTime(long timestamp) {
        if (minTime == Long.MAX_VALUE) {
            minTime = timestamp;
        }
        if (maxTime == Long.MIN_VALUE) {
            maxTime = timestamp;
        }
    }

    /**
     * @param timestamp timestamp of the sample that triggered series creation, used to set minTime
     */
    public MemSeries createSeries(long hash, Labels labels, boolean pendingCommit, long timestamp) {
        MemSeries series = stripeSeries.getByHash(hash, labels);
        if (series != null) {
            return series;
        }
        long id = seriesId.incrementAndGet();
        return createSeries(id, hash, labels, pendingCommit, timestamp);
    }

    public ShardId getShardId() {
        return shardId;
    }

    private MemSeries createSeries(long id, long hash, Labels labels, boolean pendingCommit, long timestamp) {
        MemSeries newSeries = new MemSeries(id, labels, pendingCommit);
        stripeSeries.set(hash, newSeries);
        liveSeriesIndex.addSeries(labels, newSeries.getReference(), timestamp);
        numSeries.incrementAndGet();
        return newSeries;
    }


    public HeadChunkReader chunksRange(long minTime, long maxTime) {
        long mint = minTime;
        if (this.minTime > minTime) {
            mint = this.minTime;
        }
        return new HeadChunkReader(this, mint, maxTime);
    }

    @Override
    public ChunkReader chunks() {
        return chunksRange(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    // helper method for testing, replace in HeadQuerier when query side is further implemented
    public Map<Integer, List<HeadChunk>> matchingChunks(String queryString, long minTime, long maxTime) {
        Map<Integer, List<HeadChunk>> chunks = closedChunkIndex.getChunks(queryString, minTime, maxTime);
        List<Long> matchedSeries = liveSeriesIndex.getReferences(queryString, minTime);

        // TODO: if we have per sample deduplication, can we entirely remove this dedup logic?
        Set<ByteBuffer> seenChunks = new HashSet<>(); // to deduplicate chunks that may be closed but not part of the index
        for (Long seriesRef : matchedSeries) {
            MemSeries series = stripeSeries.getById(seriesRef);
            int hash = series.getLabels().hashCode(); // todo: store hash instead of ref to avoid recomputing during queries?
            List<HeadChunk> headChunks = chunks.get(hash);

            for (HeadChunk headChunk : headChunks) {
                seenChunks.add(ByteBuffer.wrap(headChunk.getChunkUuid()));
            }

            MemChunk chunk = series.getHeadChunk();
            while (chunk != null) {
                // If the chunk does not contain data in the query range, or the chunk is already loaded from the closedChunkIndex, continue
                if (!overlapsClosedInterval(chunk, minTime, maxTime) || seenChunks.contains(ByteBuffer.wrap(chunk.getChunkUuid()))) {
                    continue; // Since this iterates from most recent to oldest, with OOO handling disabled break can work instead
                }
                headChunks.add(chunk);
                chunk = chunk.getPrev(); // iterate through all chunks in the series
            }
        }
        return chunks;
    }

    /**
     * Prepares a new ClosedChunkIndex, but does not begin using it
     */
    public void prepareNewClosedChunkIndex() {
        if (newClosedChunkIndex != null) {
            throw new IllegalStateException("New ClosedChunkIndex is already prepared, truncate it before preparing a new one");
        }
        try {
            newClosedChunkIndex = new ClosedChunkIndex(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open new ClosedChunkIndex", e);
        }
    }

    /**
     * Stop writing chunks to the current closedChunkIndex and begin using the previously prepared one.
     */
    public void cutClosedChunkIndex() {
        if (newClosedChunkIndex == null) {
            throw new IllegalStateException("New ClosedChunkIndex is must not be null, prepare a new one before truncating");
        }
        closedChunkIndex = newClosedChunkIndex;
        newClosedChunkIndex = null;
    }

    public ClosedChunkIndex getCurrentClosedChunkIndex() {
        return closedChunkIndex;
    }

    /**
     * Get the LiveSeriesIndex for search operations.
     */
    public LiveSeriesIndex getLiveSeriesIndex() {
        return liveSeriesIndex;
    }

    /**
     * Closes all MemChunks in the head that will not have new samples added. This process consists of three steps:
     * <p> 1. Iterates through all MemSeries and collects MemChunks that can be closed, adding them to the ClosedChunkIndex
     * <p> 2. Blocks until the ClosedChunkIndex is refreshed, so that new queries will see the closed chunks
     * <p> 3. Removes closed MemChunks from the MemSeries, so that they will not be used in future queries
     * <p> Note: queries must query the LiveSeriesIndex first, then the ClosedChunkIndex, and finally dedup the results to
     * ensure they are complete and accurate.
     */
    public void closeHeadChunks() {
        List<MemSeries> allSeries = getStripeSeries().getSeries();

        Map<MemSeries, Set<MemChunk>> seriesToClosedChunks = indexCloseableChunks(allSeries);
        // todo: integrate with WAL, add metrics
        closedChunkIndex.commit();
        closedChunkIndex.refresh();
        dropClosedChunks(seriesToClosedChunks);
        dropEmptySeries();
    }

    /**
     * Iterate through series and index all MemChunks that can be closed. Returns a map of the series to a set of MemChunks that were indexed.
     */
    private Map<MemSeries, Set<MemChunk>> indexCloseableChunks(List<MemSeries> seriesList) {
        Map<MemSeries, Set<MemChunk>> seriesToClosedChunks = new HashMap<>(); // track closed chunks per series, to remove later
        for (MemSeries series : seriesList) {
            List<MemChunk> chunksToClose = series.getClosableChunks();

            for (MemChunk memChunk : chunksToClose) {
                try {
                    closedChunkIndex.addNewChunk(series.getLabels(), memChunk);
                    seriesToClosedChunks.computeIfAbsent(series, k -> new HashSet<>()).add(memChunk);
                } catch (IOException e) {
                    // todo: error handling, retry failed chunks?
                    throw new RuntimeException(e);
                }
            }
        }
        return seriesToClosedChunks;
    }

    /**
     * For each key/series in the map, removes the chunks in the corresponding set from the series. todo: move logic into MemSeries
     */
    private void dropClosedChunks(Map<MemSeries, Set<MemChunk>> seriesToClosedChunks) {
        for (Map.Entry<MemSeries, Set<MemChunk>> entry : seriesToClosedChunks.entrySet()) {
            MemSeries series = entry.getKey();
            series.dropClosedChunks(entry.getValue());
        }
    }

    private void dropEmptySeries() {
        List<Long> refs = new ArrayList<>();
        List<MemSeries> allSeries = stripeSeries.getSeries();
        for (MemSeries series : allSeries) {
            series.lock();
            try {
                if (series.getHeadChunk() != null) {
                    continue; // cannot gc series that has data
                }
                if (!series.getPendingGC()) {
                    series.setPendingGC(true); // mark for GC next iteration
                    continue;
                }
                refs.add(series.getReference());
                stripeSeries.delete(series);
            } finally {
                series.unlock();
            }
        }

        try {
            liveSeriesIndex.removeSeries(refs);
            numSeries.getAndUpdate(current -> current - refs.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the head, flushing any pending writes to disk and writing a snapshot of the head state. Assumes that writes have stopped
     * before this is called.
     */
    public void close() throws IOException {
        try {
            liveSeriesIndex.close();
            closedChunkIndex.close();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while closing indices", e);
        }
    }

    /**
     * Returns true if the chunk overlaps [minTime, maxTime]
     */
    private boolean overlapsClosedInterval(HeadChunk chunk, long minTimestamp, long maxTimestamp) {
        return chunk.getMinTimestamp() <= maxTimestamp && minTimestamp <= chunk.getMaxTimestamp();
    }

    // TODO pass other required structs for querying
    public HeadQuerier newHeadQuerier() {
        return new HeadQuerier(closedChunkIndex);
    }

    public static class HeadQuerier implements Querier {
        ClosedChunkIndex closedChunkIndex;

        public HeadQuerier(ClosedChunkIndex closedChunkIndex) {
            this.closedChunkIndex = closedChunkIndex;
        }

        @Override
        public SeriesSet select(long mint, long maxt, Matcher... matchers) {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
