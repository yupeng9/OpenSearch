/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.head.index.chunk.ClosedChunkIndex;
import org.opensearch.ts.head.index.live.LiveSeriesIndex;
import org.opensearch.ts.model.Labels;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.ts.head.MemSeries.SERIALIZED_META_SIZE;

public class Head {
    private static final Logger log = LogManager.getLogger(Head.class);
    private static final String SNAPSHOT_FILE_PREFIX = "snapshot_";
    private final Path headDir;
    private final Path snapshotsDir;
    private final ChunkDiskMapper chunkDiskMapper; // todo remove this
    private final LiveSeriesIndex liveSeriesIndex;
    private final ClosedChunkIndex closedChunkIndex;
    private final StripeSeries stripeSeries;
    private final AtomicLong seriesId = new AtomicLong(0);
    private final AtomicLong numSeries = new AtomicLong(0);
    private long minTime;
    private long maxTime;

    public Head(Path dir) {
        minTime = Long.MAX_VALUE;
        maxTime = Long.MIN_VALUE;
        stripeSeries = new StripeSeries();

        headDir = Paths.get(dir.toString(), "headDir");
        snapshotsDir = Paths.get(headDir.toString(), "snapshots");
        try {
            Files.createDirectories(snapshotsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create head directory: " + headDir, e);
        }

        chunkDiskMapper = new ChunkDiskMapper(headDir); // todo remove

        liveSeriesIndex = new LiveSeriesIndex();
        try {
            closedChunkIndex = new ClosedChunkIndex(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ClosedChunkIndex", e);
        }
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

    private MemSeries createSeries(long id, long hash, Labels labels, boolean pendingCommit, long timestamp) {
        MemSeries newSeries = new MemSeries(id, labels, pendingCommit);
        stripeSeries.set(hash, newSeries);
        liveSeriesIndex.addSeries(labels, newSeries.getReference(), timestamp);
        numSeries.incrementAndGet();
        return newSeries;
    }

    /**
     * Creates a series with the given hash and MemSeries object, used when loading snapshot. When this is used,
     * numSeries and seriesId should be updated independently
     */
    private void createSeries(MemSeries memSeries) {
        stripeSeries.set(memSeries.getLabels().hashCode(), memSeries);
    }

    public HeadChunkReader chunksRange(long minTime, long maxTime) {
        long mint = minTime;
        if (this.minTime > minTime) {
            mint = this.minTime;
        }
        return new HeadChunkReader(this, mint, maxTime);
    }

    /**
     * Returns the chunk from the series with the given headChunkId. If chunk is not found, returns null.
     */
    public Chunk chunkFromSeries(MemSeries series, int headChunkId, long minTime, long maxTime) {
        HeadChunk chunk = series.getChunk(headChunkId);
        if (!overlapsClosedInterval(chunk, minTime, maxTime)) {
            return null; // todo err handling
        }

        if (chunk instanceof MMappedChunk mmappedChunk) {
            return mmappedChunk.getChunk(chunkDiskMapper);
        }
        assert chunk instanceof MemChunk : "if the chunk is not MMappedChunk, then it must be MemChunk";
        return ((MemChunk) chunk).getChunk();
    }

    // helper method for testing, replace when query side is further implemented
    public Map<Integer, List<HeadChunk>> matchingChunks(String queryString, long minTime, long maxTime) {
        Map<Integer, List<HeadChunk>> chunks = closedChunkIndex.getChunks(queryString, minTime, maxTime);
        List<Long> matchedSeries = liveSeriesIndex.getReferences(queryString, minTime);

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
        dropEmptySeries(seriesToClosedChunks.keySet());
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
            Set<MemChunk> closedChunks = entry.getValue();

            // locking the series ensures head chunk is not updated concurrently
            series.lockHeadChunk();
            try {
                MemChunk curr = series.getHeadChunk();
                while (curr != null) {
                    if (closedChunks.contains(curr)) {
                        if (curr.getPrev() != null) {
                            curr.getPrev().setNext(curr.getNext());
                        }

                        if (curr.getNext() != null) {
                            curr.getNext().setPrev(curr.getPrev());
                        }

                        if (curr == series.getHeadChunk()) {
                            series.setHeadChunk(curr.getPrev());
                        }
                    }
                    curr = curr.getPrev();
                }
            } finally {
                series.unlockHeadChunk();
            }
        }
    }

    private void dropEmptySeries(Set<MemSeries> series) {
        // todo: remove empty series that have no chunks left, and remove from the index
    }

    /**
     * Writes all non-open MemChunks to disk and replaces them with MMappedChunks.
     */
    public void mmapHeadChunks() {
        stripeSeries.getSeries().forEach(memSeries -> {
            int mappedChunkCount = memSeries.mmapChunks(chunkDiskMapper); // TODO add some metric for mapped chunk count
        });
    }

    /**
     * Removes stale series. Removes old files used for mmapped chunks.
     */
    public void truncate() {
        int minInUseChunkFileIndex = stripeSeries.gc(minTime);
        chunkDiskMapper.truncate(minInUseChunkFileIndex);
    }

    /**
     * Closes the head, flushing any pending writes to disk and writing a snapshot of the head state. Assumes that writes have stopped
     * before this is called.
     */
    public void close() throws IOException {
        chunkDiskMapper.close();

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

    /**
     * Loads existing head chunks from disk. Create MemSeries and MMappedChunks for chunks found in existing files
     *
     * @return a map of seriesRef to MMappedChunk for chunks that do not have a series in memory
     */
    private Map<Long, List<MMappedChunk>> loadExisting() {
        Map<Long, List<MMappedChunk>> chunksWithoutSeries = new HashMap<>();

        Iterator<ChunkDiskMapper.ChunkAndSeriesRef> iterator = chunkDiskMapper.chunkIterator();
        while (iterator.hasNext()) {
            ChunkDiskMapper.ChunkAndSeriesRef chunkAndSeriesRef = iterator.next();

            MMappedChunk mappedChunk = chunkAndSeriesRef.chunk();
            long seriesRef = chunkAndSeriesRef.seriesRef();

            // Ignore chunks older that head cutoff time
            if (mappedChunk.getMaxTimestamp() < minTime) {
                continue;
            }

            // Collect chunks for later if the ref does not exist in head
            MemSeries series = stripeSeries.getById(seriesRef);
            if (series == null) {
                chunksWithoutSeries.computeIfAbsent(seriesRef, k -> new ArrayList<>()).add(mappedChunk);
                continue;
            }

            List<MMappedChunk> seriesMappedChunks = series.getMMappedChunks();
            if (!seriesMappedChunks.isEmpty() && mappedChunk.getMinTimestamp() <= seriesMappedChunks.getLast().getMaxTimestamp()) {
                // If the chunk is not newer than the last chunk in the series, there has been some corruption
                // TODO inc metric
                throw new IllegalStateException("out of sequence mmapped chunk for seriesRef " + seriesRef);
            }

            // Add the chunk to the existing series
            seriesMappedChunks.add(mappedChunk);
        }

        return chunksWithoutSeries;
    }

    /**
     * Writes the current state of the head to disk
     * format: [8b - minTime][8b - maxTime][8b - nextSeriesId][8b - numSeries][[4b - series size][series bytes...][4b - series ref]...]
     */
    private void writeSnapshot() {
        // TODO correlate naming with WAL checkpoints
        File snapshot = Paths.get(snapshotsDir.toString(), SNAPSHOT_FILE_PREFIX + System.currentTimeMillis()).toFile();
        try (RandomAccessFile snapshotFile = new RandomAccessFile(snapshot, "rw")) {
            // Write head metadata
            snapshotFile.writeLong(minTime);
            snapshotFile.writeLong(maxTime);
            snapshotFile.writeLong(seriesId.get());
            snapshotFile.writeLong(numSeries.get());

            // Write all series
            byte[] serializedSeriesBuffer = new byte[MemSeries.MAX_SERIALIZED_SIZE];
            stripeSeries.getSeries().forEach(series -> {
                try {
                    int pos = series.encodeForSnapshot(serializedSeriesBuffer);
                    snapshotFile.writeInt(pos);
                    snapshotFile.write(serializedSeriesBuffer, 0, pos);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write series to snapshot", e);
                }
            });

            snapshotFile.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write snapshot to file: " + snapshot, e);
        }
    }

    /**
     * Load series from the latest snapshot file.
     */
    private void loadFromSnapshot() {
        // Get snapshot with the largest timestamp
        File[] snapshots = snapshotsDir.toFile().listFiles((dir, name) -> name.startsWith(SNAPSHOT_FILE_PREFIX));
        if (snapshots == null || snapshots.length == 0) {
            return; // No snapshots to load
        }
        File latestSnapshot = Arrays.stream(snapshots)
            .max(Comparator.comparingLong(File::lastModified))
            .orElseThrow(() -> new IllegalStateException("No valid snapshot found"));

        try (RandomAccessFile snapshotFile = new RandomAccessFile(latestSnapshot, "r")) {
            // Read minTime and maxTime
            minTime = snapshotFile.readLong();
            maxTime = snapshotFile.readLong();
            long snapshotNextSeriesId = snapshotFile.readLong();
            long snapshotNumSeries = snapshotFile.readLong();
            while (snapshotFile.getFilePointer() < snapshotFile.length()) {
                int size = snapshotFile.readInt();
                byte[] serializedSeries = new byte[size - SERIALIZED_META_SIZE];
                snapshotFile.readFully(serializedSeries);
                byte[] meta = new byte[SERIALIZED_META_SIZE];
                snapshotFile.readFully(meta);
                MemSeries memSeries = MemSeries.decodeFromSnapshot(serializedSeries, meta);
                createSeries(memSeries);
            }
            numSeries.set(snapshotNumSeries);
            seriesId.set(snapshotNextSeriesId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
