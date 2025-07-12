/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.ts.Appender;
import org.opensearch.ts.block.LuceneDocPerChunkBlock;
import org.opensearch.ts.compactor.Compactor;
import org.opensearch.ts.compactor.LuceneDocPerChunkCompactor;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.head.MemSeries;
import org.opensearch.ts.head.RangeHead;
import org.opensearch.ts.head.index.chunk.ClosedChunkIndex;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.query.Querier;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;


/**
 * An engine for Metrics
 * TODO: convert this to an OpenSearch engine
 */
public class MetricsEngine extends Engine {

    private static final long MMAP_FREQUENCY = Duration.ofMinutes(1).getSeconds();
    private static final long FLUSH_FREQUENCY = Duration.ofHours(2).getSeconds();

    private Head head;
    private Path metricsStorePath;
    private ScheduledExecutorService executor;
    private final Lock headCompactionLock = new ReentrantLock();
    private final Lock closeChunksLock = new ReentrantLock(); // control closing head chunks during ops like flushing head
    private final ReadWriteLock blocksLock = new ReentrantReadWriteLock(); // thread-safe access to blocks
    private final List<LuceneDocPerChunkBlock> blocks = new ArrayList<>();

    // Engine state management
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes = new AtomicLong(0);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
    private final AtomicBoolean isThrottled = new AtomicBoolean(false);
    private final ReleasableLock throttleLock = new ReleasableLock(new ReentrantLock());
    private final TranslogManager translogManager;
    private final String historyUUID;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final LongSupplier primaryTermSupplier;

    public MetricsEngine(EngineConfig engineConfig) throws IOException {
        this(engineConfig, null);
    }

    public MetricsEngine(EngineConfig engineConfig, Path path) throws IOException {
        super(engineConfig);

        if (engineConfig.getStore().shardPath() != null) {
            this.metricsStorePath = engineConfig.getStore().shardPath().getDataPath().resolve("metrics");
        } else {
            // FIXME: path is passed in for testing purpose
            this.metricsStorePath = path;
        }

        Files.createDirectories(metricsStorePath);

        head = new Head(metricsStorePath, engineConfig.getShardId());


        store.incRef();
        try {
            this.localCheckpointTracker = createLocalCheckpointTracker();
            String translogUUID = Objects.requireNonNull(store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY));
            this.translogManager = new InternalTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                getTranslogDeletionPolicy(engineConfig),
                shardId,
                readLock,
                this::getLocalCheckpointTracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                this::ensureOpen,
                engineConfig.getTranslogFactory(),
                engineConfig.getStartedPrimarySupplier()
            );
            this.primaryTermSupplier = engineConfig.getPrimaryTermSupplier();

            // Try to load history UUID from store, or generate a consistent one
            String historyUUIDValue;
            try {
                final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
                String existingHistoryUUID = userData.get(HISTORY_UUID_KEY);
                if (existingHistoryUUID != null) {
                    historyUUIDValue = existingHistoryUUID;
                } else {
                    // Generate a consistent history UUID based on the shard path
                    historyUUIDValue = "metrics-history-" + engineConfig.getShardId().toString().hashCode();
                }
            } catch (Exception e) {
                // If we can't read from store, generate a consistent history UUID
                historyUUIDValue = "metrics-history-" + engineConfig.getShardId().toString().hashCode();
            }
            this.historyUUID = historyUUIDValue;
        } finally {
            store.decRef(); // Ensure we release the store reference
        }

        loadMemSeries();

        executor = Executors.newScheduledThreadPool(2); // TODO check os packages
        startBackgroundJobs();
    }

    private LocalCheckpointTracker createLocalCheckpointTracker() throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            store.readLastCommittedSegmentsInfo().userData.entrySet()
        );
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;

        // Initialize local checkpoint tracker with the max seq no and local checkpoint from the store
        return new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
    }

    private LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    protected MetricsAppender newAppender() {
        return new MetricsAppender();
    }

    private void startBackgroundJobs() {
        // periodically mmap head chunks
        executor.scheduleAtFixedRate(() -> {
            closeChunksLock.lock();
            try {
                logger.info("MMAPing head chunks");
                head.closeHeadChunks();
                head.getLiveSeriesIndex().commitWithMetadata(head.getStripeSeries().getSeries());
            } catch (Exception e) {
                logger.error("Error while MMAPing head chunks", e);
            } finally {
                closeChunksLock.unlock();
            }
        }, MMAP_FREQUENCY, MMAP_FREQUENCY, TimeUnit.SECONDS);

        // periodically compact head
        // TODO: maybe this should be done together with mmapheadChunks, one after the other?
        executor.scheduleAtFixedRate(this::flushHead, FLUSH_FREQUENCY, FLUSH_FREQUENCY, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        metricsStorePath = null;
        head.close();
        executor.close();
        translogManager.close();
        super.close();
    }

    protected Head getHead() {
        return head;
    }

    /**
     * Close the Head's current closedChunkIndex and make it available for querying via blocks. After this is called, closing chunks will
     * add them to a new closedChunkIndex, which becomes the new current closedChunkIndex.
     */
    public void flushHead() {
        ClosedChunkIndex closedChunkIndex = head.getCurrentClosedChunkIndex();
        // Temporarily stop closing head chunks by acquiring the lock, since we're going to open a ref based on the current state
        closeChunksLock.lock();
        try {
            closedChunkIndex.forceMerge(); // TODO consider performing this later in the background
            closedChunkIndex.commit();
            LuceneDocPerChunkBlock pendingBlock = new LuceneDocPerChunkBlock(closedChunkIndex.getDir());
            pendingBlock.open();

            // Make the pending block available for queries and switch the head to a new closedChunkIndex
            head.prepareNewClosedChunkIndex();
            blocksLock.writeLock().lock();
            try {
                head.cutClosedChunkIndex();
                blocks.add(pendingBlock);
            } finally {
                blocksLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeChunksLock.unlock(); // It's safe to close head chunks again
        }

        closedChunkIndex.close();
    }

    private void loadMemSeries() {
        head.getLiveSeriesIndex().loadSeriesFromIndex(head);
    }

    public Querier querier(long minTimetamp, long maxTimestamp) {
        List<Querier> queriers = new ArrayList<>();

        // TODO mint/maxt pruning
        blocksLock.readLock().lock();
        try {
            queriers.add(head.newHeadQuerier()); // Ensure query accuracy by creating a Querier holding the current state of the head block

            for (LuceneDocPerChunkBlock block : blocks) {
                queriers.add(block);
            }
        } finally {
            blocksLock.readLock().unlock();
        }

        return null; // todo return merged querier of List<Querier> queriers
    }

    /**
     * Determines if the head should be compacted, based on Head block chunk sizes, time elapsed since last compaction, etc.
     *
     * @return boolean indicating whether the head should be compacted
     */
    public boolean shouldCompactHead() {
        // TODO: implement logic to determine if head should be compacted, based on head block chunk sizes, etc.
        return true;
    }

    /**
     * Compact head if conditions are met, execepts are swallowed so we don't crash the engine.
     */
    public void maybeSafeCompactHead() {
        try {
            if (!shouldCompactHead()) {
                return;
            }

            // TODO: this should be a singleton, and probably have some minimum time between compactions
            if (headCompactionLock.tryLock()) {
                RangeHead rangeHead = null;
                try {
                    // TODO: correctly set min/max time for the range head
                    rangeHead = new RangeHead(head, 0, Long.MAX_VALUE);
                    compactHead(rangeHead);
                } catch (Exception e) {
                    throw new EngineException(shardId, "Failed to compact head, rangeHead[{}]", rangeHead, e);
                } finally {
                    headCompactionLock.unlock();
                }
            }
        } catch (Exception e) {
            // swallow exceptions, we don't want to crash the engine
            logger.error("Error during head compaction", e);
        }
    }

    public void compactHead(RangeHead rangeHead) throws IOException {
        // TODO: make compactor configurable, implement logic.
        Compactor compactor = new LuceneDocPerChunkCompactor();
        var path = Path.of(metricsStorePath.toString(), "compacted");
        List<Compactor.Metadata> metadata = compactor.Write(path, rangeHead, rangeHead.getMinTime(), rangeHead.getMaxTime(), null);
        // TODO: verify block written
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        try {
            // Generate new seq number, or catch up
            if (index.origin() == Operation.Origin.PRIMARY) {
                index = new Index(
                    index.uid(),
                    index.parsedDoc(),
                    localCheckpointTracker.generateSeqNo(),
                    index.primaryTerm(),
                    index.version(),
                    index.versionType(),
                    index.origin(),
                    index.startTime(),
                    index.getAutoGeneratedIdTimestamp(),
                    index.isRetry(),
                    index.getIfSeqNo(),
                    index.getIfPrimaryTerm());
            } else {
                localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
            }

            Map<String, Object> map = XContentHelper.convertToMap(index.parsedDoc().source(), false, index.parsedDoc().getMediaType()).v2();
            MetricDocument metricDocument = MetricDocument.fromJson(map);

            Appender appender = newAppender();
            long seriesRef = 0;
            for (MetricDocument.Sample sample : metricDocument.samples) {
                seriesRef = appender.append(seriesRef, metricDocument.labels, sample.timestamp, sample.value);
            }
            appender.commit();

            if (logger.isDebugEnabled()) {
                logger.debug("received sample {}", map);
            }
            long dummyVersion = System.nanoTime(); // TODO: use a real versioning system
            var indexResult = new IndexResult(
                dummyVersion,
                index.primaryTerm(),
                index.seqNo(),
                true
            );

            // TODO refine interfaces, Appender handles creating multiple series per appender, but Index() only accepts a single series
            List<MemSeries> series = appender.createdSeries();
            if (series.isEmpty()) {
                // If no series were created, then rewrite the source without labels and with the seriesRef
                try (XContentBuilder builder = SmileXContent.contentBuilder()) {
                    builder.startObject();
                    builder.field("series_ref", seriesRef);
                    builder.startArray("samples");
                    for (MetricDocument.Sample sample : metricDocument.samples) {
                        builder.startObject();
                        builder.field("timestamp", sample.timestamp);
                        builder.field("value", sample.value);
                        builder.endObject();
                    }
                    builder.endArray();
                    builder.endObject();
                    index.parsedDoc().setSource(BytesReference.bytes(builder), XContentType.SMILE);
                }
            }
            // TODO it may be necessary to re-write the source to include the seriesRef anyway, for deterministic replay (or, add locking)

            final var location = translogManager.add(new Translog.Index(index, indexResult));
            indexResult.setTranslogLocation(location);
            return indexResult;
        } catch (IOException e) {
            throw new EngineException(shardId, "Failed to index metric document", e);
        }
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        // For now, just return a success result without actually deleting
        return new DeleteResult(1L, delete.primaryTerm(), delete.seqNo(), false);
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        return new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) throws EngineException {
        // For now, return a simple result indicating the document doesn't exist
        // This will be improved when we implement proper search functionality
        return GetResult.NOT_EXISTS;
    }

    @Override
    public void refresh(String source) throws EngineException {
        // For now, do nothing - the head manages its own refresh cycle
        refreshInternal(source, SearcherScope.EXTERNAL, true);
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return refreshInternal(source, SearcherScope.EXTERNAL, false);
    }

    private boolean refreshInternal(String source, SearcherScope scope, boolean force) {
        // For now, return false - no refresh needed
        try {
            var refManager = this.getReferenceManager(scope);
            if (force) {
                refManager.maybeRefreshBlocking();
                return true; // Indicate that a refresh was performed
            } else {
                return refManager.maybeRefresh();
            }
        } catch (IOException e) {
            throw new RefreshFailedEngineException(shardId, e);
        }
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        refreshInternal("writeIndexingBuffer", SearcherScope.INTERNAL, true);
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        // For now, return false - no periodic flush needed
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        // For now, do nothing - the head manages its own flush cycle
        try {
            this.head.getLiveSeriesIndex().commit(force, waitIfOngoing);
        } catch (IOException e) {
            throw new FlushFailedEngineException(shardId, e);
        }
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments, String forceMergeUUID) throws EngineException, IOException {
        // For now, do nothing - no force merge needed
    }

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        // For now, return null - no index commits in metrics engine
        return null;
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        // For now, return null - no safe index commits in metrics engine
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        // For now, return a simple safe commit info
        return new SafeCommitInfo(0L, 0);
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        // For now, return a no-op closeable
        return () -> {};
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount) throws IOException {
        // For now, return an empty snapshot
        return new Translog.Snapshot() {
            @Override
            public Translog.Operation next() {
                return null;
            }

            @Override
            public void close() {
                // No-op
            }

            @Override
            public int totalOperations() {
                return 0;
            }
        };
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        // For now, return 0
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        // For now, return true
        return true;
    }

    @Override
    public long getMinRetainedSeqNo() {
        // For now, return 0
        return 0;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        // For now, return 0
        return 0;
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        // For now, return 0
        return 0;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        // For now, return a simple seq no stats
        return new SeqNoStats(0L, 0L, 0L);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        // For now, return 0
        return 0;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        // For now, return 0
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        // For now, return an empty list
        return new ArrayList<>();
    }

    @Override
    public void activateThrottling() {
        isThrottled.set(true);
    }

    @Override
    public void deactivateThrottling() {
        isThrottled.set(false);
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // For now, return 0
        return 0;
    }

    @Override
    public void maybePruneDeletes() {
        // For now, do nothing
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttleTimeMillisMetric.count();
    }

    @Override
    public boolean isThrottled() {
        return isThrottled.get();
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        // For now, return null - no segment infos in metrics engine
        return null;
    }

    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        // For now, return null - no segment infos in metrics engine
        return null;
    }

    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        // ReferenceManager for LiveSeriesIndex
        return head.getLiveSeriesIndex().getOpenSearchReaderManager();
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        try {
            if (head != null) {
                head.close();
            }
            if (executor != null) {
                executor.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing metrics engine", e);
        } finally {
            closedLatch.countDown();
        }
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    @Override
    public long getWritingBytes() {
        // For now, return 0
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        // For now, return a simple completion stats
        return new CompletionStats(0L, null);
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    /**
     * This is scoped to a single request, and is not thread safe.
     */
    public class MetricsAppender implements Appender {

        private HeadAppender headAppender;

        public MetricsAppender() {

        }

        @Override
        public long append(long seriesRef, Labels labels, long timestamp, double value) {
            if (headAppender == null) {
                head.initTime(timestamp);
                headAppender = head.newAppender();
            }
            return headAppender.append(seriesRef, labels, timestamp, value);
        }

        @Override
        public void commit() {
            if (headAppender == null) {
                throw new IllegalStateException("head appender is null");
            }
            headAppender.commit();

            // TODO: perform compaction
        }

        @Override
        public List<MemSeries> createdSeries() {
            return headAppender.createdSeries();
        }

        @Override
        public void abort() {

        }
    }

    /**
     * TODO: Place holder, make this a schema driven type.
     *
     * POJO representing metric document in format of
     * <pre>
     *     """
     *         {
     *            "labels": [
     *              {"name": "__name__", "value":"http_requests_total"},
     *              {"name": "method", "value":"POST"},
     *              {"name": "handler", "value":"/api/items"},
     *              {"name": "status", "value":"200"}
     *            ],
     *            "samples":[
     *              {
     *                "timestamp": 1712576200.000,
     *                "value":1024
     *              }
     *            ],
     *            "seriesRef": 1234
     *          }
     *         """
     * </pre>
     */
    public record MetricDocument(Labels labels, List<Sample> samples, SeriesRef seriesRef) {
        public record Label(String name, String value) { }
        public record Sample(long timestamp, double value) { }
        public record SeriesRef(long ref) { }

        public static MetricDocument fromJson(Map<String, Object> source) {
            var inputLabels = (List<Map<String, String>>) source.getOrDefault("labels", List.of());
            var _labels = inputLabels.stream()
                .map(label -> new Label(label.get("name"), label.get("value")))
                .toList();

            Map<String, String> labelsMap = _labels.stream()
                .collect(Collectors.toMap(Label::name, Label::value));
            Labels labels = new Labels(labelsMap);

            var samples = (List<Map<String, Object>>) source.getOrDefault("samples", List.of());
            var _samples = samples.stream()
                .map(sample -> new Sample(
                    ((Number) sample.get("timestamp")).longValue(),
                    ((Number) sample.get("value")).doubleValue()
                )).toList();


            Number seriesRefValue = (Number) source.getOrDefault("seriesRef", null);
            SeriesRef seriesRef = seriesRefValue == null ? null : new SeriesRef(seriesRefValue.longValue());

            return new MetricDocument(labels, _samples, seriesRef);
        }
    }
}
