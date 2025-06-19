/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.ts.compactor.Compactor;
import org.opensearch.ts.compactor.LuceneDocPerChunkCompactor;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.head.RangeHead;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * An engine for Metrics
 * TODO: convert this to an OpenSearch engine
 */
public class MetricsEngine extends InternalEngine {

    private static final long MMAP_FREQUENCY = 60 * 1000;
    private static final long COMPACT_FREQUENCY = 60 * 1000;

    private Head head;
    private Path metricsStorePath;
    private ScheduledExecutorService executor;
    private final Lock headCompactionLock = new ReentrantLock();

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

        head = new Head(metricsStorePath);

        executor = Executors.newScheduledThreadPool(2); // TODO check os packages
        startBackgroundJobs();
    }

    protected MetricsAppender newAppender() {
        return new MetricsAppender();
    }

    private void startBackgroundJobs() {
        // periodically mmap head chunks
        executor.scheduleAtFixedRate(head::closeHeadChunks, MMAP_FREQUENCY, MMAP_FREQUENCY, java.util.concurrent.TimeUnit.MILLISECONDS);

        // periodically compact head
        // TODO: maybe this should be done together with mmapheadChunks, one after the other?
        executor.scheduleAtFixedRate(this::maybeSafeCompactHead, COMPACT_FREQUENCY, COMPACT_FREQUENCY, TimeUnit.MILLISECONDS);
    }

    public void close() throws IOException {
        metricsStorePath = null;
        head.close();
        executor.close();
        super.close();
    }

    protected Head getHead() {
        return head;
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
        // TODO: call metrics appender
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, Object> map = XContentHelper.convertToMap(index.parsedDoc().source(), false, index.parsedDoc().getMediaType()).v2();

        MetricDocument metricDocument = MetricDocument.fromJson(map);

        Appender headAppender = newAppender();
        long seriesRef = 0;
        for (MetricDocument.Sample sample : metricDocument.samples) {
            seriesRef = headAppender.append(seriesRef, metricDocument.labels, sample.timestamp, sample.value);
        }
        headAppender.commit();

        builder.value(map);
        logger.info("received sample {}", builder.toString());
        return null;
    }

    @Override
    public void refresh(String source) throws EngineException {
        super.refresh(source);
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
     *            ]
     *          }
     *         """
     * </pre>
     */
    record MetricDocument(Labels labels, List<Sample> samples) {
        record Label(String name, String value) { }
        record Sample(long timestamp, double value) { }

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

            return new MetricDocument(labels, _samples);
        }
    }
}
