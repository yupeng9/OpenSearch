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
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An engine for Metrics
 * TODO: convert this to an OpenSearch engine
 */
public class MetricsEngine extends InternalEngine {

    private static final long MMAP_FREQUENCY = 60 * 1000;
    private static final long GC_FREQUENCY = 5 * 60 * 1000;

    private Head head;

    private Path metricsStorePath;

    private ScheduledExecutorService executor;

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

    public MetricsAppender newAppender() {
        return new MetricsAppender();
    }

    private void startBackgroundJobs() {
        // periodically mmap head chunks
        executor.scheduleAtFixedRate(head::closeHeadChunks, MMAP_FREQUENCY, MMAP_FREQUENCY, java.util.concurrent.TimeUnit.MILLISECONDS);

        // periodically remove stale series and old chunks
        executor.scheduleAtFixedRate(head::truncate, GC_FREQUENCY, GC_FREQUENCY, java.util.concurrent.TimeUnit.MILLISECONDS);
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

    @Override
    public IndexResult index(Index index) throws IOException {
        // TODO: call metrics appender
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, Object> map = XContentHelper.convertToMap(index.parsedDoc().source(), false, index.parsedDoc().getMediaType()).v2();
        builder.value(map);
        logger.info("received sample {}", builder.toString());
        return null;
    }

    public class MetricsAppender implements Appender {

        HeadAppender headAppender;

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
}
