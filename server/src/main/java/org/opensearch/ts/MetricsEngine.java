/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.model.Labels;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An engine for Metrics
 * TODO: convert this to an OpenSearch engine
 */
public class MetricsEngine {

    private static final long MMAP_FREQUENCY = 60 * 1000;
    private static final long GC_FREQUENCY = 5 * 60 * 1000;

    private Head head;

    private ScheduledExecutorService executor;

    public MetricsEngine(Path dir) {
        head = new Head(dir);

        executor = Executors.newScheduledThreadPool(2); // TODO check os packages
        startBackgroundJobs();
    }

    public MetricsAppender newAppender() {
        return new MetricsAppender();
    }

    private void startBackgroundJobs() {
        // periodically mmap head chunks
        executor.scheduleAtFixedRate(head::mmapHeadChunks, MMAP_FREQUENCY, MMAP_FREQUENCY, java.util.concurrent.TimeUnit.MILLISECONDS);

        // periodically remove stale series and old chunks
        executor.scheduleAtFixedRate(head::truncate, GC_FREQUENCY, GC_FREQUENCY, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void close() {
        executor.close();
    }

    protected Head getHead() {
        return head;
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
