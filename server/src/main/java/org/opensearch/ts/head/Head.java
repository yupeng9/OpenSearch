/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.model.Labels;

import java.util.concurrent.atomic.AtomicLong;

public class Head {
    private HeadAppender appender;
    private long minTime;
    private long maxTime;
    private StripeSeries stripeSeries;
    private AtomicLong seriesId = new AtomicLong(0);
    private AtomicLong numSeries = new AtomicLong(0);

    public Head() {
        minTime = Long.MAX_VALUE;
        maxTime = Long.MIN_VALUE;
        stripeSeries = new StripeSeries();
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

    StripeSeries getStripeSeries() {
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

    public MemSeries createSeries(long hash, Labels labels, boolean pendingCommit) {
        MemSeries series = stripeSeries.getByHash(hash, labels);
        if (series != null) {
            return series;
        }
        long id = seriesId.incrementAndGet();
        return createSeries(id, hash, labels, pendingCommit);
    }

    public MemSeries createSeries(long id, long hash, Labels labels, boolean pendingCommit) {
        MemSeries newSeries = new MemSeries(id, labels, pendingCommit);
        stripeSeries.set(hash, newSeries);
        numSeries.incrementAndGet();
        return newSeries;
    }
}
