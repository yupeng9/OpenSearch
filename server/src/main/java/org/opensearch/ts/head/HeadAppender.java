/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.Appender;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.model.RefSample;
import org.opensearch.ts.utils.Constants;

import java.util.ArrayList;
import java.util.List;

public class HeadAppender implements Appender {
    // the actual appender
    private final Head head;
    private long minValidTimestamp;
    private long minTime;
    private long maxTime;
    private boolean closed;

    // TODO: do we need to track labels as in prometheus?
    // TODO: can we simplify this?
    private List<Long> seriesRefs;  // holds ref id to the newly created series
    private List<MemSeries> seriesList; // pointers to the new memSeries, same order as seriesRefs
    private List<RefSample> refSamples; // samples along with the series reference
    private List<MemSeries> sampleSeries; // pointers to the series corresponding to the samples

    public HeadAppender(Head head, long minValidTimestamp, long minTime, long maxTime) {
        this.head = head;
        this.minValidTimestamp = minValidTimestamp;
        this.minTime = minTime;
        this.maxTime = maxTime;

        seriesRefs = new ArrayList<>();
        seriesList = new ArrayList<>();
        refSamples = new ArrayList<>();
        sampleSeries = new ArrayList<>();
    }

    @Override
    public long append(long seriesRef, Labels labels, long timestamp, double value) {
        // TODO: ooo support
        MemSeries series = head.getStripeSeries().getById(seriesRef);
        if (series == null) {
            series = createSeries(labels, timestamp);
        }
        if (timestamp < minTime) {
            minTime = timestamp;
        }
        if (timestamp > maxTime) {
            maxTime = timestamp;
        }
        refSamples.add(new RefSample(series.getReference(), timestamp, value));
        sampleSeries.add(series);
        return series.getReference();
    }

    public MemSeries createSeries(Labels labels, long timestamp) {
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("Labels cannot be empty");
        }

        // TODO: use better hashcode
        MemSeries series = head.createSeries(labels.hashCode(), labels, true, timestamp);
        seriesRefs.add(series.getReference());
        seriesList.add(series);
        return series;
    }

    @Override
    public void commit() {
        if (closed) {
            throw new IllegalStateException("Appender is closed");
        }

        CommitContext context = new CommitContext(new ChunkOptions(Constants.DEFAULT_BLOCK_DURATION, Constants.DEFAULT_SAMPLES_PER_CHUNK));
        commitSamples(context);

        // TODO: commit metadata
        closed = true;
    }

    protected void commitSamples(CommitContext context) {
        for (int i = 0; i < refSamples.size(); i++) {
            RefSample refSample = refSamples.get(i);
            MemSeries s = sampleSeries.get(i);

            if (s.isOOO(refSample.getTimestamp())) {
                return; // TODO: ooo handling - for now skip
            }

            // TODO: appender isolation handling
            boolean chunkCreated = s.append(refSample.getTimestamp(), refSample.getValue(), context.options);
            if (chunkCreated) {
                // TODO: update metrics
            }
            s.commit();
        }
    }

    @Override
    public void abort() {

    }

    public record CommitContext(ChunkOptions options) {}
}
