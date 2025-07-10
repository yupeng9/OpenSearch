/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.MemSeries;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;

public class LiveSeriesIndexTests extends OpenSearchTestCase {

    public void testHeadIndex() throws IOException, InterruptedException {
        LiveSeriesIndex headIndex = new LiveSeriesIndex(new ShardId("test", "test", 1), createTempDir("testHeadIndex"));
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k2", "v2"), 0L, 100L);
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k3", "v3"), 10L, 100L);
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k4", "v4"), 20L, 200L);

        // allow time for the index to refresh after insertion
        headIndex.getOpenSearchReaderManager().maybeRefreshBlocking();
//        Thread.sleep(2000);

        // search by labels/minTimestamp
        List<Long> refs = headIndex.getReferences("/k1:v1/", 50);
        assertEquals(List.of(0L, 10L, 20L), refs);

        refs = headIndex.getReferences("/k1:v1/", 150);
        assertEquals(List.of(20L), refs);

        refs = headIndex.getReferences("/k1:v.*/", 50);
        assertEquals(List.of(0L, 10L, 20L), refs);

        refs = headIndex.getReferences("/k4:.*/", 50);
        assertEquals(List.of(20L), refs);

        // deletion
        headIndex.removeSeries(List.of(0L, 10L));

        // allow time for the index to refresh after deletion
        headIndex.getOpenSearchReaderManager().maybeRefreshBlocking();
//        Thread.sleep(2000); // allow time for the index to refresh after deletion

        refs = headIndex.getReferences("/k1:v1/", 50);
        assertEquals(List.of(20L), refs);

        refs = headIndex.getReferences("/k2:v2/", 50);
        assertEquals(List.of(), refs);

        headIndex.close();
    }

    public void testLoadSeries() throws IOException, InterruptedException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(new ShardId("test", "test", 1), createTempDir("testLoadSeries"));
        Labels labels1 = Labels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = Labels.fromStrings("k1", "v1", "k3", "v3");
        Labels labels3 = Labels.fromStrings("k1", "v1", "k4", "v4");
        MemSeries series1 = new MemSeries(0L, labels1, false);
        MemSeries series2 = new MemSeries(10L, labels2, false);
        MemSeries series3 = new MemSeries(20L, labels3, false);
        series1.setMaxMmapTimestamp(100L);
        series2.setMaxMmapTimestamp(200L);
        series3.setMaxMmapTimestamp(300L);

        liveSeriesIndex.addSeries(labels1, 0L, 40L);
        liveSeriesIndex.addSeries(labels2, 10L, 50L);
        liveSeriesIndex.addSeries(labels3, 20L, 60L);

        List<MemSeries> activeSeries = List.of(series1, series2, series3);
        liveSeriesIndex.commitWithMetadata(activeSeries);
        liveSeriesIndex.getOpenSearchReaderManager().maybeRefreshBlocking();

        Head head = new Head(createTempDir("testLoadSeries"));
        liveSeriesIndex.loadSeriesFromIndex(head);

        assertEquals(3, head.getNumSeries());
        assertEquals(labels1, head.getStripeSeries().getById(0L).getLabels());
        assertEquals(labels2, head.getStripeSeries().getById(10L).getLabels());
        assertEquals(labels3, head.getStripeSeries().getById(20L).getLabels());

        assertEquals(100L, head.getStripeSeries().getById(0L).getMaxMmapTimestamp());
        assertEquals(200L, head.getStripeSeries().getById(10L).getMaxMmapTimestamp());
        assertEquals(300L, head.getStripeSeries().getById(20L).getMaxMmapTimestamp());

        liveSeriesIndex.close();
    }
}
