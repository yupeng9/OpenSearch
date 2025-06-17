/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;

public class LiveSeriesIndexTests extends OpenSearchTestCase {

    public void testHeadIndex() throws IOException, InterruptedException {
        LiveSeriesIndex headIndex = new LiveSeriesIndex();
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k2", "v2"), 0L, 100L);
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k3", "v3"), 10L, 100L);
        headIndex.addSeries(Labels.fromStrings("k1", "v1", "k4", "v4"), 20L, 200L);

        Thread.sleep(2000); // allow time for the index to refresh after insertion

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
        Thread.sleep(2000); // allow time for the index to refresh after deletion

        refs = headIndex.getReferences("/k1:v1/", 50);
        assertEquals(List.of(20L), refs);

        refs = headIndex.getReferences("/k2:v2/", 50);
        assertEquals(List.of(), refs);

        headIndex.close();
    }
}
