/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.model.Labels;

import java.util.List;

public class StripeSeriesTests extends OpenSearchTestCase {

    public void testStripeSeries() {
        StripeSeries series = new StripeSeries();
        Labels labels = Labels.fromStrings("k1", "v1", "k2", "v2");
        long ref = 123L;

        MemSeries memSeries = new MemSeries(ref, labels, false);

        // Test setting and getting values
        series.set(labels.hashCode(), memSeries);
        assertEquals(memSeries, series.getById(ref));
        assertEquals(memSeries, series.getByHash(labels.hashCode(), labels));

        // Test deletion
        series.delete(memSeries);
        assertNull(series.getById(ref));
        assertNull(series.getByHash(labels.hashCode(), labels));
    }
}
