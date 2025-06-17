/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LiveSeriesIndexCollectorManager implements CollectorManager<LiveSeriesIndexCollector, List<Long>> {
    @Override
    public LiveSeriesIndexCollector newCollector() throws IOException {
        return new LiveSeriesIndexCollector();
    }

    @Override
    public List<Long> reduce(Collection<LiveSeriesIndexCollector> collectors) throws IOException {
        List<Long> refs = new ArrayList<>();
        for (LiveSeriesIndexCollector collector : collectors) {
            List<Long> collectorRefs = collector.getReferences();
            if (collectorRefs != null) {
                refs.addAll(collectorRefs);
            }
        }
        return refs;
    }
}
