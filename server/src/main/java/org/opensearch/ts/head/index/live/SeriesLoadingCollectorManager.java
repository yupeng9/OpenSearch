/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.apache.lucene.search.CollectorManager;
import org.opensearch.ts.head.Head;

import java.io.IOException;
import java.util.Collection;

public class SeriesLoadingCollectorManager implements CollectorManager<SeriesLoadingCollector, Void> {
    private Head head;

    public SeriesLoadingCollectorManager(Head head) {
        this.head = head;
    }

    @Override
    public SeriesLoadingCollector newCollector() throws IOException {
        return new SeriesLoadingCollector(head);
    }

    @Override
    public Void reduce(Collection<SeriesLoadingCollector> collectors) throws IOException {
        return null;
    }
}
