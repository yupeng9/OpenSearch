/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.chunk;

import org.apache.lucene.search.CollectorManager;
import org.opensearch.ts.head.HeadChunk;

import java.io.IOException;
import java.util.*;

public class ClosedChunkIndexCollectorManager implements CollectorManager<ClosedChunkIndexCollector, Map<Integer, List<HeadChunk>>> {
    @Override
    public ClosedChunkIndexCollector newCollector() throws IOException {
        return new ClosedChunkIndexCollector();
    }

    @Override
    public Map<Integer, List<HeadChunk>> reduce(Collection<ClosedChunkIndexCollector> collectors) throws IOException {
        Map<Integer, List<HeadChunk>> seriesToChunks = new HashMap<>();
        for (ClosedChunkIndexCollector collector : collectors) {
            Map<Integer, List<HeadChunk>> collectorSeriesToChunks = collector.getChunks();
            seriesToChunks.putAll(collectorSeriesToChunks);
        }
        return seriesToChunks;
    }
}
