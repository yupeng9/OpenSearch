/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.chunk;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.head.ClosedChunk;
import org.opensearch.ts.head.HeadChunk;

import java.io.IOException;
import java.util.*;

import static org.opensearch.ts.head.index.IndexUtils.*;

public class ClosedChunkIndexCollector implements Collector {

    private final Map<Integer, List<HeadChunk>> seriesToChunks = new HashMap<>();

    private BinaryDocValues chunkBytesValues;
    private NumericDocValues labelsHashValues;
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        chunkBytesValues = leafReaderContext.reader().getBinaryDocValues(CHUNK_FIELD);
        labelsHashValues = leafReaderContext.reader().getNumericDocValues(LABELS_HASH_FIELD);

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // no scoring needed
            }

            @Override
            public void collect(int doc) throws IOException {
                chunkBytesValues.advanceExact(doc);
                labelsHashValues.advanceExact(doc);

                // Labels.hash is guaranteed to be an integer
                seriesToChunks.computeIfAbsent((int) labelsHashValues.longValue(), k -> new ArrayList<>())
                    .add(ClosedChunkIndexUtils.getClosedChunkFromSerialized(chunkBytesValues.binaryValue()));
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public Map<Integer, List<HeadChunk>> getChunks() {
        return seriesToChunks;
    }
}
