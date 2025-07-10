/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeriesRefCollector implements Collector {

    private final List<Long> references = new ArrayList<>();

    private NumericDocValues referenceValues;

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        referenceValues = leafReaderContext.reader().getNumericDocValues("reference");

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // no scoring needed
            }

            @Override
            public void collect(int doc) throws IOException {
                referenceValues.advanceExact(doc);
                long reference = referenceValues.longValue();
                references.add(reference);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public List<Long> getReferences() {
        return references;
    }
}
