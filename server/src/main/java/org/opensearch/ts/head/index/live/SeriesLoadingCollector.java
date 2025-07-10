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
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.MemSeries;
import org.opensearch.ts.model.Labels;

import java.io.IOException;

import static org.opensearch.ts.head.index.IndexUtils.LABELS_FIELD;

public class SeriesLoadingCollector implements Collector {

    private Head head;
    private NumericDocValues referenceValues;
    private StoredFields storedFields;

    public SeriesLoadingCollector(Head head) {
        this.head = head;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        referenceValues = leafReaderContext.reader().getNumericDocValues("reference");
        storedFields = leafReaderContext.reader().storedFields();

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // no scoring needed
            }

            @Override
            public void collect(int doc) throws IOException {
                referenceValues.advanceExact(doc);
                long reference = referenceValues.longValue();

                // todo update once labels is changed to separate stored fields, regex should not be used here
                String labelsKeyValues = storedFields.document(doc).get(LABELS_FIELD);
                String[] keyValues = labelsKeyValues.split("[: ]");

                head.loadExistingSeries(new MemSeries(reference, Labels.fromStrings(keyValues), false));
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }
}
