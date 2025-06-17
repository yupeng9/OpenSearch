/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.live;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;

import static org.opensearch.ts.head.index.IndexUtils.*;

/**
 * LiveChunkIndex indexes series in the head block which have open chunks.
 */
public class LiveSeriesIndex {

    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;
    private final Thread refreshThread;
    private boolean stopRefresh;

    public LiveSeriesIndex() {
        analyzer = new WhitespaceAnalyzer();
        directory = new ByteBuffersDirectory(); // on heap since this impl only uses references to chunks
        try {
            indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));
            searcherManager = new SearcherManager(indexWriter, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize HeadIndex", e);
        }

        refreshThread = new Thread(() -> {
            while (!stopRefresh) {
                try {
                    searcherManager.maybeRefreshBlocking();
                    Thread.sleep(1000); // refresh every second
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        refreshThread.start();
    }

    // todo: benchmark if minTimestamp actually helps, or if it's easier to resolve from matched series
    public void addSeries(Labels labels, long reference, long minTimestamp) {
        Document doc = new Document();
        doc.add(new TextField(LABELS_FIELD, labels.toKeyValueString(), Field.Store.NO));
        doc.add(new NumericDocValuesField(REFERENCE_FIELD, reference));
        doc.add(new LongPoint(REFERENCE_FIELD, reference));
        doc.add(new LongPoint(MIN_TIMESTAMP_FIELD, minTimestamp)); // live chunks assumed to max infinite max timestamp
        try {
            indexWriter.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeSeries(List<Long> references) throws IOException {
        Query query = LongPoint.newSetQuery(REFERENCE_FIELD, references);
        indexWriter.deleteDocuments(query);
    }

    // todo coordinate on query search interface
    public List<Long> getReferences(String queryString, long minTimestamp) {
        IndexSearcher searcher = null;
        try {
            IndexReader reader = searcherManager.acquire().getIndexReader();
            searcher = new IndexSearcher(reader);
            BooleanQuery query =
                new BooleanQuery.Builder().add(new QueryParser(LABELS_FIELD, analyzer).parse(queryString), BooleanClause.Occur.MUST)
                    .add(LongPoint.newRangeQuery(MIN_TIMESTAMP_FIELD, minTimestamp, Long.MAX_VALUE), BooleanClause.Occur.FILTER)
                    .build();

            LiveSeriesIndexCollectorManager collectorManager = new LiveSeriesIndexCollectorManager();
            return searcher.search(query, collectorManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get references", e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            if (searcher != null) {
                try {
                    searcherManager.release(searcher);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release searcher", e);
                }
            }
        }
    }

    public void close() throws IOException, InterruptedException {
        stopRefresh = true;
        refreshThread.join();
        indexWriter.close();
        directory.close();
        searcherManager.close();
        analyzer.close();
    }
}
