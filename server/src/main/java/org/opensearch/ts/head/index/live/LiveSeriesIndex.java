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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.OpenSearchReaderManager;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;

import static org.opensearch.ts.head.index.IndexUtils.LABELS_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.MIN_TIMESTAMP_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.REFERENCE_FIELD;

/**
 * LiveChunkIndex indexes series in the head block which have open chunks.
 */
public class LiveSeriesIndex {

    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final OpenSearchReaderManager searcherManager;
//    private final Thread refreshThread;
    private final DirectoryReader directorReader;
    private final OpenSearchDirectoryReader opensearchDirectoryReader;
    private boolean stopRefresh;

    public LiveSeriesIndex(ShardId shardId) {
        analyzer = new WhitespaceAnalyzer();
        directory = new ByteBuffersDirectory(); // on heap since this impl only uses references to chunks
        try {
            indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));
//            searcherManager = new SearcherManager(indexWriter, null);

            directorReader = DirectoryReader.open(indexWriter, true, false);
            opensearchDirectoryReader = OpenSearchDirectoryReader.wrap(directorReader, shardId);
            searcherManager = new OpenSearchReaderManager(opensearchDirectoryReader);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize HeadIndex", e);
        }

        /*
           TODO: this thread seems to cause some dead lock issue after we wire it up to the engine.
            May need to think about how to handle this, because as-is, the engine will already call refresh() on a timer based on the index.settings.refresh_interval, so this isn't technically required.
            however, if we want to decouple the live index refresh and the block index refresh, then there's currently isn't a way to have 2 different interval settings in index settings. So we may need to revisit.
         */
//        refreshThread = new Thread(() -> {
//            while (!stopRefresh) {
//                try {
//                    searcherManager.maybeRefreshBlocking();
//                    Thread.sleep(1000); // refresh every second
//                } catch (IOException | InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        });
//        refreshThread.start();
    }

    // todo: benchmark if minTimestamp actually helps, or if it's easier to resolve from matched series
    public void addSeries(Labels labels, long reference, long minTimestamp) {
        Document doc = new Document();
        doc.add(new TextField(LABELS_FIELD, labels.toKeyValueString(), Field.Store.NO));
        // placeholder for local testing, adding this makes filter queris at the OpenSearch level work with our schema
        // mapping.
//        for (var label : labels.toMapView().entrySet()) {
//            doc.add(new StringField("labels.name", label.getKey(),  Field.Store.NO));
//            doc.add(new StringField("labels.value", label.getValue(), Field.Store.NO));
//        }
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
        OpenSearchDirectoryReader reader = null;
        try {
            reader = searcherManager.acquire();
            IndexSearcher searcher = new IndexSearcher(reader);
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
            if (reader != null) {
                try {
                    searcherManager.release(reader);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release searcher", e);
                }
            }
        }
    }

    public void close() throws IOException, InterruptedException {
        stopRefresh = true;
//        refreshThread.join();
        indexWriter.close();
        directory.close();
        searcherManager.close();
        analyzer.close();
    }

    public OpenSearchReaderManager getOpenSearchReaderManager() {
        return searcherManager;
    }

    public long commit(boolean force, boolean waitIfOngoing) throws IOException {
        long seqNo;
        if (true /* waitIfOngoing */) {
            // TODO: handle locking. We might want to do that in the engine layer, or here.
            seqNo = indexWriter.commit();
        }
        if (force) {
            searcherManager.maybeRefreshBlocking();
        } else {
            searcherManager.maybeRefresh();
        }
        return seqNo;
    }
}
