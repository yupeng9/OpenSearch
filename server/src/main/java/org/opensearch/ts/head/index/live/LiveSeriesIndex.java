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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.OpenSearchReaderManager;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.MemSeries;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ts.head.index.IndexUtils.LABELS_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.MIN_TIMESTAMP_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.REFERENCE_FIELD;

/**
 * LiveChunkIndex indexes series in the head block which have open chunks.
 */
public class LiveSeriesIndex {
    private static final String SERIES_METADATA_KEY = "live_series_metadata";

    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final OpenSearchReaderManager searcherManager;
//    private final Thread refreshThread;
    private final DirectoryReader directorReader;
    private final OpenSearchDirectoryReader opensearchDirectoryReader;
    private boolean stopRefresh;

    public LiveSeriesIndex(ShardId shardId, Path dir) throws IOException {
        Path indexPath = dir.resolve("live_series_index");
        if (Files.notExists(indexPath)) {
            Files.createDirectory(indexPath);
        }

        analyzer = new WhitespaceAnalyzer();
        directory = new MMapDirectory(indexPath);
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
        doc.add(new TextField(LABELS_FIELD, labels.toKeyValueString(), Field.Store.YES));
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

            SeriesRefCollectorManager collectorManager = new SeriesRefCollectorManager();
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

    /**
     * Creates MemSeries in the given head based on references/labels stored in the index, as well as metadata in the LiveCommitData
     */
    public void loadSeriesFromIndex(Head head) {
        OpenSearchDirectoryReader reader = null;
        try {
            // create MemSeries
            reader = searcherManager.acquire();
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.search(new MatchAllDocsQuery(), new SeriesLoadingCollectorManager(head));

            // update MemSeries with the saved timestamps
            Iterable<Map.Entry<String, String>> commitData = indexWriter.getLiveCommitData();
            if (commitData == null) {
                return;
            }

            for (Map.Entry<String, String> entry : commitData) {
                if (entry.getKey().equals(SERIES_METADATA_KEY)) {
                    String seriesMetadata = entry.getValue();
                    byte[] bytes = Base64.getDecoder().decode(seriesMetadata);
                    try (BytesStreamInput input = new BytesStreamInput(bytes)) {
                        while (input.available() > 0) {
                            long ref = input.readVLong();
                            long ts = input.readVLong();
                            head.getStripeSeries().getById(ref).setMaxMmapTimestamp(ts);
                        }
                    }
                }
            }
        } catch (IOException e) {
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

    /**
     * Commit the current state, including live series references and their max mmap timestamps. This data is used during translog replay to
     * skip adding samples for data that has already been committed.
     */
    public void commitWithMetadata(List<MemSeries> liveSeries) {
        Map<String, String> commitData = new HashMap<>();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            for (MemSeries series : liveSeries) {
                output.writeVLong(series.getReference());
                output.writeVLong(series.getMaxMmapTimestamp());;
            }
            String liveSeriesMetadata = new String(Base64.getEncoder().encode(output.bytes().toBytesRef().bytes));
            commitData.put(SERIES_METADATA_KEY, liveSeriesMetadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize live series", e);
        }

        try {
            commitWithMetadata(() -> commitData.entrySet().iterator());
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit ", e);
        }
    }

    private void commitWithMetadata(Iterable<Map.Entry<String, String>> commitData) throws IOException {
        indexWriter.setLiveCommitData(commitData, true); // force increment version
        indexWriter.commit();
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
