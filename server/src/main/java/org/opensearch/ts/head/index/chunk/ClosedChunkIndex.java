/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.chunk;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.ts.head.HeadChunk;
import org.opensearch.ts.head.MemChunk;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.ts.head.index.IndexUtils.CHUNK_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.LABELS_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.LABELS_HASH_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.MAX_TIMESTAMP_FIELD;
import static org.opensearch.ts.head.index.IndexUtils.MIN_TIMESTAMP_FIELD;
import static org.opensearch.ts.head.index.chunk.ClosedChunkIndexUtils.getSerializedMemChunk;

/**
 * Simple head index that stores chunks, current as one doc per chunk.
 */
public class ClosedChunkIndex {

    private final Path dir;
    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;

    public ClosedChunkIndex(Path dir) throws IOException {
        Path indexPath = dir.resolve("block_" + System.currentTimeMillis()); // TODO: naming
        Files.createDirectory(indexPath);
        this.dir = indexPath;

        analyzer = new WhitespaceAnalyzer(); // todo tune/span queries?
        directory = new MMapDirectory(indexPath);
        try {
            indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));
            searcherManager = new SearcherManager(indexWriter, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize HeadIndex", e);
        }
    }

    public void addNewChunk(Labels labels, MemChunk memChunk) throws IOException {
        Document doc = new Document();
        doc.add(new TextField(LABELS_FIELD, labels.toKeyValueString(), Field.Store.NO));
        doc.add(new BinaryDocValuesField(CHUNK_FIELD, getSerializedMemChunk(memChunk)));
        doc.add(new NumericDocValuesField(LABELS_HASH_FIELD, labels.hashCode()));
        doc.add(new LongPoint(MIN_TIMESTAMP_FIELD, memChunk.getMinTimestamp()));
        doc.add(new LongPoint(MAX_TIMESTAMP_FIELD, memChunk.getMaxTimestamp()));
        indexWriter.addDocument(doc);
    }

    // todo coordinate on query search interface
    public Map<Integer, List<HeadChunk>> getChunks(String queryString, long minTimestamp, long maxTimestamp) {
        IndexSearcher searcher = null;
        try {
            IndexReader reader = searcherManager.acquire().getIndexReader();
            searcher = new IndexSearcher(reader);

            BooleanQuery query = new BooleanQuery.Builder()
                    .add(new QueryParser(LABELS_FIELD, analyzer).parse(queryString), BooleanClause.Occur.MUST)
                    .add(LongPoint.newRangeQuery(MIN_TIMESTAMP_FIELD, Long.MIN_VALUE, maxTimestamp), BooleanClause.Occur.FILTER)
                    .add(LongPoint.newRangeQuery(MAX_TIMESTAMP_FIELD, minTimestamp, Long.MAX_VALUE), BooleanClause.Occur.FILTER)
                    .build();

            ClosedChunkIndexCollectorManager collector = new ClosedChunkIndexCollectorManager();
            return searcher.search(query, collector);
        } catch (ParseException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                searcherManager.release(searcher);
            } catch (IOException e) {
                // todo: emit metric
            }
        }
    }

    /**
     * Force a refresh, future queries will see the latest changes.
     */
    public void refresh() {
        try {
            searcherManager.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new RuntimeException("Failed to refresh", e);
        }
    }

    /**
     * Commit the changes to the index.
     */
    public void commit() {
        try {
            indexWriter.commit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit", e);
        }
    }

    public Path getDir() {
        return dir;
    }

    public void forceMerge() {
        try {
            indexWriter.forceMerge(1);
        } catch (IOException e) {
            throw new RuntimeException("Failed to force merge", e);
        }
    }

    public void close() {
        try {
            analyzer.close();
            searcherManager.close();
            indexWriter.close();
            directory.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close ClosedChunkIndex", e);
        }
    }
}
