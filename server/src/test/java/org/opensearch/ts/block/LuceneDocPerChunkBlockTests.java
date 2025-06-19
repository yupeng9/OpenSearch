/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.block;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.XORChunk;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.model.Series;
import org.opensearch.ts.query.Querier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LuceneDocPerChunkBlockTests extends OpenSearchTestCase {

    public void testBlockSeriesSet() throws IOException {
        // This test should validate the functionality of the BlockSeriesSet
        // and ensure it correctly iterates over series in a block.
        // Implement the test logic here.

        Path tempDir = createTempDir();
        Directory directory = new MMapDirectory(tempDir);
        IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        // generate 2 series, each with 2 chunks, each chunk with 120 samples
        // a series is defined by a unique label set, e.g. label1=value1, label2=value2

        List<TestSeries> series = createTestSeries();

        try (var indexWriter = new IndexWriter(directory, config)) {
            for (var s : series) {
                var labels = s.labels();
                var chunks = s.chunks();

                for (var chunk : chunks) {
                    Document doc = createDocument(labels, chunk);

                    indexWriter.addDocument(doc);
                }
            }
            indexWriter.commit();
            indexWriter.forceMerge(1);
        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        }

        var block = new LuceneDocPerChunkBlock(tempDir);
        block.open();
        try {
            var q = block.select(0, Long.MAX_VALUE);
            List<Series> gotSeries = new ArrayList<>();
            while (q.hasNext()) {
                var s = q.next();
                gotSeries.add(s);
            }

            var numInputChunks = series.stream().map(TestSeries::chunks).mapToLong(List::size).sum();
            assertEquals(numInputChunks, gotSeries.size());

            for (var s : series) {
                var labels = s.labels();
                var chunks = s.chunks();

                // TODO: check chunks and labels
            }

            // test basic label filter query
            var q2 = block.select(120000, 240000, new Querier.Matcher(Querier.MatchType.EQUALS, "label1", "value1"));
            List<Series> gotSeries2 = new ArrayList<>();
            while (q2.hasNext()) {
                var s = q2.next();
                gotSeries2.add(s);
            }
            assertEquals(2, gotSeries2.size());

        } finally {
            block.close();
        }
    }

    Document createDocument(Labels labels, TestChunk chunk) {

        Document doc = new Document();
        for (var label : labels.toMapView().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            doc.add(new StringField(label.getKey(), label.getValue(), Field.Store.NO));
            doc.add(new SortedSetDocValuesField("__labels__", new BytesRef(label.getKey() + "=" + label.getValue())));
            // TODO: get time from chunk
            doc.add(new LongPoint("__mint__", chunk.mint()));
            doc.add(new LongPoint("__maxt__", chunk.maxt()));
        }

        ByteBuffer buffer = ByteBuffer.allocate(chunk.chunk().bytesSize() + Integer.BYTES * 2);
        buffer.putInt(chunk.chunk().encoding().ordinal());
        buffer.putInt(chunk.chunk().bytesSize());
        buffer.put(chunk.chunk().bytes());
        // TODO: consider adding CRC
        buffer.flip();

        BytesRef bytesRef = new BytesRef(buffer.array());
        doc.add(new BinaryDocValuesField("__chunks__", bytesRef));

        return doc;
    }

    List<TestSeries> createTestSeries() {

        Labels labels1 = Labels.fromStrings("label1", "value1", "label2", "value2");
        Labels labels2 = Labels.fromStrings("label2", "value2", "label3", "value3");

        TestChunk chunk1 = new TestChunk(createChunk(120, 0, 120000), 0, 120000);
        TestChunk chunk2 = new TestChunk(createChunk(120, 120000, 240000), 120000, 240000);
        TestChunk chunk3 = new TestChunk(createChunk(120, 240000, 360000), 240000, 360000);
        TestChunk chunk4 = new TestChunk(createChunk(120, 360000, 480000), 360000, 480000);

        return List.of(
            new TestSeries(labels1, List.of(chunk1, chunk2)),
            new TestSeries(labels2, List.of(chunk3, chunk4))
        );
    }

    XORChunk createChunk(int sampleCount, long startTime, long endTime) {
        XORChunk chunk = new XORChunk();
        var appender = chunk.appender();

        for (int i = 0; i < sampleCount; i++) {
            appender.append(startTime + i * 1000, 10.0 + i);
        }
        return chunk;
    }

    record TestChunk(Chunk chunk, long mint, long maxt) {}
    record TestSeries(Labels labels, List<TestChunk> chunks) {}
}
