/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.compactor;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ts.block.BlockReader;
import org.opensearch.ts.block.BlockSeriesSet;
import org.opensearch.ts.chunks.ChunkIterator;
import org.opensearch.ts.chunks.ChunkReader;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.model.Series;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * LuceneDocPerChunkCompactor writes blocks of time series as a single underlying Lucene index.
 * Each time series chunk is stored as a single document in the index, which includes both the series metadata, inverted index, and the encoded chunk data as a docvalue.
 * Each series can have multiple chunks, so each series can have multiple documents in the index.
 * The Lucene index can potentially have multiple segments, that should be abstracted away from the user.
 */
public class LuceneDocPerChunkCompactor implements Compactor {

    @Override
    public List<Metadata> Write(Path destDir, BlockReader reader, long minTime, long maxTime, Metadata base) throws IOException {
        // TODO: generate ULID like prometheus (maybe not necessary)
        String blockId = UUID.randomUUID().toString();
        Path destBlockDir = destDir.resolve(blockId);

        // TODO: extract lucene stuff
        MMapDirectory mmapDir = new MMapDirectory(destBlockDir);
        IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer()).setUseCompoundFile(false);
        try(IndexWriter indexWriter = new IndexWriter(mmapDir, config)) {
            ChunkReader chunks = reader.chunks();

            // TODO: implement chunk reader
            //  This should be reading from the head block.
            var blockSeriesSet = new BlockSeriesSet(chunks, minTime, maxTime);

            while (blockSeriesSet.hasNext()) {
                Series series = blockSeriesSet.next();
                // Process each series as needed, e.g., write to destination directory

                Labels labels = series.getLabels();
                Map<String, String> labelMap = labels.toMapView();
                List<Map.Entry<String, String>> sortedLabels = labelMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

                Document seriesChunkDoc = new Document();
                for (var label : sortedLabels) {
                    // Here you would add fields to the document for each label
                    // For example:
                    seriesChunkDoc.add(new StringField(label.getKey(), label.getValue(), Field.Store.NO));

                    String keyValuePair = label.getKey() + "=" + label.getValue();
                    seriesChunkDoc.add(new SortedSetDocValuesField("__labels__", new BytesRef(keyValuePair)));
                }

                ChunkIterator seriesIter = series.iterator();
                ChunkIterator.ValueType iterValueType;
                while ((iterValueType = seriesIter.next()) != ChunkIterator.ValueType.NONE) {
                    var sample = seriesIter.next();
                    // TODO: get encoded whole chunk and set as docvalue
                    switch (iterValueType) {
                        case FLOAT:
                            // TODO: encode
                            break;
                        case NONE:
                            // No more values to read
                            break;
                    }
                }

                indexWriter.addDocument(seriesChunkDoc);
            }

            indexWriter.commit();
            // TODO: make this optional
            indexWriter.forceMerge(1);
        }

        Metadata metadata = new Metadata(blockId);
        return List.of(metadata);
    }
}
