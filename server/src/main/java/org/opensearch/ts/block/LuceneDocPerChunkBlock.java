/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.block;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ts.chunks.ChimpChunk;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkIterator;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.ImmutableRawChunk;
import org.opensearch.ts.chunks.Meta;
import org.opensearch.ts.chunks.XORChunk;
import org.opensearch.ts.model.Labels;
import org.opensearch.ts.model.Series;
import org.opensearch.ts.model.SeriesSet;
import org.opensearch.ts.query.Querier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * LuceneDocPerChunkBlock is a block backed by a Lucene index.
 */
public class LuceneDocPerChunkBlock implements BlockReader, Querier {
    private static final String CHUNK_FIELD_NAME = "__chunks__";
    private static final String mintFieldName = "__mint__";
    private static final String maxtFieldName = "__maxt__";
    private static final String labelsFieldName = "__labels__";

    private final Path luceneDir;
    private IndexReader indexReader;

    public LuceneDocPerChunkBlock(Path luceneDir) {
        // TODO store block min/max ts for block level pruning
        this.luceneDir = luceneDir;
    }

    public boolean isOpen() {
        return indexReader != null;
    }

    public void open() throws IOException {
        // TODO: open index
        MMapDirectory directory = new MMapDirectory(luceneDir);
        indexReader = DirectoryReader.open(directory);
    }

    public void close() throws IOException {
        // TODO: close index
        if (indexReader != null) {
            indexReader.close();
        }
    }

    @Override
    public ChunkReader chunks() throws IOException {
        return new ChunkReader(indexReader);
    }

    @Override
    public SeriesSet select(long mint, long maxt, Matcher... matchers) {
        Query query = createQuey(mint, maxt, matchers);
        return new LuceneDocPerChunkBlockSeriesSet(indexReader, query, mint, maxt);
    }

    private Query createQuey(long mint, long maxt, Matcher[] matchers) {
        if (matchers == null || matchers.length == 0) {
            // If no matchers are provided, we return all documents in the time range.
            return new MatchAllDocsQuery();
        }

        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (Matcher matcher : matchers) {
            switch (matcher.type()) {
                case EQUALS:
                    queryBuilder.add(new TermQuery(new Term(matcher.labelName(), matcher.query())), BooleanClause.Occur.FILTER);
                    break;
                // TODO: implement
            }
        }
        // Placeholder, should create a query based on mint, maxt, and matchers

        // TODO: handle min_t, max_t filtering across query tree.
        return queryBuilder.build();
    }

    public static class LuceneDocPerChunkBlockSeriesEntry implements Series {

        private final Chunk chunk;
        private final Labels labels;

        public LuceneDocPerChunkBlockSeriesEntry(Chunk chunk, Labels labels) {
            this.chunk = chunk;
            this.labels = labels;
        }

        @Override
        public Labels getLabels() {
            return labels;
        }

        @Override
        public ChunkIterator iterator() {
            return chunk.iterator(null);
        }
    }

    /**
     * maps to prometheus `blockSeriesSet`
     */
    public static class LuceneDocPerChunkBlockSeriesSet implements SeriesSet {


        private final long minTime;
        private final long maxTime;
        private final Query query;
        private final IndexReader indexReader;
        private final IndexSearcher searcher;

        private Weight weight;
        private List<LeafReaderContext> leaves;
        private int leafIndex = 0;
        private DocIdSetIterator docIdIterator;
        private BinaryDocValues currentDocValues;
        private BytesRef nextValue;

        public LuceneDocPerChunkBlockSeriesSet(IndexReader indexReader, Query query, long minTime, long maxTime) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.indexReader = indexReader;
            this.query = filterTimeRangeQuery(query, minTime, maxTime);
            this.searcher = new IndexSearcher(indexReader);
            this.leaves = indexReader.leaves();
        }

        private Query filterTimeRangeQuery(Query baseQuery, long minTime, long maxTime) {
            BooleanQuery.Builder timeRangeQuery = new BooleanQuery.Builder();
            timeRangeQuery.add(LongPoint.newRangeQuery(mintFieldName, Long.MIN_VALUE, maxTime), BooleanClause.Occur.FILTER);
            timeRangeQuery.add(LongPoint.newRangeQuery(maxtFieldName, minTime, Long.MAX_VALUE), BooleanClause.Occur.FILTER);
            timeRangeQuery.add(baseQuery, BooleanClause.Occur.FILTER);
            return timeRangeQuery.build();
        }

        @Override
        public Series next() {
            // TODO: query lucene index and return Series iterator (Iterator<Iterator<ChunkDoc>>)
            // hasNext() finds and caches the value. If it returns false,
            // it means there's nothing left to return.
            if (!hasNext()) {
                throw new NoSuchElementException("iterator exhausted, no more series available");
            }

            BytesRef result = this.nextValue;
            // Clear the cache so the next call to hasNext() will find the next value.
            this.nextValue = null;

            // TODO: this is already deep copied in hasNext(), so maybe we don't need to do it again here.
            Chunk chunk = chunkFromBytesRef(result);
            Labels labels = new Labels(Map.of()); // TODO: extract labels from the current document context
            return new LuceneDocPerChunkBlockSeriesEntry(chunk, labels);
        }

        @Override
        public boolean hasNext() {
            // If we've already found and cached the next value, we're good.
            if (nextValue != null) {
                return true;
            }
            if (weight == null) {
                try {
                    this.weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                while (true) {
                    // If the current docIdIterator is null or exhausted,
                    // we need to try to load the next one from the next segment.
                    if (docIdIterator == null) {
                        if (leafIndex >= leaves.size()) {
                            // No more leaves, so no more documents.
                            return false;
                        }
                        // Attempt to prepare the scorer and doc values for the next segment.
                        setupNextLeaf();
                        continue; // Loop again to start processing the new leaf.
                    }

                    // Advance the iterator to the next matching document in this segment.
                    int docId = docIdIterator.nextDoc();

                    // TODO: check docId not deleted in live docs?

                    if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                        // This segment is exhausted. Nullify the iterator
                        // so the next loop iteration will advance to the next leaf.
                        docIdIterator = null;
                        continue;
                    }

                    // We have a matching docId, now try to get its doc value.
                    // Not all matching documents might have a value for the target field.
                    if (currentDocValues != null && currentDocValues.advanceExact(docId)) {
                        // Success! We found a value. Cache it and return.
                        // A deep copy is essential as the underlying BytesRef is reused by Lucene.
                        this.nextValue = BytesRef.deepCopyOf(currentDocValues.binaryValue());
                        return true;
                    }
                    // This document matched the query but didn't have a value in the 'chunks' field.
                    // Continue the loop to find the next one.
                }
            } catch (IOException e) {
                // Iterators don't throw checked exceptions, so we wrap it.
                throw new RuntimeException(e);
            }
        }

        /**
         * Sets up the iterators for the next leaf (segment).
         */
        private void setupNextLeaf() throws IOException {
            LeafReaderContext leaf = leaves.get(leafIndex);
            leafIndex++;

            // Get a Scorer for this specific leaf from the pre-compiled Weight.
            // The Scorer contains the iterator over matching documents for this segment.
            Scorer scorer = weight.scorer(leaf);
            if (scorer != null) {
                docIdIterator = scorer.iterator();
                // Also get the doc values for this leaf.
                currentDocValues = leaf.reader().getBinaryDocValues(CHUNK_FIELD_NAME);
            } else {
                // This segment has no documents matching the query.
                docIdIterator = null;
                currentDocValues = null;
            }
        }
    }

    /**
     * ChunkReader reads chunks from the Lucene index based.
     * It uses BinaryDocValues to read the chunk data stored in the index.
     *
     * Used by `ChunkQuerier` and {@link org.opensearch.ts.compactor.Compactor}.
     */
    public static class ChunkReader implements org.opensearch.ts.chunks.ChunkReader {

        private final IndexReader indexReader;

        private BinaryDocValues chunkDVs;

        public ChunkReader(IndexReader reader) {
            this.indexReader = reader;
        }

        @Override
        public Chunk readChunk(Meta meta) throws IOException {
            // TODO: the BlockSeriesSet (or caller) should be handling the query and passing the doc IDs to this reader.

            // TODO: HUGE HACK, this is **NOT** the correct doc id, this is just a placeholder to get the code to compile.
            //  this needs to somehow hook into the query layer to get the correct doc id iterator for the specific query,
            //  and the doc IDs need to somehow be passed in via the `Meta` object.
            //  For a full block scanner (e.g. for compaction), the meta.getChunkRef() maybe should be the doc ID
            //  generated from a DocIdSetIterator or similar, which is then used to read the chunk data.
            //  If we have multiple segments per index, we may need to figure out how to handle sorting across segments
            //  and merging the chunks so it iterates within a series correctly.
            int docId = (int) meta.getChunkRef();

            // TODO: based on java docs, this is **NOT** performant, and we should use the LeafReader ourselves directly.
            //  Evaluate whether that's true. I'm also not sure if this handles deletes/liveDocs.
            if (chunkDVs == null) {
                chunkDVs = MultiDocValues.getBinaryValues(indexReader, CHUNK_FIELD_NAME);
            }

            boolean found = chunkDVs.advanceExact(docId);
            if (!found) {
                return null;
            }

            // Careful: the binaryValue() returns a BytesRef that is reused by the underlying iterator, we should deep copy the data.
            BytesRef dvBytesRef = chunkDVs.binaryValue();

            return chunkFromBytesRef(dvBytesRef);
        }

        @Override
        public void close() {
        }
    }

    private static Chunk chunkFromBytesRef(BytesRef dvBytesRef) {
        // TODO: extract to common utility class for both encoding and decoding Lucene DV chunks
        ByteBuffer byteBuffer = ByteBuffer.wrap(dvBytesRef.bytes);
        int encodingEnumValue = byteBuffer.getInt();
        Encoding encoding = Encoding.values()[encodingEnumValue];
        int chunkBytesLen = byteBuffer.getInt();
        byte[] chunkBytes = new byte[chunkBytesLen];
        // TODO: maybe this deep copy is not required, if we can reuse the byte buffer, we decrease the allocations.
        //  But we need to be careful, because the DV byteRef is reused by the underlying iterator.
        byteBuffer.get(chunkBytes);

        return switch (encoding) {
            case Encoding.RAW -> new ImmutableRawChunk(chunkBytes);
            case Encoding.XOR -> new XORChunk(chunkBytes);
            case Encoding.CHIMP -> new ChimpChunk(chunkBytes);
        };
    }
}
