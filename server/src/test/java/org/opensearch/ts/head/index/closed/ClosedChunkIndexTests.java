/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.closed;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.MutableRawChunk;
import org.opensearch.ts.head.HeadChunk;
import org.opensearch.ts.head.MemChunk;
import org.opensearch.ts.head.index.chunk.ClosedChunkIndex;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ClosedChunkIndexTests extends OpenSearchTestCase {

    public void testClosedChunkIndex() throws IOException, InterruptedException {
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(createTempDir("testClosedChunkIndex"));


        Labels labels1 = Labels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = Labels.fromStrings("k1", "v1", "k3", "v3");
        closedChunkIndex.addNewChunk(labels1, getMemChunk(5, 0, 90));
        closedChunkIndex.addNewChunk(labels2, getMemChunk(10, 0, 190));
        closedChunkIndex.addNewChunk(labels1, getMemChunk(15, 100, 190));
        closedChunkIndex.addNewChunk(labels2, getMemChunk(20, 200, 390));

        closedChunkIndex.refresh(); // ensure docs can be searched

        // search by labels, minTimestamp inclusive
        Map<Integer, List<HeadChunk>> chunks = closedChunkIndex.getChunks("/k1:v1/", 50, 90);
        assertEquals(2, chunks.size());
        // first chunk
        HeadChunk chunk = chunks.get(labels1.hashCode()).getFirst();
        assertEquals(0, chunk.getMinTimestamp());
        assertEquals(90, chunk.getMaxTimestamp());
        assertEquals(Encoding.RAW, chunk.getChunk().encoding());
        assertEquals(5, chunk.getChunk().numSamples());
        // second chunk
        chunk = chunks.get(labels2.hashCode()).getFirst();
        assertEquals(0, chunk.getMinTimestamp());
        assertEquals(190, chunk.getMaxTimestamp());
        assertEquals(Encoding.RAW, chunk.getChunk().encoding());
        assertEquals(10, chunk.getChunk().numSamples());

        // search by labels, maxTimestamp inclusive
        chunks = closedChunkIndex.getChunks("/k2:v2/", 100, 190);
        chunk = chunks.get(labels1.hashCode()).getFirst();
        assertEquals(1, chunks.size());
        // first chunk
        assertEquals(100, chunk.getMinTimestamp());
        assertEquals(190, chunk.getMaxTimestamp());
        assertEquals(Encoding.RAW, chunk.getChunk().encoding());
        assertEquals(15, chunk.getChunk().numSamples());

        closedChunkIndex.close();
    }

    private MemChunk getMemChunk(int numSamples, int minTimestamp, int maxTimestamp) {
        long interval = (maxTimestamp - minTimestamp) / numSamples;

        MemChunk chunk = new MemChunk(0, maxTimestamp, null);
        Chunk rawChunk = new MutableRawChunk();
        ChunkAppender appender = rawChunk.appender();
        for (int i = 0; i < numSamples; i++) {
            appender.append(i, i * interval);
        }
        chunk.setChunk(rawChunk);
        chunk.setMinTimestamp(minTimestamp);
        chunk.setMaxTimestamp(maxTimestamp);
        return chunk;
    }
}
