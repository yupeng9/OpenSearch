/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.closed;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.MutableRawChunk;
import org.opensearch.ts.head.ClosedChunk;
import org.opensearch.ts.head.MemChunk;
import org.opensearch.ts.head.index.chunk.ClosedChunkIndexUtils;

public class ClosedChunkIndexUtilsTests extends OpenSearchTestCase {

    public void testChunkSerDeser() {
        MemChunk memChunk = new MemChunk(0, 100, null);
        Chunk chunk = new MutableRawChunk();
        memChunk.setChunk(chunk);
        ChunkAppender appender = chunk.appender();
        for (int i = 0; i < 10; i++) {
            appender.append(i, i * 10);
        }

        BytesRef ref = ClosedChunkIndexUtils.getSerializedMemChunk(memChunk);
        ClosedChunk deserializedChunk = ClosedChunkIndexUtils.getClosedChunkFromSerialized(ref);

        assertEquals(memChunk.getMinTimestamp(), deserializedChunk.getMinTimestamp());
        assertEquals(memChunk.getMaxTimestamp(), deserializedChunk.getMaxTimestamp());
        assertEquals(memChunk.getChunk().encoding(), deserializedChunk.getChunk().encoding());
        assertArrayEquals(memChunk.getChunk().bytes(), deserializedChunk.getChunk().bytes());
        assertArrayEquals(memChunk.getChunkUuid(), deserializedChunk.getChunkUuid());
    }
}
