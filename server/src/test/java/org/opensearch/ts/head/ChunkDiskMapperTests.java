/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.ChunkAppender;
import org.opensearch.ts.chunks.MutableRawChunk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opensearch.ts.head.ChunkDiskMapper.*;

public class ChunkDiskMapperTests extends OpenSearchTestCase {

    public void testWriteReadChunk() {
        MemChunk chunk = getMemChunk(1);

        ChunkDiskMapper chunkDiskMapper = new ChunkDiskMapper(createTempDir("testWriteReadChunk"));
        ChunkDiskMapper.ChunkRef chunkRef = chunkDiskMapper.writeChunk(0, chunk);

        Chunk readChunk = chunkDiskMapper.chunkFor(chunkRef);
        assertEquals(chunk.getChunk().numSamples(), readChunk.numSamples());
        assertEquals(chunk.getChunk().encoding(), readChunk.encoding());

        byte[] expectedBytes = chunk.getChunk().bytes();
        byte[] readBytes = readChunk.bytes();
        for (int i = 0; i < expectedBytes.length; i++) {
            assertEquals(expectedBytes[i], readBytes[i]);
        }
    }

    public void testTruncate() {
        Path dir = createTempDir("testTruncate");
        ChunkDiskMapper chunkDiskMapper = new ChunkDiskMapper(dir, 1024); // 1KB per chunkfile

        // Write 3 chunks, each with 25 samples (400 bytes + header) to create two chunk files
        for (int i = 0; i < 3; i++) {
            MemChunk chunk = getMemChunk(25);
            chunkDiskMapper.writeChunk(i, chunk);
        }

        assertEquals(2, chunkDiskMapper.getNumFiles());
        assertEquals(List.of("chunk_0", "chunk_1"), getChunkFileNamesIn(dir));

        chunkDiskMapper.truncate(1);

        assertEquals(1, chunkDiskMapper.getNumFiles());
        assertEquals(List.of("chunk_1"), getChunkFileNamesIn(dir));
    }

    public void testClose() throws IOException {
        Path dir = createTempDir("testTruncate");
        ChunkDiskMapper chunkDiskMapper = new ChunkDiskMapper(dir, 1024); // 1KB per chunkfile
        MemChunk chunk = getMemChunk(50); // 800 bytes + header
        chunkDiskMapper.writeChunk(0, chunk);
        chunkDiskMapper.close();

        assertEquals(1, chunkDiskMapper.getNumFiles());
        assertEquals(List.of("chunk_0"), getChunkFileNamesIn(dir));

        File file = dir.resolve("chunk_0").toFile();
        assertEquals(800 + CHUNK_META_SIZE + CHUNK_FILE_HEADER_SIZE, file.length());
    }

    public void testLoadExistingDirectory() throws IOException {
        Path dir = createTempDir("testLoadExistingDirectory");
        ChunkDiskMapper chunkDiskMapper = new ChunkDiskMapper(dir, 1024); // 1KB per chunkfile

        // Write 3 chunks, to create two chunk files
        List<ChunkDiskMapper.ChunkRef> chunkRefs = new ArrayList<>();
        List<byte[]> chunkBytes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MemChunk chunk = getMemChunk(25 + i);
            chunkRefs.add(chunkDiskMapper.writeChunk(i, chunk));
            chunkBytes.add(chunk.getChunk().bytes());
        }
        chunkDiskMapper.close();

        assertEquals(2, chunkDiskMapper.getNumFiles());
        assertEquals(List.of("chunk_0", "chunk_1"), getChunkFileNamesIn(dir));

        // Recreate ChunkDiskMapper to load existing directory (additional chunk file is created)
        chunkDiskMapper = new ChunkDiskMapper(dir, 1024);
        assertEquals(3, chunkDiskMapper.getNumFiles());

        // Read using the chunk references, which should not change
        for (int i = 0; i < chunkRefs.size(); i++) {
            Chunk readChunk = chunkDiskMapper.chunkFor(chunkRefs.get(i));
            assertEquals(25 + i, readChunk.numSamples());
        }

        // Write another chunk, and verify all chunks readable (no overwrite)
        MemChunk chunk = getMemChunk(28);
        chunkBytes.add(chunk.getChunk().bytes());
        chunkRefs.add(chunkDiskMapper.writeChunk(4, chunk));
        for (int i = 0; i < chunkRefs.size(); i++) {
            Chunk readChunk = chunkDiskMapper.chunkFor(chunkRefs.get(i));
            assertEquals(25 + i, readChunk.numSamples());
            assertArrayEquals(chunkBytes.get(i), readChunk.bytes());
        }
    }

    public void testChunkIterator() {
        Path dir = createTempDir("testChunkIterator");
        ChunkDiskMapper chunkDiskMapper = new ChunkDiskMapper(dir, 1024); // 1KB per chunkfile

        // Write 3 chunks, to create two chunk files
        List<ChunkDiskMapper.ChunkRef> chunkRefs = new ArrayList<>();
        List<byte[]> chunkBytes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MemChunk chunk = getMemChunk(25 + i);
            chunkRefs.add(chunkDiskMapper.writeChunk(i, chunk));
            chunkBytes.add(chunk.getChunk().bytes());
        }

        int count = 0;

        List<ChunkDiskMapper.ChunkAndSeriesRef> l = new ArrayList<>();
        Iterator<ChunkDiskMapper.ChunkAndSeriesRef> it = chunkDiskMapper.chunkIterator();
        while (it.hasNext()) {
            l.add(it.next());
            count++;
        }

        assertEquals(3, count);

    }

    // Helper to create a MemChunk with a raw chunk for testing
    private MemChunk getMemChunk(int numSamples) {
        MemChunk chunk = new MemChunk(0, numSamples * 10L, null);
        Chunk rawChunk = new MutableRawChunk();
        ChunkAppender appender = rawChunk.appender();
        for (int i = 0; i < numSamples; i++) {
            appender.append(i, i);
        }
        chunk.setChunk(rawChunk);
        return chunk;
    }

    // Helper to return chunk files names in the given directory.
    private List<String> getChunkFileNamesIn(Path dir) {
        // Operating systems may create extra files, and createTempDir utility method simulates the behavior. Filter extra files out.
        return Arrays.stream(Objects.requireNonNull(dir.toFile().list()))
            .filter(name -> name.startsWith(CHUNK_FILE_PREFIX))
            .collect(Collectors.toList());
    }
}
