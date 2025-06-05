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
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class HeadTests extends OpenSearchTestCase {

    public void testHeadAppendAndMMap() throws IOException {
        Head head = new Head(createTempDir("testHeadLifecycle"));
        HeadAppender.CommitContext context = new HeadAppender.CommitContext(new ChunkOptions(1000, 10));
        Labels seriesLabels = Labels.fromStrings("k1", "v1", "k2", "v2");

        List<Long> expectedTimestamps = new ArrayList<>();
        List<Double> expectedValues = new ArrayList<>();

        // three batches create three chunks, with [8, 8, 2] samples respectively
        int sample = 0;
        for (int batch = 0; batch < 3; batch++) {
            HeadAppender appender = head.newAppender();
            for (int i = 0; i < 6; i++) {
                expectedTimestamps.add((long) sample);
                expectedValues.add((double) i);
                appender.append(0, seriesLabels, sample++, i);

            }
            appender.commitSamples(context);
        }

        head.mmapHeadChunks();

        MemSeries series = head.getStripeSeries().getByHash(seriesLabels.hashCode(), seriesLabels);
        Chunk firstChunk = head.chunkFromSeries(series, 0, 0, 7); // mmapped
        Chunk secondChunk = head.chunkFromSeries(series, 1, 8, 15); // mmapped
        Chunk thirdChunk = head.chunkFromSeries(series, 2, 16, 17); // in memory

        assertEquals(firstChunk.numSamples(), 8);
        assertEquals(secondChunk.numSamples(), 8);
        assertEquals(thirdChunk.numSamples(), 2);

        List<Long> actualTimestamps = new ArrayList<>();
        List<Double> actualValues = new ArrayList<>();
        append(firstChunk, actualTimestamps, actualValues);
        append(secondChunk, actualTimestamps, actualValues);
        append(thirdChunk, actualTimestamps, actualValues);

        assertEquals(expectedTimestamps, actualTimestamps);
        assertEquals(expectedValues, actualValues);

        head.close();
    }

    public void testLoadSnapshot() throws IOException {
        Path testDir = createTempDir("testLoadSnapshot");
        Head head = new Head(testDir);
        head.initTime(0);
        HeadAppender.CommitContext context = new HeadAppender.CommitContext(new ChunkOptions(1000, 10));
        Labels seriesLabels = Labels.fromStrings("k1", "v1", "k2", "v2");

        List<Long> expectedTimestamps = new ArrayList<>();
        List<Double> expectedValues = new ArrayList<>();

        // three batches create three chunks, with [8, 8, 2] samples respectively
        HeadAppender appender = head.newAppender();
        for (int i = 0; i < 16; i++) {
            expectedTimestamps.add((long) i);
            expectedValues.add((double) i);
            appender.append(0, seriesLabels, i, i);
        }
        appender.commitSamples(context);

        head.mmapHeadChunks();
        head.close();
        head = new Head(testDir);

        MemSeries series = head.getStripeSeries().getByHash(seriesLabels.hashCode(), seriesLabels);
        Chunk firstChunk = head.chunkFromSeries(series, 0, 0, 7); // mmapped

        assertEquals(firstChunk.numSamples(), 8);

        List<Long> actualTimestamps = new ArrayList<>();
        List<Double> actualValues = new ArrayList<>();
        append(firstChunk, actualTimestamps, actualValues);

        // TODO: only mmapped chunks are currently persisted in snapshot
        assertEquals(expectedTimestamps.subList(0, 8), actualTimestamps);
        assertEquals(expectedValues.subList(0, 8), actualValues);
    }

    // helper appends timestamps and values from a chunk to the provided lists
    private void append(Chunk chunk, List<Long> timestamps, List<Double> values) {
        // raw chunk bytes format is [timestamp, value, timestamp, value], stored as 8 byte longs
        ByteBuffer bytes = ByteBuffer.wrap(chunk.bytes()).asReadOnlyBuffer();

        for (int i = 0; i < chunk.numSamples(); i++) {
            timestamps.add(bytes.getLong());
            values.add(Double.longBitsToDouble(bytes.getLong()));
        }
    }
}
