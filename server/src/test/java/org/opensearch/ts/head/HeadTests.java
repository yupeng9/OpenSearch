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
import org.opensearch.ts.chunks.ChunkIterator;
import org.opensearch.ts.chunks.ImmutableRawChunk;
import org.opensearch.ts.chunks.XORChunk;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HeadTests extends OpenSearchTestCase {

    public void testHeadLifecycle() throws IOException, InterruptedException {
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

        Thread.sleep(2000L); // allow the live index to refresh

        head.closeHeadChunks();

        MemSeries series = head.getStripeSeries().getByHash(seriesLabels.hashCode(), seriesLabels);

        Map<Integer, List<HeadChunk>> chunks = head.matchingChunks("/k1:v1/", 0, 20);
        assertEquals(1, chunks.size());
        List<HeadChunk> seriesChunks = chunks.get(series.getLabels().hashCode());
        assertEquals(3, seriesChunks.size());

        assertTrue(seriesChunks.get(0) instanceof ClosedChunk); // mmapped
        assertTrue(seriesChunks.get(1) instanceof ClosedChunk); // mmapped
        assertTrue(seriesChunks.get(2) instanceof MemChunk);    // heap

        Chunk firstChunk = seriesChunks.get(0).getChunk();
        Chunk secondChunk = seriesChunks.get(1).getChunk();
        Chunk thirdChunk = seriesChunks.get(2).getChunk();

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

    public void testHeadGC() throws IOException {
        Head head = new Head(createTempDir("testHeadGC"));
        HeadAppender.CommitContext context = new HeadAppender.CommitContext(new ChunkOptions(1000, 10));
        Labels seriesNoData = Labels.fromStrings("k1", "v1", "k2", "v2");
        Labels seriesWithData = Labels.fromStrings("k1", "v1", "k3", "v3");

        MemSeries emptySeries = head.createSeries(seriesNoData.hashCode(), seriesNoData, true, 0L);
        HeadAppender appender = head.newAppender();
        for (int i = 0; i < 8; i++) {
            appender.append(0, seriesWithData, i++, i);

        }
        appender.commitSamples(context);

        head.closeHeadChunks();
        // both series present after the first closeHeadChunks invocation
        assertNotNull(head.getStripeSeries().getByHash(seriesNoData.hashCode(), seriesNoData));
        assertNotNull(head.getStripeSeries().getByHash(seriesWithData.hashCode(), seriesWithData));

        head.closeHeadChunks();
        // empty series removed after the second closeHeadChunks invocation
        assertNull(head.getStripeSeries().getByHash(seriesNoData.hashCode(), seriesNoData));
        assertNotNull(head.getStripeSeries().getByHash(seriesWithData.hashCode(), seriesWithData));

        head.close();
    }

    // helper appends timestamps and values from a chunk to the provided lists
    private void append(Chunk chunk, List<Long> timestamps, List<Double> values) {
        switch (chunk.encoding()) {
            case RAW:
                appendRawChunk((ImmutableRawChunk) chunk, timestamps, values);
                break;
            case XOR:
                appendXORChunk((XORChunk) chunk, timestamps, values);
                break;
            default:
                throw new IllegalArgumentException("Unsupported chunk encoding: " + chunk.encoding());
        }
    }

    private void appendRawChunk(ImmutableRawChunk chunk, List<Long> timestamps, List<Double> values) {
        // raw chunk bytes format is [timestamp, value, timestamp, value], stored as 8 byte longs
        ByteBuffer bytes = ByteBuffer.wrap(chunk.bytes()).asReadOnlyBuffer();
        for (int i = 0; i < chunk.numSamples(); i++) {
            timestamps.add(bytes.getLong());
            values.add(Double.longBitsToDouble(bytes.getLong()));
        }
    }

    private void appendXORChunk(XORChunk chunk, List<Long> timestamps, List<Double> values) {
        ChunkIterator iterator = chunk.iterator(null);
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            timestamps.add(tv.timestamp());
            values.add(tv.value());
        }
    }
}
