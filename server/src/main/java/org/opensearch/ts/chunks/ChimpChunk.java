/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

public class ChimpChunk extends CompressionChunk {

    public ChimpChunk() {
        super();
    }

    public ChimpChunk(byte[] bytes) {
        super(bytes);
    }

    @Override
    public Encoding encoding() {
        return Encoding.CHIMP;
    }

    @Override
    public ChunkAppender appender() {
        ChimpIterator it = new ChimpIterator(bytes());

        // To get an appender we must know the state it would have if we had
        // appended all existing data from scratch.
        // We iterate through the end and populate via the iterator's state.
        while (it.next() != ChunkIterator.ValueType.NONE) {
        }
        if (it.error() != null) {
            throw new RuntimeException("Error reading existing chunk data", it.error());
        }

        ChimpAppender a = new ChimpAppender(
            this,
            it.currentTimestamp,
            it.currentValue,
            it.timeDelta,
            it.storedValues,
            it.currentIndex,
            it.storedValuesCount,
            it.indices,
            it.index,
            it.storedLeadingZeros
        );
        return a;
    }

    @Override
    public ChunkIterator iterator(ChunkIterator iterator) {
        if (iterator instanceof ChimpIterator) {
            ChimpIterator chimpIterator = (ChimpIterator) iterator;
            chimpIterator.reset(bytes());
            return chimpIterator;
        }
        return new ChimpIterator(bytes());
    }
} 