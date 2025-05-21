/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * A XOR appender that implements delta-of-delta encoding in Facebook's Gorilla paper:
 * https://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
 */
public class XORAppender implements ChunkAppender {
    @Override
    public void append(long timestamp, double value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
