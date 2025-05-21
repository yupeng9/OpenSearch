/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * A chunk of time series sample.
 */
public interface Chunk {
    /**
     * @return the underlying bytes of the chunk
     */
    byte[] bytes();

    Encoding encoding();

    ChunkAppender appender();

    int numSamples();

    void compact();

}
