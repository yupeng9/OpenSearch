/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * Appender to a chunk
 */
public interface ChunkAppender {
    /**
     * Append a sample to the appender.
     *
     * @param timestamp the timestamp
     * @param value the value
     */
    void append(long timestamp, double value);

    // TODO: add histogram support
}
