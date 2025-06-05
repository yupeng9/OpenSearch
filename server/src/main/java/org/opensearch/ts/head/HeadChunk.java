/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;

/**
 * HeadChunk represents a chunk in the head block. It may be in-memory or mmapped
 */
public interface HeadChunk {
    long getMinTimestamp();

    long getMaxTimestamp();
}
