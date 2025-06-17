/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.opensearch.ts.chunks.Chunk;

import java.io.IOException;

/**
 * HeadChunk represents a chunk in the head block. It may be in-memory or mmapped
 */
public interface HeadChunk {

    Chunk getChunk();

    long getMinTimestamp();

    long getMaxTimestamp();

    byte[] getChunkUuid();
}
