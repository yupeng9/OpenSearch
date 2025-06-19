/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import java.io.IOException;

public interface ChunkReader {
    Chunk readChunk(Meta meta) throws IOException;

    void close();
}
