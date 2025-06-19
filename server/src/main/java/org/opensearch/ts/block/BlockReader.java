/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.block;

import org.opensearch.ts.chunks.ChunkReader;

import java.io.IOException;

public interface BlockReader {
    ChunkReader chunks() throws IOException;
}
