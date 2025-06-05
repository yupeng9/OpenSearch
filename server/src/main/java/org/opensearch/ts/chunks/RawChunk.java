/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

public interface RawChunk extends Chunk {
    // a sample is 8-byte timestamp + 8-byte value
    int SAMPLE_SIZE = 16;

    @Override
    default Encoding encoding() {
        return Encoding.RAW;
    }

    @Override
    default int numSamples() {
        return bytesSize() / SAMPLE_SIZE;
    }
}
