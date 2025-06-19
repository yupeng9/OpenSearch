/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.compactor;

import org.opensearch.ts.block.BlockReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Compactor provides compaction against an underlying storage of time series data.
 * This is also responsible for creating new blocks (not just for merging existing ones).
 */
public interface Compactor {
    // TODO: define block metadata/ULID type, extract to class
    record Metadata(String ulid) {}

    /**
     * Write persists one or more Blocks into a directory.
     * No Block is written when resulting Block has 0 samples and returns an empty slice.
     * We always return one or no block. The interface allows returning more than one
     * block for downstream users to experiment with compactor.
     *
     * @param destDir the destination directory path where the compacted data will be written
     * @param reader the BlockReader providing access to the input time series data
     * @param minTime the minimum timestamp for the data to be compacted
     * @param maxTime the maximum timestamp for the data to be compacted
     * @param base optional base metadata for compaction, can be null
     * @return Metadata containing information about compacted output block.
     */
    List<Metadata> Write(Path destDir, BlockReader reader, long minTime, long maxTime, Metadata base) throws IOException;
}
