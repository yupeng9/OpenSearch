/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head.index.chunk;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.ImmutableRawChunk;
import org.opensearch.ts.head.ClosedChunk;
import org.opensearch.ts.head.MemChunk;

import java.io.IOException;
import java.util.Base64;

import static org.opensearch.ts.head.MemChunk.UUID_BYTES_LENGTH;

public class ClosedChunkIndexUtils {

    public static BytesRef getSerializedMemChunk(MemChunk memChunk) {
        Chunk chunk = memChunk.getChunk();
        int metaSize = 5 + 9 * 3; // upper bound of vint/long encoding bytes usage for [encoding, minTimestamp, maxTimestamp]
        byte[] uuidBytes = memChunk.getChunkUuid();
        byte[] serializedBytes = new byte[chunk.bytesSize() + uuidBytes.length + metaSize];
        ByteArrayDataOutput out = new ByteArrayDataOutput(serializedBytes);
        try {
            out.writeVInt(chunk.encoding().ordinal());
            out.writeVLong(memChunk.getMinTimestamp());
            out.writeVLong(memChunk.getMaxTimestamp());
            out.writeBytes(uuidBytes, uuidBytes.length);
            out.writeBytes(chunk.bytes(), chunk.bytesSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new BytesRef(serializedBytes, 0, out.getPosition());
    }

    public static ClosedChunk getClosedChunkFromSerialized(BytesRef serialized) {
        ByteArrayDataInput in = new ByteArrayDataInput(serialized.bytes, serialized.offset, serialized.length);
        int length = serialized.length - serialized.offset;
        Encoding encoding = Encoding.values()[in.readVInt()];
        long minTimestamp = in.readVLong();
        long maxTimestamp = in.readVLong();

        byte[] uuid = new byte[UUID_BYTES_LENGTH];
        in.readBytes(uuid, 0, UUID_BYTES_LENGTH);
        byte[] chunkBytes = new byte[length - in.getPosition()];
        in.readBytes(chunkBytes, 0, chunkBytes.length);

        Chunk chunk = switch (encoding) {
            case RAW -> new ImmutableRawChunk(chunkBytes);
            case XOR -> throw new UnsupportedOperationException("XOR encoding not yet supported");
        };
        return new ClosedChunk(minTimestamp, maxTimestamp, chunkBytes, chunk.encoding(), uuid);
    }
}
