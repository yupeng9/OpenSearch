/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.utils;

import java.util.Arrays;

public class BitStream {
    // TODO: support buffer resizing
    private byte[] buffer;
    private int bytePos;
    private int bitPos; // 0–7
    private boolean immutable; // TODO add iface and split the class

    private static final int DEFAULT_CAPACITY = 64; // Prometheus uses 128 bytes as default capacity, should we use that?

    public BitStream() {
        this.buffer = new byte[DEFAULT_CAPACITY];
        this.bytePos = 0;
        this.bitPos = 0;
    }

    public BitStream(byte[] bytes) {
        this.buffer = bytes;
        this.bytePos = bytes.length;
        this.immutable = true;
    }

    // Write a single bit (0 or 1)
    public void writeBit(int bit) {
        ensureCapacity(1);
        if (bit != 0) {
            buffer[bytePos] |= (1 << (7 - bitPos));
        }
        bitPos++;
        if (bitPos == 8) {
            bitPos = 0;
            bytePos++;
        }
    }

    public void writeBits(long value, int numBits) {
        ensureCapacity((numBits + 7) / 8);

        // Shift value to align with most significant bits
        value <<= (64 - numBits);
        
        // Write whole bytes first
        while (numBits >= 8 && bitPos == 0) {
            byte byteValue = (byte) (value >>> 56);
            buffer[bytePos] = byteValue;
            bytePos++;
            value <<= 8;
            numBits -= 8;
        }
        
        // Write remaining bits individually
        while (numBits > 0) {
            int bit = ((value >>> 63) == 1) ? 1 : 0;
            buffer[bytePos] |= (bit << (7 - bitPos));
            bitPos++;
            if (bitPos == 8) {
                bitPos = 0;
                bytePos++;
            }
            value <<= 1;
            numBits--;
        }
    }

    public int size() {
        return bytePos + (bitPos > 0 ? 1 : 0);
    }

    public void writeByte(byte value) {
        writeBits(value & 0xFF, 8);
    }

    public void writeLong(long value) {
        for (int i = 7; i >= 0; i--) {
            writeByte((byte) (value >> (i * 8)));
        }
    }

    public void writeDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
    }

    // Write variable-length signed integer
    public void writeVarint(long value) {
        // Convert signed to unsigned
        long uvalue = (value << 1) ^ (value >> 63);
        writeUvarint(uvalue);
    }

    // Write variable-length unsigned integer
    public void writeUvarint(long value) {
        while (value >= 0x80) {
            writeByte((byte) (value | 0x80));
            value >>>= 7;
        }
        writeByte((byte) value);
    }

    public byte[] toByteArray() {
        if (immutable) {
            return buffer;
        }
        int totalBytes = bytePos + (bitPos > 0 ? 1 : 0);
        return Arrays.copyOf(buffer, totalBytes);
    }

    public void resetStream() {
        Arrays.fill(buffer, (byte) 0);
        bytePos = 0;
        bitPos = 0;
    }

    private void ensureCapacity(int additionalBytes) {
        int required = bytePos + additionalBytes + 1;
        if (required > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(required, buffer.length * 2));
        }
    }

    // Update methods for modifying existing data
    public void updateBytesAt(int offset, byte[] newBytes) {
        if (offset < 0 || offset + newBytes.length > buffer.length) {
            throw new IndexOutOfBoundsException("Update would exceed buffer bounds");
        }
        System.arraycopy(newBytes, 0, buffer, offset, newBytes.length);
    }

    public void updateByteAt(int offset, byte newByte) {
        if (offset < 0 || offset >= buffer.length) {
            throw new IndexOutOfBoundsException("Offset out of bounds: " + offset);
        }
        buffer[offset] = newByte;
    }

    public void updateShortAt(int offset, short value) {
        if (offset < 0 || offset + 1 >= buffer.length) {
            throw new IndexOutOfBoundsException("Short update would exceed buffer bounds");
        }
        buffer[offset] = (byte) (value >>> 8);     // High byte
        buffer[offset + 1] = (byte) (value & 0xFF); // Low byte
    }

}
