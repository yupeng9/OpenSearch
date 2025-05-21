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

    private static final int DEFAULT_CAPACITY = 64;

    public BitStream() {
        this.buffer = new byte[DEFAULT_CAPACITY];
        this.bytePos = 0;
        this.bitPos = 0;
    }

    // Writes 'numBits' (<=32) from 'value' into the stream
    public void writeBits(int value, int numBits) {
        ensureCapacity((numBits + 7) / 8);

        for (int i = numBits - 1; i >= 0; i--) {
            int bit = (value >> i) & 1;
            buffer[bytePos] |= (bit << (7 - bitPos));
            bitPos++;
            if (bitPos == 8) {
                bitPos = 0;
                bytePos++;
            }
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

    public byte[] toByteArray() {
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

}
