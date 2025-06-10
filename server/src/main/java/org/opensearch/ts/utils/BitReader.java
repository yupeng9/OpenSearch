/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.utils;

public class BitReader {
    private byte[] buffer;
    private int bytePos;
    private int bitPos; // 0–7

    public BitReader(byte[] bytes) {
        this.buffer = bytes;
        this.bytePos = 0;
        this.bitPos = 0;
    }

    public void reset(byte[] bytes) {
        this.buffer = bytes;
        this.bytePos = 0;
        this.bitPos = 0;
    }

    public int readBit() {
        if (bytePos >= buffer.length) {
            throw new IllegalStateException("End of stream reached");
        }
        
        int bit = (buffer[bytePos] >> (7 - bitPos)) & 1;
        bitPos++;
        if (bitPos == 8) {
            bitPos = 0;
            bytePos++;
        }
        return bit;
    }

    public long readBits(int numBits) {
        if (numBits > 64) {
            throw new IllegalArgumentException("Cannot read more than 64 bits");
        }
        
        long value = 0;
        for (int i = 0; i < numBits; i++) {
            value = (value << 1) | readBit();
        }
        return value;
    }

    public byte readByte() {
        return (byte) readBits(8);
    }

    public long readLong() {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (readByte() & 0xFF);
        }
        return value;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public boolean hasMore() {
        return bytePos < buffer.length || (bytePos == buffer.length && bitPos > 0);
    }

    // Read variable-length integer
    public long readVarint() {
        long value = 0;
        int shift = 0;
        
        while (true) {
            if (bytePos >= buffer.length) {
                throw new IllegalStateException("End of stream reached while reading varint");
            }
            
            byte b = (byte) readBits(8);
            if ((b & 0x80) == 0) {
                // Most significant bit is 0, this is the last byte
                if (shift == 63 && b > 1) {
                    throw new IllegalStateException("Varint overflow");
                }
                value |= ((long) b) << shift;
                break;
            }
            
            value |= ((long) (b & 0x7F)) << shift;
            shift += 7;
            
            if (shift >= 64) {
                throw new IllegalStateException("Varint overflow");
            }
        }
        
        // Convert unsigned to signed
        return (value >>> 1) ^ (-(value & 1));
    }

    // Read variable-length unsigned integer
    public long readUvarint() {
        long value = 0;
        int shift = 0;
        
        while (true) {
            if (bytePos >= buffer.length) {
                throw new IllegalStateException("End of stream reached while reading uvarint");
            }
            
            byte b = (byte) readBits(8);
            if ((b & 0x80) == 0) {
                // Most significant bit is 0, this is the last byte
                if (shift == 63 && b > 1) {
                    throw new IllegalStateException("Uvarint overflow");
                }
                value |= ((long) b) << shift;
                break;
            }
            
            value |= ((long) (b & 0x7F)) << shift;
            shift += 7;
            
            if (shift >= 64) {
                throw new IllegalStateException("Uvarint overflow");
            }
        }
        
        return value;
    }
} 