/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.utils;

/**
 * Shared timestamp encoding/decoding utilities for time series compression algorithms.
 * Used by both XOR (Gorilla) and CHIMP compression algorithms.
 * 
 * Implements delta-of-delta encoding where:
 * - First timestamp: stored directly as varint
 * - Second timestamp: stored as simple delta (uvarint) 
 * - Subsequent timestamps: stored as delta-of-delta with variable-length encoding
 */
public class TimestampEncoder {
    
    /**
     * Writes a timestamp delta-of-delta using variable-length encoding
     * @param bitStream the bit stream to write to
     * @param deltaOfDelta the delta-of-delta value to encode
     */
    public static void writeTimestampDelta(BitStream bitStream, long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            bitStream.writeBit(0);                    // 1 bit: no change
        } else if (bitRange(deltaOfDelta, 14)) {
            bitStream.writeBits(0b10L << 6 | (deltaOfDelta >> 8 & 0x3FL), 8);  // 2+6 bits
            bitStream.writeBits(deltaOfDelta & 0xFFL, 8);                      // +8 bits = 16 total
        } else if (bitRange(deltaOfDelta, 17)) {
            bitStream.writeBits(0b110, 3);            // 3 bits: header  
            bitStream.writeBits(deltaOfDelta, 17);    // 17 bits: delta = 20 total
        } else if (bitRange(deltaOfDelta, 20)) {
            bitStream.writeBits(0b1110, 4);           // 4 bits: header
            bitStream.writeBits(deltaOfDelta, 20);    // 20 bits: delta = 24 total
        } else {
            bitStream.writeBits(0b1111, 4);           // 4 bits: header
            bitStream.writeBits(deltaOfDelta, 64);    // 64 bits: full delta = 68 total
        }
    }
    
    /**
     * Reads a timestamp delta-of-delta from the bit stream
     * @param bitReader the bit reader to read from
     * @return the decoded delta-of-delta value
     */
    public static long readTimestampDelta(BitReader bitReader) {
        byte d = 0;
        // read delta-of-delta header
        for (int i = 0; i < 4; i++) {
            d <<= 1;
            int bit = bitReader.readBit();
            if (bit == 0) {
                break;
            }
            d |= 1;
        }
        
        byte sz = 0;
        long dod = 0;
        switch (d) {
            case 0b0:
                // dod == 0
                break;
            case 0b10:
                sz = 14;
                break;
            case 0b110:
                sz = 17;
                break;
            case 0b1110:
                sz = 20;
                break;
            case 0b1111:
                long bits = bitReader.readBits(64);
                dod = bits;
                break;
            default:
                throw new IllegalStateException("Invalid delta-of-delta header: " + d);
        }

        if (sz != 0) {
            long bits = bitReader.readBits(sz);
            
            // Account for negative numbers, which come back as high unsigned numbers.
            if (bits > (1L << (sz - 1))) {
                bits -= 1L << sz;
            }
            dod = bits;
        }

        return dod;
    }
    
    /**
     * Check if a value can be represented in the given number of bits (signed)
     * @param value the value to check
     * @param numBits number of bits available
     * @return true if the value fits in the specified number of bits
     */
    private static boolean bitRange(long value, int numBits) {
        long max = (1L << (numBits - 1)) - 1;
        long min = -(1L << (numBits - 1)) + 1;
        return value >= min && value <= max;
    }
} 