/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitStream;

/**
 * ChimpAppender implements the CHIMP-128 compression algorithm for time series data.
 * CHIMP improves upon Gorilla's XOR compression by using a ring buffer of 128 previous values
 * and more efficient encoding for leading zeros.
 * paper: https://vldb.org/pvldb/vol15/p3058-liakos.pdf
 * github: https://github.com/panagiotisl/chimp
 */
public class ChimpAppender extends CompressionAppender {
    private static final int CHIMP_BUFFER_SIZE = 128;

    // Maps leading zero counts to 3-bit representations (0-7). 
    // Compresses 6-bit leading zero counts to 3 bits.
    public static final short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 2, 2, 2, 2,
        3, 3, 4, 4, 5, 5, 6, 6,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7
    };

    // Rounds leading zero counts to canonical values for grouping.
    // Groups: 0-7 exact, 8-11→8, 12-15→12, 16-17→16, 18-19→18, 20-21→20, 22-23→22, 24-63→24
    public static final short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
        8, 8, 8, 8, 12, 12, 12, 12,
        16, 16, 18, 18, 20, 20, 22, 22,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24
    };
    
    // Ring buffer to store previous values for CHIMP-128
    private long[] storedValues;
    private int currentIndex;
    private int storedValuesCount;
    
    // Hash-based indexing for fast lookup
    private int[] indices;
    private int index = 0;
    private int setLsb;
    private int threshold;
    private int previousValuesLog2;
    private int flagZeroSize;
    private int flagOneSize;
    private int storedLeadingZeros = Integer.MAX_VALUE;

    public ChimpAppender() {
        super();
        initializeChimpState();
    }

    public ChimpAppender(ChimpChunk chunk, long lastTimestamp, double lastValue, 
                        long timeDelta, long[] storedValues, int currentIndex, int storedValuesCount,
                        int[] indices, int index, int storedLeadingZeros) {
        super(chunk, lastTimestamp, lastValue, timeDelta);
        
        // Initialize basic state
        this.previousValuesLog2 = (int)(Math.log(CHIMP_BUFFER_SIZE) / Math.log(2));
        this.threshold = 6 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.flagZeroSize = previousValuesLog2 + 2;
        this.flagOneSize = previousValuesLog2 + 11;
        
        // Restore state
        this.storedValues = storedValues != null ? storedValues : new long[CHIMP_BUFFER_SIZE];
        this.currentIndex = currentIndex;
        this.storedValuesCount = storedValuesCount;
        this.indices = indices != null ? indices : new int[(int) Math.pow(2, threshold + 1)];
        this.index = index;
        this.storedLeadingZeros = storedLeadingZeros;
    }
    
    private void initializeChimpState() {
        // Initialize ring buffer for storing last 128 values
        this.storedValues = new long[CHIMP_BUFFER_SIZE];
        this.currentIndex = 0;  // Start at beginning of ring buffer
        this.storedValuesCount = 0;  // No values stored initially
        
        // Calculate parameters based on buffer size (128 = 2^7)
        this.previousValuesLog2 = (int)(Math.log(CHIMP_BUFFER_SIZE) / Math.log(2));  // log2(128) = 7
        this.threshold = 6 + previousValuesLog2;  // 6 + 7 = 13, minimum trailing zeros for hash reference
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;  // 2^14 - 1 = 16383, hash key mask for lower 14 bits
        
        // Initialize hash table for fast value lookup (2^14 = 16384 entries)
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        
        // Flag sizes for encoding different patterns
        this.flagZeroSize = previousValuesLog2 + 2;  // 7 + 2 = 9 bits for exact match encoding
        this.flagOneSize = previousValuesLog2 + 11;  // 7 + 11 = 18 bits for trailing zeros encoding
        
        // Initialize to invalid value to force new leading zero pattern on first use
        this.storedLeadingZeros = Integer.MAX_VALUE;
    }

    @Override
    protected void onFirstValue(double value) {
        long valueBits = Double.doubleToRawLongBits(value);
        addToBuffer(valueBits);
        indices[(int) valueBits & setLsb] = index;
    }

    @Override
    protected void writeValue(double value) {
        writeChimpValue(value);
    }
    
    private void addToBuffer(long value) {
        storedValues[currentIndex] = value;
        currentIndex = (currentIndex + 1) % CHIMP_BUFFER_SIZE;
        if (storedValuesCount < CHIMP_BUFFER_SIZE) {
            storedValuesCount++;
        }
    }
    
    private void writeChimpValue(double value) {
        long valueBits = Double.doubleToRawLongBits(value);
        
        // Find the best reference value and calculate XOR
        ReferenceResult ref = findBestReference(valueBits);
        
        // Encode based on XOR result
        if (ref.xor == 0) {
            encodeExactMatch(ref.previousIndex);
        } else {
            encodeNonZeroXor(ref.xor, ref.previousIndex, ref.trailingZeros);
        }
        
        // Update algorithm state
        updateState(valueBits);
    }
    
    private static class ReferenceResult {
        final int previousIndex;
        final long xor;
        final int trailingZeros;
        
        ReferenceResult(int previousIndex, long xor, int trailingZeros) {
            this.previousIndex = previousIndex;
            this.xor = xor;
            this.trailingZeros = trailingZeros;
        }
    }
    
    private ReferenceResult findBestReference(long valueBits) {
        int key = (int) valueBits & setLsb;
        int currIndex = indices[key];
        
        // Try hash-based lookup first
        if ((index - currIndex) < CHIMP_BUFFER_SIZE && currIndex > 0) {
            long tempXor = valueBits ^ storedValues[currIndex % CHIMP_BUFFER_SIZE];
            int trailingZeros = Long.numberOfTrailingZeros(tempXor);
            
            if (trailingZeros > threshold) {
                // Hash lookup gives good compression
                return new ReferenceResult(currIndex % CHIMP_BUFFER_SIZE, tempXor, trailingZeros);
            }
        }
        
        // Fall back to previous value
        int previousIndex = index % CHIMP_BUFFER_SIZE;
        long xor = storedValues[previousIndex] ^ valueBits;
        return new ReferenceResult(previousIndex, xor, 0);
    }
    
    private void encodeExactMatch(int previousIndex) {
        chunk.getBitStream().writeBits(previousIndex, flagZeroSize);
        storedLeadingZeros = 65;  // Reset to invalid value (>63) to force new leading zero pattern
    }
    
    private void encodeNonZeroXor(long xor, int previousIndex, int trailingZeros) {
        int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];
        
        if (trailingZeros > threshold) {
            encodeWithTrailingZeros(xor, previousIndex, leadingZeros, trailingZeros);
        } else if (leadingZeros == storedLeadingZeros) {
            encodeWithReusedLeadingZeros(xor, leadingZeros);
        } else {
            encodeWithNewLeadingZeros(xor, leadingZeros);
        }
    }
    
    private void encodeWithTrailingZeros(long xor, int previousIndex, int leadingZeros, int trailingZeros) {
        BitStream bitStream = chunk.getBitStream();
        int significantBits = 64 - leadingZeros - trailingZeros;
        
        // Pack three values into flagOneSize bits (18 bits total):
        // Bits 0-5:   significantBits (6 bits, 0-63)
        // Bits 6-8:   leadingRepresentation (3 bits, 0-7)  
        // Bits 9-17:  (CHIMP_BUFFER_SIZE + previousIndex) (9 bits)
        int flagValue = significantBits |
                       (leadingRepresentation[leadingZeros] << 6) |
                       ((CHIMP_BUFFER_SIZE + previousIndex) << 9);
        
        bitStream.writeBits(flagValue, flagOneSize);
        bitStream.writeBits(xor >>> trailingZeros, significantBits);
        storedLeadingZeros = 65;  // Reset to invalid value (>63) to force new leading zero pattern
    }
    
    private void encodeWithReusedLeadingZeros(long xor, int leadingZeros) {
        BitStream bitStream = chunk.getBitStream();
        bitStream.writeBits(2, 2);
        int significantBits = 64 - leadingZeros;
        bitStream.writeBits(xor, significantBits);
    }
    
    private void encodeWithNewLeadingZeros(long xor, int leadingZeros) {
        BitStream bitStream = chunk.getBitStream();
        storedLeadingZeros = leadingZeros;
        int significantBits = 64 - leadingZeros;
        bitStream.writeBits(24 + leadingRepresentation[leadingZeros], 5);
        bitStream.writeBits(xor, significantBits);
    }
    
    private void updateState(long valueBits) {
        // Update ring buffer
        currentIndex = (currentIndex + 1) % CHIMP_BUFFER_SIZE;
        if (storedValuesCount < CHIMP_BUFFER_SIZE) {
            storedValuesCount++;
        }
        storedValues[currentIndex] = valueBits;
        
        // Update hash table and global index
        index++;
        int key = (int) valueBits & setLsb;
        indices[key] = index;
    }
} 