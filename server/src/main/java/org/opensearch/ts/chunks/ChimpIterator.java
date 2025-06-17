/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * ChimpIterator implements decompression for CHIMP-128 compressed time series data.
 * It reverses the operations performed by ChimpAppender to restore the original values.
 */
public class ChimpIterator extends CompressionIterator {
    private static final int CHIMP_BUFFER_SIZE = 128;
    
    // Ring buffer to store previous values for CHIMP-128
    long[] storedValues;
    int currentIndex;
    protected int storedValuesCount;
    
    // Hash-based indexing for fast lookup
    protected int[] indices;
    protected int index = 0;
    private int setLsb;
    private int threshold;
    private int previousValuesLog2;
    private int flagZeroSize;
    private int flagOneSize;
    protected int storedLeadingZeros = Integer.MAX_VALUE;

    public ChimpIterator() {
        super();
        initializeChimpState();
    }

    public ChimpIterator(byte[] data) {
        super(data);
    }
    
    private void initializeChimpState() {
        this.storedValues = new long[CHIMP_BUFFER_SIZE];
        this.currentIndex = 0;
        this.storedValuesCount = 0;
        this.previousValuesLog2 = (int)(Math.log(CHIMP_BUFFER_SIZE) / Math.log(2));
        this.threshold = 6 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        this.flagZeroSize = previousValuesLog2 + 2;
        this.flagOneSize = previousValuesLog2 + 11;
        this.storedLeadingZeros = Integer.MAX_VALUE;
    }

    @Override
    protected void resetAlgorithmState() {
        initializeChimpState();
    }

    @Override
    protected void onFirstValueRead(double value) {
        long valueBits = Double.doubleToRawLongBits(value);
        addToBuffer(valueBits);
        indices[(int) valueBits & setLsb] = index;
    }

    @Override
    protected void readValue() {
        readChimpValue();
    }
    
    private void addToBuffer(long value) {
        storedValues[currentIndex] = value;
        currentIndex = (currentIndex + 1) % CHIMP_BUFFER_SIZE;
        if (storedValuesCount < CHIMP_BUFFER_SIZE) {
            storedValuesCount++;
        }
    }
    
    private void readChimpValue() {
        // Read first 2 bits to determine encoding pattern
        int first2Bits = (int) bitReader.readBits(2);
        
        if (first2Bits == 2) {
            // Pattern: 10 (2 bits) - Reuse previous leading zeros
            handleReuseLeadingZeros();
        } else {
            // Read 3 more bits to get 5 bits total for pattern identification
            int next3Bits = (int) bitReader.readBits(3);
            int first5Bits = (first2Bits << 3) | next3Bits;
            
            if (isNewLeadingZerosPattern(first5Bits)) {
                // Pattern: 11xxx (5 bits, 24-31) - New leading zeros pattern
                handleNewLeadingZeros(first5Bits - 24);
            } else {
                // Pattern: either exact match or trailing zeros - need more bits to determine
                handleExactMatchOrTrailingZeros(first5Bits);
            }
        }
        
        // Update algorithm state after successful decode
        updateBufferState();
    }
    
    /**
     * Handle reuse leading zeros case: decode XOR using stored leading zeros
     */
    private void handleReuseLeadingZeros() {
        int significantBits = 64 - storedLeadingZeros;
        long xor = bitReader.readBits(significantBits);
        currentValue = decodeXor(xor, index % CHIMP_BUFFER_SIZE);
    }
    
    /**
     * Handle new leading zeros pattern: update stored leading zeros and decode XOR
     */
    private void handleNewLeadingZeros(int leadingRepIndex) {
        int leadingZeros = findLeadingZerosFromRepresentation(leadingRepIndex);
        storedLeadingZeros = leadingZeros;
        
        int significantBits = 64 - leadingZeros;
        long xor = bitReader.readBits(significantBits);
        currentValue = decodeXor(xor, index % CHIMP_BUFFER_SIZE);
    }
    
    /**
     * Handle exact match or trailing zeros cases based on remaining bit patterns
     */
    private void handleExactMatchOrTrailingZeros(int first5Bits) {
        if (flagZeroSize <= 5) {
            // We've read enough bits for complete flagZeroSize pattern
            handleCompletePattern(first5Bits);
        } else {
            // Need to read more bits to complete flagZeroSize pattern
            int remainingBits = flagZeroSize - 5;
            int nextBits = (int) bitReader.readBits(remainingBits);
            int completePattern = (first5Bits << remainingBits) | nextBits;
            handleCompletePattern(completePattern);
        }
    }
    
    /**
     * Handle complete bit pattern - either exact match or trailing zeros
     */
    private void handleCompletePattern(int pattern) {
        if (pattern < CHIMP_BUFFER_SIZE) {
            // Exact match: pattern is direct buffer index
            handleExactMatch(pattern);
        } else {
            // Trailing zeros: pattern is start of complex flag, need more bits
            handleTrailingZeros(pattern);
        }
    }
    
    /**
     * Handle exact match case: retrieve value directly from buffer
     */
    private void handleExactMatch(int bufferIndex) {
        if (bufferIndex >= CHIMP_BUFFER_SIZE) {
            throw new RuntimeException("Invalid exact match index: " + bufferIndex);
        }
        currentValue = Double.longBitsToDouble(storedValues[bufferIndex]);
        storedLeadingZeros = 65; // Reset to force new leading zero pattern next time
    }
    
    /**
     * Handle trailing zeros case: decode complex flag format
     */
    private void handleTrailingZeros(int patternStart) {
        // Read remaining bits to complete flagOneSize pattern
        int remainingBits = flagOneSize - flagZeroSize;
        int finalBits = (int) bitReader.readBits(remainingBits);
        long fullFlag = ((long) patternStart << remainingBits) | finalBits;
        
        // Decode flag components (matches encoding in ChimpAppender)
        int significantBits = (int) (fullFlag & 63);                    // Bits 0-5: significant bits count
        int leadingRepIndex = (int) ((fullFlag >> 6) & 7);              // Bits 6-8: leading zeros representation
        int referenceIndex = (int) ((fullFlag >> 9) - CHIMP_BUFFER_SIZE); // Bits 9+: reference buffer index
        
        // Calculate actual leading and trailing zeros
        int leadingZeros = findLeadingZerosFromRepresentation(leadingRepIndex);
        int trailingZeros = 64 - leadingZeros - significantBits;
        
        // Read and reconstruct XOR value
        long significantValue = bitReader.readBits(significantBits);
        long xor = significantValue << trailingZeros;
        
        // Decode using reference value
        currentValue = decodeXor(xor, referenceIndex % CHIMP_BUFFER_SIZE);
        storedLeadingZeros = 65; // Reset to force new leading zero pattern next time
    }
    
    /**
     * Decode a value by XORing with reference value from buffer
     */
    private double decodeXor(long xor, int referenceIndex) {
        long referenceValue = storedValues[referenceIndex];
        long newValueBits = referenceValue ^ xor;
        return Double.longBitsToDouble(newValueBits);
    }
    
    /**
     * Check if bit pattern represents new leading zeros encoding (24-31)
     */
    private boolean isNewLeadingZerosPattern(int pattern) {
        return pattern >= 24 && pattern <= 31;
    }
    
    /**
     * Update ring buffer and hash table state after successful decode
     */
    private void updateBufferState() {
        long valueBits = Double.doubleToRawLongBits(currentValue);
        
        // Update ring buffer position
        currentIndex = (currentIndex + 1) % CHIMP_BUFFER_SIZE;
        if (storedValuesCount < CHIMP_BUFFER_SIZE) {
            storedValuesCount++;
        }
        
        // Store new value and update hash table
        storedValues[currentIndex] = valueBits;
        index++;
        indices[(int) valueBits & setLsb] = index;
    }
    
    private int findLeadingZerosFromRepresentation(int representation) {
        // Reverse lookup in leadingRepresentation array
        for (int i = 0; i < ChimpAppender.leadingRepresentation.length; i++) {
            if (ChimpAppender.leadingRepresentation[i] == representation) {
                return ChimpAppender.leadingRound[i];
            }
        }
        return 0; // fallback
    }
} 