/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * Iterator for reading XOR-compressed chunk data
 */
public class XORIterator extends CompressionIterator {
    byte leading;
    byte trailing;

    public XORIterator() {
        super();
    }

    public XORIterator(byte[] data) {
        super(data);
    }

    @Override
    protected void resetAlgorithmState() {
        this.leading = 0;
        this.trailing = 0;
    }

    @Override
    protected void onFirstValueRead(double value) {
        // No special initialization needed for XOR
    }

    @Override
    protected void readValue() {
        readXOR();
    }

    private void readXOR() {
        int bit = bitReader.readBit();
        if (bit == 0) {
            // No change in value
            return;
        }

        bit = bitReader.readBit();
        
        byte newLeading, newTrailing;
        byte numBits;

        if (bit == 0) {
            // Reuse previous leading/trailing
            newLeading = leading;
            newTrailing = trailing;
            numBits = (byte) (64 - newLeading - newTrailing);
        } else {
            // Read new leading/trailing
            newLeading = (byte) bitReader.readBits(5);
            byte sigBits = (byte) bitReader.readBits(6);
            
            // Handle special case where 0 means 64 bits
            if (sigBits == 0) {
                sigBits = 64;
            }
            
            newTrailing = (byte) (64 - newLeading - sigBits);
            numBits = sigBits;
            
            // Update leading/trailing for next iteration
            leading = newLeading;
            trailing = newTrailing;
        }

        long valueBits = bitReader.readBits(numBits);
        long currentValueBits = Double.doubleToRawLongBits(currentValue);
        long newValueBits = currentValueBits ^ (valueBits << newTrailing);
        currentValue = Double.longBitsToDouble(newValueBits);
    }


} 