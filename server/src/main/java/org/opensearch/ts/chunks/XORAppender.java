/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

/**
 * A XOR appender that implements delta-of-delta encoding in Facebook's Gorilla paper:
 * https://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
 */
public class XORAppender extends CompressionAppender {
    private byte leading;
    private byte trailing;

    // Constructor for new appender (used when chunk is empty)
    public XORAppender() {
        super();
        this.leading = (byte) 0xFF; // Initial state
        this.trailing = 0;
    }

    // Constructor for existing chunk state
    public XORAppender(XORChunk chunk, long lastTimestamp, double lastValue, 
                      long timeDelta, byte leading, byte trailing) {
        super(chunk, lastTimestamp, lastValue, timeDelta);
        this.leading = leading;
        this.trailing = trailing;
    }

    public void setLeading(byte leading) {
        this.leading = leading;
    }

    public void setChunk(XORChunk chunk) {
        this.chunk = chunk;
    }

    @Override
    protected void onFirstValue(double value) {
        // No special initialization needed for XOR beyond constructor
    }

    @Override
    protected void writeValue(double value) {
        writeXOR(value, lastValue);
    }

    private void writeXOR(double newValue, double currentValue) {
        long newBits = Double.doubleToRawLongBits(newValue);
        long currentBits = Double.doubleToRawLongBits(currentValue);
        long delta = newBits ^ currentBits;

        if (delta == 0) {
            chunk.getBitStream().writeBit(0);
            return;
        }

        chunk.getBitStream().writeBit(1);

        byte newLeading = (byte) Long.numberOfLeadingZeros(delta);
        byte newTrailing = (byte) Long.numberOfTrailingZeros(delta);

        // Clamp leading zeros to avoid overflow when encoding
        if (newLeading >= 32) {
            newLeading = 31;
        }

        if (leading != (byte) 0xFF && newLeading >= leading && newTrailing >= trailing) {
            // Reuse previous leading/trailing
            chunk.getBitStream().writeBit(0);
            int numBits = 64 - leading - trailing;
            chunk.getBitStream().writeBits(delta >>> trailing, numBits);
        } else {
            // Update leading/trailing
            leading = newLeading;
            trailing = newTrailing;

            chunk.getBitStream().writeBit(1);
            chunk.getBitStream().writeBits(newLeading, 5);

            int sigBits = 64 - newLeading - newTrailing;
            // Handle special case where sigBits would be 64 (doesn't fit in 6 bits)
            if (sigBits == 64) {
                chunk.getBitStream().writeBits(0, 6);
            } else {
                chunk.getBitStream().writeBits(sigBits, 6);
            }
            chunk.getBitStream().writeBits(delta >>> newTrailing, sigBits);
        }
    }


}
