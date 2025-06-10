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
public class XORAppender implements ChunkAppender {
    private XORChunk chunk;
    private long lastTimestamp;
    private double lastValue;
    private long timeDelta;
    private byte leading;
    private byte trailing;

    // Constructor for new appender (used when chunk is empty)
    public XORAppender() {
        this.lastTimestamp = Long.MIN_VALUE;
        this.lastValue = 0.0;
        this.timeDelta = 0;
        this.leading = (byte) 0xFF; // Initial state
        this.trailing = 0;
    }

    // Constructor for existing chunk state
    public XORAppender(XORChunk chunk, long lastTimestamp, double lastValue, 
                      long timeDelta, byte leading, byte trailing) {
        this.chunk = chunk;
        this.lastTimestamp = lastTimestamp;
        this.lastValue = lastValue;
        this.timeDelta = timeDelta;
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
    public void append(long timestamp, double value) {
        if (chunk == null) {
            throw new IllegalStateException("XORAppender not properly initialized with chunk");
        }

        int numSamples = chunk.numSamples();
        
        if (numSamples == 0) {
            // First sample - write timestamp and value directly
            chunk.getBitStream().writeVarint(timestamp);
            chunk.getBitStream().writeBits(Double.doubleToRawLongBits(value), 64);
        } else if (numSamples == 1) {
            // Second sample - write timestamp delta and XOR-compressed value
            long tDelta = timestamp - lastTimestamp;
            chunk.getBitStream().writeUvarint(tDelta);
            writeValueDelta(value);
            timeDelta = tDelta;
        } else {
            // Subsequent samples - use delta-of-delta encoding for timestamps
            long tDelta = timestamp - lastTimestamp;
            long deltaOfDelta = tDelta - timeDelta;
            
            writeTimestampDelta(deltaOfDelta);
            writeValueDelta(value);
            timeDelta = tDelta;
        }

        lastTimestamp = timestamp;
        lastValue = value;
        
        chunk.getBitStream().updateShortAt(0, (short) (numSamples + 1));
    }

    private void writeTimestampDelta(long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            chunk.getBitStream().writeBit(0);
        } else if (bitRange(deltaOfDelta, 14)) {
            // Write 2-bit header '10' followed by 14-bit delta
            chunk.getBitStream().writeBits(0b10L << 6 | (deltaOfDelta >> 8 & 0x3FL), 8);
            chunk.getBitStream().writeBits(deltaOfDelta & 0xFFL, 8);
        } else if (bitRange(deltaOfDelta, 17)) {
            chunk.getBitStream().writeBits(0b110, 3);
            chunk.getBitStream().writeBits(deltaOfDelta, 17);
        } else if (bitRange(deltaOfDelta, 20)) {
            chunk.getBitStream().writeBits(0b1110, 4);
            chunk.getBitStream().writeBits(deltaOfDelta, 20);
        } else {
            chunk.getBitStream().writeBits(0b1111, 4);
            chunk.getBitStream().writeBits(deltaOfDelta, 64);
        }
    }

    private void writeValueDelta(double value) {
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

    // Check if a value can be represented in the given number of bits (signed)
    private boolean bitRange(long value, int numBits) {
        long max = (1L << (numBits - 1)) - 1;
        long min = -(1L << (numBits - 1)) + 1;
        return value >= min && value <= max;
    }
}
