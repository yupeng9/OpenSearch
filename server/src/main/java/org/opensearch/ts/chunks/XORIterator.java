/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.chunks;

import org.opensearch.ts.utils.BitReader;

import java.nio.ByteBuffer;

/**
 * Iterator for reading XOR-compressed chunk data
 */
public class XORIterator implements ChunkIterator {
    private BitReader bitReader;
    int totalSamples;
    private int samplesRead;
    
    long currentTimestamp;
    double currentValue;
    long timeDelta;
    byte leading;
    byte trailing;
    private Exception error;

    public XORIterator() {
        reset(null);
    }

    public XORIterator(byte[] data) {
        reset(data);
    }

    public void reset(byte[] data) {
        // The first 2 bytes contain chunk headers.
        // We skip that for actual samples.
        this.bitReader = new BitReader(java.util.Arrays.copyOfRange(data, 2, data.length));
        this.totalSamples = ByteBuffer.wrap(data, 0, 2).getShort() & 0xFFFF;

        this.samplesRead = 0;
        this.currentTimestamp = 0;
        this.currentValue = 0.0;
        this.leading = 0;
        this.trailing = 0;
        this.timeDelta = 0;
        this.error = null;
    }

    @Override
    public ChunkIterator.ValueType next() {
        if (error != null || samplesRead >= totalSamples) {
            return ChunkIterator.ValueType.NONE;
        }

        try {
            if (samplesRead == 0) {
                // First sample: read timestamp and value directly
                currentTimestamp = bitReader.readVarint();
                currentValue = Double.longBitsToDouble(bitReader.readBits(64));
                samplesRead++;
                return ChunkIterator.ValueType.FLOAT;
            } else if (samplesRead == 1) {
                // Second sample: read timestamp delta and XOR-compressed value
                timeDelta = bitReader.readUvarint();
                currentTimestamp += timeDelta;
                readValueDelta();
                samplesRead++;
                return ChunkIterator.ValueType.FLOAT;
            } else {
                // Subsequent samples: read delta-of-delta for timestamp
                long deltaOfDelta = readTimestampDelta();
                timeDelta = timeDelta + deltaOfDelta;
                currentTimestamp += timeDelta;
                readValueDelta();
                samplesRead++;
                return ChunkIterator.ValueType.FLOAT;
            }
        } catch (Exception e) {
            this.error = e;
            return ChunkIterator.ValueType.NONE;
        }
    }

    @Override
    public TimestampValue at() {
        return new TimestampValue(currentTimestamp, currentValue);
    }

    @Override
    public Exception error() {
        return error;
    }

    private long readTimestampDelta() {
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
            // TODO: make the read faster?
            long bits = bitReader.readBits(sz);
            
            // Account for negative numbers, which come back as high unsigned numbers.
            if (bits > (1L << (sz - 1))) {
                bits -= 1L << sz;
            }
            dod = bits;
        }

        return dod;
    }

    private void readValueDelta() {
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