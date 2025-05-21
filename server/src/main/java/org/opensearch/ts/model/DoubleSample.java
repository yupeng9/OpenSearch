/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

public class DoubleSample implements Sample {

    private final long timestamp;
    private final double value;

    public DoubleSample(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public ValueType valueType() {
        return ValueType.DOUBLE;
    }

    public double getValue() {
        return value;
    }
}
