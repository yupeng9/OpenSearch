/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

public class RefSample extends DoubleSample {
    private long reference;

    public RefSample(long reference, long timestamp, double value) {
        super(timestamp, value);
        this.reference = reference;
    }

    public long getReference() {
        return reference;
    }
}
