/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

public interface Sample {
    /**
     * Get the timestamp of the sample.
     *
     * @return the timestamp
     */
    long getTimestamp();

    ValueType valueType();
}
