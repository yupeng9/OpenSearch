/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

/**
 * The iterator iterates over the time series data.
 */
public interface Iterator {
    boolean hasNext();

    Sample next();
}
