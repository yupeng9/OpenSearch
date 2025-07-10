/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.ts.head.MemSeries;
import org.opensearch.ts.model.Labels;

import java.util.List;

/**
 * An appender provides a batched appends of samples.
 */
public interface Appender {
    /**
     * Append a sample to the appender.
     *
     * @param seriesRef the optional series id for acceleration
     * @param labels the labels
     * @param timestamp the timestamp
     * @param value the value
     * @return the series reference
     */
    long append(long seriesRef, Labels labels, long timestamp, double value);

    void commit();

    /**
     * Returns all series that were creating by this appender.
     */
    List<MemSeries> createdSeries();

    void abort();
}
