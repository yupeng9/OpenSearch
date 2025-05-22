/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

/**
 * Series exposes a single time series and allows iterating over the samples.
 *
 * TODO: can we simplify this since we store labels in lucene index?
 */
public interface Series {
    Labels getLabels();

    Iterator iterator();
}
