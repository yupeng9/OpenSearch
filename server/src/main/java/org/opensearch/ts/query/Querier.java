/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.query;

import org.opensearch.ts.model.SeriesSet;

/**
 * Querier provides querying access to the time series data over a fixed time range.
 */
public interface Querier {
    enum MatchType {
        EQUALS,
        NOT_EQUALS,
        REGEX,
        NOT_REGEX;
    }

    // TODO(phiiip): consider using OpenSearch QueryBuilder style interface instead.
    record Matcher(MatchType type, String labelName, String query) {}

    // prometheus style Querier interface
    SeriesSet select(long mint, long maxt, Matcher ...matchers);
}
