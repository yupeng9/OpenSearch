/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

import java.util.Map;
import java.util.Objects;

/**
 * Labels is a set of name/value pairs.
 * TODO: add the encoding-based label impl
 */
public class Labels {
    private final Map<String, String> labels;

    Labels(Map<String, String> labels) {
        this.labels = labels;
    }

    public static Labels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("Labels must be in pairs");
        }
        Map<String, String> labelMap = new java.util.HashMap<>();
        for (int i = 0; i < labels.length; i += 2) {
            labelMap.put(labels[i], labels[i + 1]);
        }
        return new Labels(labelMap);
    }

    public static Labels emptyLabels() {
        return new Labels(Map.of());
    }

    public boolean isEmpty() {
        return labels.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Labels)) return false;
        Labels other = (Labels) o;
        return Objects.equals(this.labels, other.labels);
    }

    @Override
    public int hashCode() {
        // TODO: implement a similar stable hash as prometheus
        return Objects.hash(labels);
    }

}
