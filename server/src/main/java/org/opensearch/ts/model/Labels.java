/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Labels is a set of name/value pairs.
 * TODO: add the encoding-based label impl
 */
public class Labels {
    private final Map<String, String> labels;

    public Labels(Map<String, String> labels) {
        this.labels = labels;
    }

    /**
     * @param labels list of strings divisible by 2, where [k1, v1, k2, v2, ...]
     * @return
     */
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

    public String toKeyValueString() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue());
            sb.append(' ');
        }
        // Remove the trailing space, assumes there is at least one label with len > 0
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static Labels fromSerializedBytes(byte[] bytes) {
        Map<String, String> labelMap = new java.util.HashMap<>();
        try {
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            while (in.getPosition() < bytes.length) {
                String key = in.readString();
                String value = in.readString();
                labelMap.put(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize labels", e);
        }
        return new Labels(labelMap);
    }

    public static Labels emptyLabels() {
        return new Labels(Map.of());
    }

    public Map<String, String> toMapView() {
        return Map.copyOf(labels);
    }

    public boolean isEmpty() {
        return labels.isEmpty();
    }

    /**
     * Returns the labels serialized as a byte array.
     * @return
     */
    public int bytes(byte[] bytes) throws IOException {
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        return out.getPosition();
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
