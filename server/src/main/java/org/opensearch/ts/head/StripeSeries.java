/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.apache.lucene.internal.hppc.LongObjectHashMap;
import org.apache.lucene.internal.hppc.ObjectCursor;
import org.opensearch.common.util.concurrent.ConcurrentHashMapLong;
import org.opensearch.ts.model.Labels;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A collection of series. The series can be looked up by either hash or ID.
 */
public class StripeSeries {
    // TODO: prometheus does it's own sharding to avoid lock contention, is java's concurrent map good enough?
    private final ConcurrentHashMapLong<MemSeries> series;
    private final SeriesHashmap seriesHashmap;

    public StripeSeries() {
        series = new ConcurrentHashMapLong<>(new ConcurrentHashMap<>());
        seriesHashmap = new SeriesHashmap();
    }

    public MemSeries getById(long id) {
        return series.get(id);
    }

    public MemSeries getByHash(long hash, Labels labels) {
        return seriesHashmap.get(hash, labels);
    }

    /**
     * Returns a list containing a snapshot of the current series.
     */
    public List<MemSeries> getSeries() {
        return new ArrayList<>(series.values());
    }

    public void set(long hash, MemSeries s) {
        series.put(s.getReference(), s);
        seriesHashmap.set(hash, s);
    }

    public void delete(MemSeries s) {
        long ref = s.getReference();
        series.remove(ref);
        seriesHashmap.delete(s.getLabels().hashCode(), ref);
    }

    public static class SeriesHashmap {
        private final ConcurrentHashMapLong<MemSeries> unique;
        private final ConcurrentHashMapLong<List<MemSeries>> conflicts; // .get is rare, so unboxing isn't a major concern

        public SeriesHashmap() {
            unique = new ConcurrentHashMapLong<>(new ConcurrentHashMap<>());
            conflicts = new ConcurrentHashMapLong<>(new ConcurrentHashMap<>());
        }

        public MemSeries get(long hash, Labels labels) {
            MemSeries s = unique.get(hash);
            if (s != null && labels.equals(s.getLabels())) {
                return s;
            }
            List<MemSeries> conflictList = conflicts.get(hash);
            if (conflictList != null) {
                for (MemSeries series : conflictList) {
                    if (labels.equals(series.getLabels())) {
                        return series;
                    }
                }
            }
            return null;
        }

        public void set(long hash, MemSeries s) {
            MemSeries existing = unique.get(hash);
            // TODO: why does prometheuse use || ?
            //   => series refs can change during their compaction or wal replay, do we also need to use ||?
            if (existing == null || existing.getLabels().equals(s.getLabels())) {
                unique.put(hash, s);
                return;
            }

            conflicts.computeIfAbsent(hash, k -> new ArrayList<>());
            List<MemSeries> conflictList = conflicts.get(hash);

            for (int i = 0; i < conflictList.size(); i++) {
                MemSeries prev = conflictList.get(i);
                if (s.getLabels().equals(prev.getLabels())) {
                    conflictList.set(i, s);
                    return;
                }
            }

            conflictList.add(s);
        }

        public void delete(long hash, long ref) {
            MemSeries uniqueSeries = unique.get(hash);
            if (uniqueSeries == null) return;

            List<MemSeries> conflictList = conflicts.get(hash);
            if (uniqueSeries.getReference() == ref) {
                if (conflictList == null || conflictList.isEmpty()) {
                    // exactly one series with this hash
                    unique.remove(hash);
                    return;
                }
                unique.put(hash, conflictList.getFirst());
                conflictList.removeFirst();
            } else {
                if (conflictList == null) return;

                List<MemSeries> remaining = new ArrayList<>();
                for (MemSeries s : conflictList) {
                    if (s.getReference() != ref) {
                        remaining.add(s);
                    }
                }

                if (remaining.isEmpty()) {
                    conflicts.remove(hash);
                } else {
                    conflicts.put(hash, remaining);
                }
            }
        }
    }
}
