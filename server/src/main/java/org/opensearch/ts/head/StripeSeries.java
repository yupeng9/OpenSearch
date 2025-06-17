/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.model.Labels;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A collection of series. The series can be looked up by either hash or ID.
 */
public class StripeSeries {
    // TODO: why does prometheus do an additional sharding?
    private final Map<Long, MemSeries> series;
    private final SeriesHashmap seriesHashmap;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public StripeSeries() {
        series = new HashMap<>();
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
        lock.readLock().lock();
        try {
            return new ArrayList<>(series.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public void set(long hash, MemSeries s) {
        lock.writeLock().lock();
        try {
            series.put(s.getReference(), s);
            seriesHashmap.set(hash, s); // separate locks?
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static class SeriesHashmap {
        private Map<Long, MemSeries> unique;
        private Map<Long, List<MemSeries>> conflicts;

        public SeriesHashmap() {
            unique = new HashMap<>();
            conflicts = new HashMap<>();
        }

        public MemSeries get(long hash, Labels labels) {
            MemSeries s = unique.get(hash);
            if (s != null && labels.equals(s.getLabels())) {
                return s;
            }
            List<MemSeries> conflictList = conflicts.get(hash);
            if (conflictList != null) {
                for (MemSeries series : conflictList) {
                    if (labels.equals(s.getLabels())) {
                        return series;
                    }
                }
            }
            return null;
        }

        public void set(long hash, MemSeries s) {
            MemSeries existing = unique.get(hash);
            // TODO: why does prometheuse use || ?
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
                unique.put(hash, conflictList.get(0));
                conflictList.remove(0);
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
