/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.model.Labels;

/**
 * An engine for Metrics
 * TODO: convert this to an OpenSearch engine
 */
public class MetricsEngine {
    private Head head;

    public MetricsEngine() {
        head = new Head();
    }

    public MetricsAppender newAppender() {
        return new MetricsAppender();
    }

    public class MetricsAppender implements Appender {

        HeadAppender headAppender;

        public MetricsAppender() {

        }

        @Override
        public long append(long seriesRef, Labels labels, long timestamp, double value) {
            if (headAppender == null) {
                head.initTime(timestamp);
                headAppender = head.newAppender();
            }
            return headAppender.append(seriesRef, labels, timestamp, value);
        }

        @Override
        public void commit() {
            if(headAppender == null) {
                throw new IllegalStateException("head appender is null");
            }
            headAppender.commit();

            // TODO: perform compaction
        }

        @Override
        public void abort() {

        }
    }
}
