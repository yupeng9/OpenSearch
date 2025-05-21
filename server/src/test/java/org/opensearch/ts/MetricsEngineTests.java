/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.model.Labels;
import org.junit.Assert;

public class MetricsEngineTests extends OpenSearchTestCase {
    public void testEngineAppender() {
        MetricsEngine engine = new MetricsEngine();
        MetricsEngine.MetricsAppender appender = engine.newAppender();

        long ref1 = appender.append(0, Labels.fromStrings("a", "b"), 123, 0.0);

        // reference shall work before commit
        long ref2 = appender.append(ref1, Labels.emptyLabels(), 124, 1);

        Assert.assertEquals(ref1, ref2);

        appender.commit();
    }

}
