/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.Meta;
import org.opensearch.ts.head.RangeHead;
import org.opensearch.ts.model.Labels;
import org.junit.Assert;

public class MetricsEngineTests extends OpenSearchTestCase {
    public void testEngineAppender() {
        MetricsEngine engine = new MetricsEngine(createTempDir("ts"));
        MetricsEngine.MetricsAppender appender = engine.newAppender();

        long ref1 = appender.append(0, Labels.fromStrings("a", "b"), 123, 0.0);

        // reference shall work before commit
        long ref2 = appender.append(ref1, Labels.emptyLabels(), 124, 1);

        Assert.assertEquals(ref1, ref2);

        appender.commit();
        engine.close();

        // query
        RangeHead head = new RangeHead(engine.getHead(), 0, 1000);
        Chunk chunk = head.chunks().readChunk(new Meta(0,null, 120, 199));
    }

}
