/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.model;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.ts.head.MemSeries;

import java.io.IOException;
import java.util.Arrays;

public class LabelsTests extends OpenSearchTestCase {

    public void testSerDeser() throws IOException {
        String[] labels = { "k1", "v1", "k2", "v2" };
        Labels labelObj = Labels.fromStrings(labels);

        byte[] bytes = new byte[MemSeries.MAX_SERIALIZED_SIZE];
        int pos = labelObj.bytes(bytes);
        Labels deserialized = Labels.fromSerializedBytes(Arrays.copyOfRange(bytes, 0, pos)); // just to check deserialization

        assertEquals(labelObj, deserialized);
    }
}
