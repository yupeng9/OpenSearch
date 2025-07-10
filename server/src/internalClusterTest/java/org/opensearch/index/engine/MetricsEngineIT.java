/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.transport.client.Client;
import org.opensearch.ts.model.Labels;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

@ClusterScope(supportsDedicatedMasters = false, numDataNodes = 1, scope = OpenSearchIntegTestCase.Scope.SUITE)
public class MetricsEngineIT extends OpenSearchIntegTestCase {
    private static final String MAPPING = """
        {
          "properties": {
            "series_ref": {
              "type": "long"
            },
            "labels": {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "value": {
                  "type": "keyword"
                }
              }
            },
            "samples": {
              "properties": {
                "value": {
                  "type": "float"
                },
                "timestamp": {
                  "type": "date",
                  "format": "epoch_millis"
                }
              }
            }
          }
        }
        """ ;
    private static final String TEST_INDEX_NAME = "metrics-test";

    static Labels series1 = Labels.fromStrings(
        "__name__", "http_requests_total",
        "method", "POST",
        "handler", "/api/items",
        "status", "200"
    );

    static Labels series2 = Labels.fromStrings(
        "__name__", "http_requests_total",
        "method", "POST",
        "handler", "/api/items",
        "status", "429"
    );
    private List<String> SAMPLES_JSON = List.of(
        // series 1: 2 samples in the same chunk
        createSampleJson(
            new MetricsEngine.MetricDocument(
                series1,
                List.of(
                    new MetricsEngine.MetricDocument.Sample(1712576200L, 1024.0)),
                null
            )
        ),
        createSampleJson(
            new MetricsEngine.MetricDocument(
                series1,
                List.of(
                    new MetricsEngine.MetricDocument.Sample(1712576400L, 1026.0)),
                null
            )
        ),

        // series 2 (creates new chunk)
        createSampleJson(
            new MetricsEngine.MetricDocument(
                series2,
                List.of(
                    new MetricsEngine.MetricDocument.Sample(1712576200L, 1024.0),
                    new MetricsEngine.MetricDocument.Sample(1712576400L, 1026.0)
                ), null
            )
        )
    );

    private static String createSampleJson(MetricsEngine.MetricDocument document) {
        StringBuilder sb = new StringBuilder("{ \"labels\": [");

        // iterate over labels map and append to JSON
        List<Map.Entry<String, String>> labels = document.labels().toMapView().entrySet().stream().toList();

        for (int i = 0; i < labels.size(); i++) {
            var label = labels.get(i);
            sb.append("{\"name\":\"").append(label.getKey()).append("\",\"value\":\"").append(label.getValue()).append("\"}");
            if (i < labels.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("], \"samples\": [");
        List<MetricsEngine.MetricDocument.Sample> samples = document.samples();
        for (int i = 0; i < samples.size(); i++) {
            MetricsEngine.MetricDocument.Sample sample = samples.get(i);
            sb.append("{\"timestamp\":").append(sample.timestamp()).append(",\"value\":").append(sample.value()).append("}");
            if (i < samples.size() - 1) {
                sb.append(",");
            }
        }

        sb.append("]}");
        return sb.toString();
    }

    @Before
    public void setup() throws Exception {
        final int numOfShards = 1;
        // some settings to keep num segments low
        assertAcked(
            prepareCreate(TEST_INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put("index.metrics.enabled", true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                ).setMapping(MAPPING)
        );
    }

    @After
    public void cleanup() throws Exception {
        assertAcked(client().admin().indices().prepareDelete(TEST_INDEX_NAME));
    }

    public void testCreateIndex() {
        ensureGreen(TEST_INDEX_NAME);
    }

    public void testIndexSample() {
        ensureGreen(TEST_INDEX_NAME);

        try (Client client = client()) {
            for (String sample : SAMPLES_JSON) {
                var indexResponse =
                    client
                        .prepareIndex(TEST_INDEX_NAME)
                        .setSource(sample, XContentType.JSON)
                        .get();

                // assert index success
                assertEquals(DocWriteResponse.Result.CREATED,  indexResponse.getResult());
            }

            // refresh the index to make sure all documents are searchable
            refresh(TEST_INDEX_NAME);

            // match_all query, should match 2 docs corresponding to series 1 and series 2
            SearchResponse searchResponse = client
                .prepareSearch(TEST_INDEX_NAME)
                .setQuery(new MatchAllQueryBuilder())
                .setSize(5)
                .get();

            assertHitCount(searchResponse, 2);
        }
    }

    public void testIndexSampleBulk() {
        ensureGreen(TEST_INDEX_NAME);

        try (Client client = client()) {
            var bulkResponse = client.prepareBulk()
                .add(
                    client.prepareIndex(TEST_INDEX_NAME)
                        .setSource(SAMPLES_JSON.get(0), XContentType.JSON))
                .add(
                    client.prepareIndex(TEST_INDEX_NAME)
                        .setSource(SAMPLES_JSON.get(1), XContentType.JSON))
                .add(
                    client.prepareIndex(TEST_INDEX_NAME)
                        .setSource(SAMPLES_JSON.get(2), XContentType.JSON))
                .get();

            // assert bulk success
            assertFalse(bulkResponse.hasFailures());
            assertNoFailures(bulkResponse);

            // refresh the index to make sure all documents are searchable
            refresh(TEST_INDEX_NAME);

            // match_all query, should match 2 docs corresponding to series 1 and series 2
            SearchResponse searchResponse = client
                .prepareSearch(TEST_INDEX_NAME)
                .setQuery(new MatchAllQueryBuilder())
                .setSize(5)
                .get();

            assertHitCount(searchResponse, 2);
        }
    }
}
