/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.shard.IndexShardIT.newIndexShard;
import static org.opensearch.index.shard.IndexShardIT.recoverShard;
import static org.opensearch.index.shard.IndexShardTestCase.getTranslog;

public class MetricsEngineSingleNodeIT extends OpenSearchSingleNodeTestCase {

    public void testMetricsEngineTranslogRecovery() throws IOException {
        String mapping = """
        {
          "properties": {
            "seriesRef": {
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

        // create the metrics index, wait for it to be green
        client().admin()
            .indices()
            .prepareCreate("metrics")
            .setSettings(Settings.builder()
                .put("index.metrics.enabled", true)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()).setMapping(mapping).get();
        ensureGreen("metrics");

        // insert some samples
        for (int series = 0; series < 1; series++) {
            for (int sample = 0; sample < 3; sample++) {
                client().prepareIndex("metrics")
                    .setSource(createSampleJson(new MetricsEngine.MetricDocument(
                        Labels.fromStrings("__name__",
                            "http_requests_total",
                            "method",
                            "POST",
                            "handler",
                            "/api/items",
                            "status",
                            "" + (200 + series)
                        ),
                        List.of(new MetricsEngine.MetricDocument.Sample(10000 + sample * 10, sample)), null
                    )), XContentType.JSON).get();
            }
        }

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("metrics"));
        IndexShard originalShard = indexService.getShardOrNull(0);

        Translog translog = getTranslog(originalShard);
        assertTrue("translog should have operations", translog.stats().getUncommittedOperations() > 0);

        // 1. simluate node shutdown by closing the shard
        originalShard.close("simulate shutdown", false, false);

        // 2. simulate node restart by creating a new shard
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexShard newShard = newIndexShard(indexService,
            originalShard,
            directoryReader -> directoryReader,
            getInstanceFromNode(CircuitBreakerService.class),
            env.nodeId(),
            getInstanceFromNode(ClusterService.class)
        );

        // 3. perform translog recovery
        recoverShard(newShard);

        // 4. verify recovery works
        assertTrue(newShard.isStartedPrimary());

        newShard.close("test cleanup", false, false);
    }

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

}
