/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.MetricsEngine;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.ts.model.Labels;
import java.util.Map;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class MetricsEngineTests extends EngineTestCase {

    private IndexSettings indexSettings;
    private Store engineStore;
    private MetricsEngine metricsEngine;
    private EngineConfig engineConfig;
    private ClusterApplierService clusterApplierService;
    private MapperService mapperService;

    // TODO: figure out mapping
    //  If we want to use `nested` field, the parent doc must contain a `_primary_term` field because the query stack
    //  will implicitly add a `FieldExistsQuery(field=_primary_term)` for nested index schema queries. And since
    //  we're not actually doing a nested field/lucene block join, we should probably not use nested type to describe
    //  this.
    private static final String MAPPING = """
        {
          "properties": {
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


    /**
     * Sample JSON document representing a metric series.
     * <pre>
     *   {
     *      "labels": [
     *        {"name": "__name__", "value":"http_requests_total"},
     *        {"name": "method", "value":"POST"},
     *        {"name": "handler", "value":"/api/items"},
     *        {"name": "status", "value":"200"}
     *      ],
     *      "samples":[
     *        {
     *          "timestamp": 1712576200.000,
     *          "value":1024
     *        }
     *      ]
     *    }
     * </pre>
     */


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


    @Override
    @Before
    public void setUp() throws Exception {
        // Enable debug logging for the metrics engine package
        Configurator.setAllLevels("org.opensearch.ts", Level.DEBUG);

        indexSettings = newIndexSettings();
        super.setUp();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(indexSettings, newDirectory());

        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.state()).thenReturn(ClusterState.EMPTY_STATE);
        metricsEngine = buildMetricsEngine(globalCheckpoint, engineStore, indexSettings, clusterApplierService);
    }

    private MetricsEngine buildMetricsEngine(
        AtomicLong globalCheckpoint,
        Store store,
        IndexSettings settings,
        ClusterApplierService clusterApplierService
    ) throws IOException {
        if (engineConfig == null) {
            engineConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        }
        // overwrite the config
        mapperService = createMapperService(MAPPING);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null), clusterApplierService);
        if (!Lucene.indexExists(store.directory())) {
            store.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }
        MetricsEngine engine = new MetricsEngine(engineConfig, createTempDir("metrics"));
        return engine;
    }

    public void publishSample(int id, String json) throws IOException {
        SourceToParse sourceToParse = new SourceToParse("index", Integer.toString(id), new BytesArray(json), XContentType.JSON);
        ParsedDocument parsedDoc = mapperService
            .documentMapper()
            .parse(sourceToParse);
        metricsEngine.index(new Engine.Index(
            new Term("_id",  Integer.toString(id)),
            parsedDoc,
            UNASSIGNED_SEQ_NO, // seqNo
            0, // primaryTerm,
            id,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        ));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (metricsEngine != null) {
            metricsEngine.close();
        }
        if (engineStore != null) {
            engineStore.close();
        }
        super.tearDown();
        engineConfig = null;
    }

    public void testEngine() throws IOException {
        metricsEngine.refresh("warm_up");

        List<String> samples = List.of(
            // series 1
            createSampleJson(
                new MetricsEngine.MetricDocument(
                    series1,
                    List.of(
                        new MetricsEngine.MetricDocument.Sample(1712576200L, 1024.0))
                )
            ),
            createSampleJson(
                new MetricsEngine.MetricDocument(
                    series1,
                    List.of(
                        new MetricsEngine.MetricDocument.Sample(1712576400L, 1026.0))
                )
            ),

            // series 2 (creates new chunk)
            createSampleJson(
                new MetricsEngine.MetricDocument(
                    series2,
                    List.of(
                        new MetricsEngine.MetricDocument.Sample(1712576200L, 1024.0),
                        new MetricsEngine.MetricDocument.Sample(1712576400L, 1026.0))
                )
            )
        );

        for (int i = 0; i < samples.size(); i++) {
            publishSample(i, samples.get(i));
        }

        metricsEngine.refresh("index");
        metricsEngine.maybeSafeCompactHead();
    }

//    public void testEngineAppender() {
//        MetricsEngine engine = new MetricsEngine(createTempDir("ts"));
//        MetricsEngine.MetricsAppender appender = engine.newAppender();
//
//        long ref1 = appender.append(0, Labels.fromStrings("a", "b"), 123, 0.0);
//
//        // reference shall work before commit
//        long ref2 = appender.append(ref1, Labels.emptyLabels(), 124, 1);
//
//        Assert.assertEquals(ref1, ref2);
//
//        appender.commit();
//        engine.close();
//
//        // query
//        RangeHead head = new RangeHead(engine.getHead(), 0, 1000);
//        Chunk chunk = head.chunks().readChunk(new Meta(0,null, 120, 199));
//    }

    protected IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
    }
}
