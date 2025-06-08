/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts;

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
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
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

    private static final String MAPPING = """
        {
          "properties": {
            "labels": {
              "type": "nested",
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
              "type": "nested",
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

    private static String SAMPLE_1 = """
        {
           "labels": [
             {"name": "__name__", "value":"http_requests_total"},
             {"name": "method", "value":"POST"},
             {"name": "handler", "value":"/api/items"},
             {"name": "status", "value":"200"}
           ],
           "samples":[
             {
               "timestamp": 1712576200.000,
               "value":1024
             }
           ]
         }
        """;

    private static String SAMPLE_2 = """
        {
           "labels": [
             {"name": "__name__", "value":"http_requests_total"},
             {"name": "method", "value":"POST"},
             {"name": "handler", "value":"/api/items"},
             {"name": "status", "value":"200"}
           ],
           "samples":[
             {
               "timestamp": 1712576400.000,
               "value":1026
             }
           ]
         }
        """;


    @Override
    @Before
    public void setUp() throws Exception {
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
        engine.refresh("warm_up");
        publishSample(1, SAMPLE_1);
        publishSample(2, SAMPLE_2);
        engine.refresh("index");
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
