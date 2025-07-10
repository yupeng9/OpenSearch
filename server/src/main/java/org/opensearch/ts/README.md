## Start the server

```bash
./gradlew run
```

## Create an index of metrics

```bash
curl -X PUT -H 'Content-Type: application/json' http://localhost:9200/my-index --data '{
  "settings": {
    "index.metrics.enabled":true,
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "refresh_interval": "1s"
  },
  "mappings": {
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
  }'
```

Note the mappings later can be built into the engine as default.

NOTE:
* Nested Field schemas mapping will trigger the query flow to implicitly generated FieldExistsQuery[name=_primary_term] internally in the query layer [here](https://sg.uberinternal.com/code.uber.internal/uber-code/search-opensearch@8444ec23e224318aeb14eba1054e42b6956fe32b/-/blob/server/src/main/java/org/opensearch/common/lucene/search/Queries.java?L94). So if we defined the schema with `nested` type, then we must include a _primary_term field in the lucene doc. We should re-evaluate the design again later.

## Index some metrics

```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:9200/my-index/_doc --data '{
  "labels": [
    {"name": "__name__", "value":"http_requests_total"},
    {"name": "method", "value":"POST"},
    {"name": "handler", "value":"/api/items"},
    {"name": "status", "value":"200"}
  ],
  "samples": [
    {"value": 10.1, "timestamp": 1633072800000},
    {"value": 11.5, "timestamp": 1633076400000}
  ]
}'
```
```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:9200/my-index/_doc --data '{
  "labels": [
    {"name": "__name__", "value":"http_requests_total"},
    {"name": "method", "value":"POST"},
    {"name": "handler", "value":"/api/stores"},
    {"name": "status", "value":"200"}
  ],
  "samples": [
    {"value": 10.1, "timestamp": 1633072800000},
    {"value": 11.5, "timestamp": 1633076400000}
  ]
}'
```

#### Bulk API
```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:9200/my-index/_bulk --data-binary '
{ "index": { "_index": "my-index" } }
{"labels":[{"name":"__name__","value":"http_requests_total"},{"name":"method","value":"POST"},{"name":"handler","value":"/api/stores"},{"name":"status","value":"200"}],"samples":[{"value":10.1,"timestamp":1633072800000},{"value":11.5,"timestamp":1633076400000}]}
{ "index": { "_index": "my-index" } }
{"labels":[{"name":"__name__","value":"http_requests_total"},{"name":"method","value":"GET"},{"name":"handler","value":"/api/products"},{"name":"status","value":"404"}],"samples":[{"value":5.3,"timestamp":1633072800000},{"value":6.7,"timestamp":1633076400000}]}
'
```


Refresh to trigger segment flush and reader refresh so that the indexed documents are searchable.
```bash
curl -XGET http://localhost:9200/my-index/_refresh
```

## Query the metrics

Query all
```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:9200/my-index/_search --data '{
  "query": {
    "match_all": {}
  }
}'
```

Query by label
TODO: This doesn't work yet because our index isn't synchronized with MappingService, so when you try to query a field,
the query generator treats it as "field does not exist" and generates a MatchNoDocsQuery instead.
The other problem is that the actual Lucene field names we use in the metric index does not actually map to the MappingService schema.
e.g. OS expects "labels.name" and "labels.value" to be the lucene fields, but internally we actually use internal names like "labels" or "__labels__",
so the queries will always return emtpy results when you start doing fitler queries.
The below query will work if we add this to the LiveSeriesIndex#addSeries() method:
```java
        for (var label : labels.toMapView().entrySet()) {
            doc.add(new StringField("labels.name", label.getKey(),  Field.Store.NO));
            doc.add(new StringField("labels.value", label.getValue(), Field.Store.NO));
        }
```
```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:9200/my-index/_search --data '{
  "query": {
    "bool": {
      "filter": [
        {
          "match": {
            "labels.value": "/api/stores"
          }
        },
        {
          "match": {
            "labels.name": "handler"
          }
        }
      ]
    }
  }
}'
```

# Optional
## Inspect index data on disk
```bash
tree build/testclusters/runTask-0/data
```
```plaintext
build/testclusters/runTask-0/data
└── nodes
    └── 0
        ├── _state
        │	 ├── _3.cfe
        │	 ├── _3.cfs
        │	 ├── _3.si
        │	 ├── _5.cfe
        │	 ├── _5.cfs
        │	 ├── _5.si
        │	 ├── manifest-0.st
        │	 ├── node-0.st
        │	 ├── segments_8
        │	 └── write.lock
        ├── indices
        │	 └── AovuL-g2QWOvPuDPQ05eig
        │	     ├── _state
        │	     │	 └── state-1.st
        │	     └── 0
        │	         ├── _state
        │	         │	 ├── retention-leases-1.st
        │	         │	 └── state-0.st
        │	         ├── index
        │	         │	 ├── segments_2
        │	         │	 └── write.lock
        │	         ├── metrics
        │	         │	 └── headDir
        │	         │	     └── block_1750985050083
        │	         │	         └── write.lock
        │	         └── translog
        │	             ├── translog-1.tlog
        │	             └── translog.ckp
        └── node.lock

14 directories, 19 files
```

## Cleanup: Delete the index
```bash
curl -X DELETE http://localhost:9200/my-index
```

