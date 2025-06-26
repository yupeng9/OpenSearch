## Start the server

```bash
./gradlew run
```

## Create an index of metrics

```bash
curl -X PUT -H 'Content-Type: application/json' http://localhost:9200/my-index --data '{
  "settings": {
    "index.metrics.enabled":true
  },
  "mappings": {
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
  }'
```

Note the mappings later can be built into the engine as default

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
Note the index endpoint fails for now due to null returned value, but it logs samples to index.
