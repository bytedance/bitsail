# Elasticsearch connector example

Parent document: [Elasticsearch connector](./elasticsearch.md)


The following configuration shows how to organize parameter configuration to write the specified Elasticsearch index.

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchSink",
      "es_id_fields": "id",
      "es_index": "es_index_test",
      "es_hosts": ["http://localhost:1234"],
      "json_serializer_features": "QuoteFieldNames,UseSingleQuotes",
      "bulk_backoff_max_retry_count": 10,
      "columns": [
        {
          "name": "id",
          "type": "int"
        },
        {
          "name": "double_type",
          "type": "double"
        },
        {
          "name": "text_type",
          "type": "text"
        },
        {
          "name": "bigint_type",
          "type": "bigint"
        }
      ]
    }
  }
}
```
