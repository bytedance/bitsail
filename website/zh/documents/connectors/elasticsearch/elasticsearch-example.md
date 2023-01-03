# Elasticsearch 连接器示例

上级文档：[Elasticsearch 连接器](./elasticsearch.md) 

如下展示了如何使用用户参数配置写入指定的elasticsearch索引。

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
