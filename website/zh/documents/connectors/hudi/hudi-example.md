# Hudi 连接器示例

上级文档：[Hudi 连接器](./hudi.md)

## Hudi 读连接器

读取 Hudi 表的用户配置：

```json
{
   "job": {
     "reader":{
       "hoodie":{
         "datasource":{
           "query":{
             "type":"snapshot"
           }
         }
       },
       "path":"/path/to/table",
       "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder",
       "table":{
         "type":"MERGE_ON_READ"
       }
     }
   }
}
```

## Hudi 写连接器

写入hudi表的用户配置:

```json
{
  "job": {
    "writer": {
      "hoodie": {
        "bucket": {
          "index": {
            "num": {
              "buckets": "4"
            },
            "hash": {
              "field": "id"
            }
          }
        },
        "datasource": {
          "write": {
            "recordkey": {
              "field": "id"
            }
          }
        },
        "table": {
          "name": "test_table"
        }
      },
      "path": "/path/to/table",
      "format_type": "json",
      "index": {
        "type": "BUCKET"
      },
      "class": "com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder",
      "write": {
        "operation": "upsert"
      },
      "table": {
        "type": "MERGE_ON_READ"
      },
      "source_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]",
      "sink_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]"
    }
  }
}
```

## Hudi compaction示例

实例参数用于压缩Hudi表:

```json
{
  "job":{
    "reader":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.source.HudiCompactSourceDAGBuilder"
    },
    "writer":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.sink.HudiCompactSinkDAGBuilder"
    }
  }
}
```
