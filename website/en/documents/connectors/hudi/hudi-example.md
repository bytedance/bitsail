# Hudi connector example

Parent document: [Hudi connector](./hudi.md)

## Hudi reader example

Configuration for reading the test hudi table:

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

## Hudi writer example

Configuration for writing the test hudi table:

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

## Hudi compaction example

Configuration for compacting the test hudi table:

```json
{
  "job":{
    "reader":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder"
    },
    "writer":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder"
    }
  }
}
```