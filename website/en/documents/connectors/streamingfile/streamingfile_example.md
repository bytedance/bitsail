# StreamingFile connector example

Parent document: [StreamingFile connector](./streamingfile.md)

```json
{
  "job": {
    "writer": {
      "hdfs": {
        "replication": 1,
        "dump_type": "hdfs.dump_type.json",
        "compression_codec": "none"
      },
      "dump": {
        "output_dir": "file:///tmp/streaming_file_hdfs/",
        "format": {
          "type": "hdfs"
        }
      },
      "class": "com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder",
      "partition_infos": "[{\"name\":\"date\",\"type\":\"TIME\"},{\"name\":\"hour\",\"type\":\"TIME\"}]",
      "enable_event_time": true,
      "event_time_fields": "timestamp"
    }
  }
}
```

## HIVE' Example (Static Partition)

### job writer's configuration

```json
{
  "job": {
    "writer": {
      "db_name": "default",
      "table_name": "bitsail_test_static",
      "hdfs": {
        "dump_type": "hdfs.dump_type.json"
      },
      "dump": {
        "format": {
          "type": "hive"
        }
      },
      "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}",
      "partition_infos": "[{\"name\":\"date\",\"type\":\"TIME\"},{\"name\":\"hour\",\"type\":\"TIME\"}]",
      "enable_event_time": "true",
      "event_time_fields": "timestamp",
      "class": "com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder",
      "source_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"bigint\"}]",
      "sink_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"bigint\"}]"
    }
  }
}
```

### job's example table ddl.

```hiveql
CREATE TABLE IF NOT EXISTS `default`.`bitsail_test_static`
(
    `id`        bigint,
    `text`      string,
    `timestamp` bigint
) PARTITIONED BY (`date` string, `hour` string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
```

## HIVE' Example (Dynamic Partition)

### job writer's configuration(dynamic)

```json
{
  "job": {
    "writer": {
      "db_name": "default",
      "table_name": "bitsail_test_dynamic",
      "hdfs": {
        "dump_type": "hdfs.dump_type.json"
      },
      "dump": {
        "format": {
          "type": "hive"
        }
      },
      "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}",
      "partition_infos": "[{\"name\":\"date\",\"type\":\"TIME\"},{\"name\":\"hour\",\"type\":\"TIME\"},{\"name\":\"app_name\",\"type\":\"DYNAMIC\"}]",
      "enable_event_time": "true",
      "event_time_fields": "timestamp",
      "class": "com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder",
      "source_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"bigint\"},{\"name\":\"app_name\",\"type\":\"string\"}]",
      "sink_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"bigint\"},{\"name\":\"app_name\",\"type\":\"string\"}]"
    }
  }
}
```

### job's example table ddl (dynamic).

```hiveql
CREATE TABLE IF NOT EXISTS `default`.`bitsail_test_dynamic`
(
    `id`        bigint,
    `text`      string,
    `timestamp` bigint
) PARTITIONED BY (`date` string, `hour` string, `app_name` string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
```