# Doris connector

Parent document: [Connectors](../README.md)

**BitSail** Doris connector supports writing doris. The main function points are as follows:

- Use StreamLoad to write doris.
- Support firstly create and then write partition

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-doris</artifactId>
   <version>${revision}</version>
</dependency>
```

## Doris Writer

### Supported data type

Doris writer uses stream load, and the content type can be csv or json.
It supports common data type in doris:

- CHAR
- VARCHAR
- TEXT
- BOOLEAN
- BINARY
- VARBINARY
- DECIMAL
- DECIMALV2
- INT
- TINYINT
- SMALLINT
- INTEGER
- INTERVAL_YEAR_MONTH
- INTERVAL_DAY_TIME
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE
- DATE
- DATETIME

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.doris.sink.DorisSink",
      "db_name": "test_db",
      "table_name": "test_doris_table"
    }
  }
}
```

#### Necessary parameters

| Param name          | Required | Optional value | Description                                                        |
|:--------------------|:---------|:---------------|:-------------------------------------------------------------------|
| class               | yes  |       | Doris writer class name, `com.bytedance.bitsail.connector.doris.sink.DorisSink` |
| fe_hosts            | yes  |       | Doris FE address, multi addresses separated by comma |
| mysql_hosts         | yes  |       | Doris jdbc query address , multi addresses separated by comma |
| user                | yes  |  | Doris account user |
| password            | yes  |  | Doris account password, can be empty  |
| db_name             | yes  |  | database to write |
| table_name          | yes  |  | table to write |
| partitions          | Yes if target table has partition | | target partition to write |
| table_has_partition | Yes if target table does not have partition | | True if target table does not have partition  |

<!--AGGREGATE<br/>DUPLICATE-->

Notice, `partitions` has the following requirements:
 1. You can determine multi partitions
 2. Each partition should contain:
    1. `name`: name of the partition
    2. `start_range`, `end_range`: left and right range of the partition

partitions example:
```json
{
  "partitions": [
    {
      "name": "p20220210_03",
      "start_range": [
        "2022-02-10",
        "3"
      ],
      "end_range": [
        "2022-02-10",
        "4"
      ]
    },
    {
      "name": "p20220211_03",
      "start_range": [
        "2022-02-11",
        "3"
      ],
      "end_range": [
        "2022-02-11",
        "4"
      ]
    }
  ]
}
```

#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num | no |       | Writer parallelism num   |
| sink_flush_interval_ms | no | | Flush interval in upsert mode, default 5000 ms |
| sink_max_retries | no | | Max retry times, default 3 |
| sink_buffer_size | no  | | Max size of buffer, default 20971520 bytes (20MB) |
| sink_buffer_count | no | | Max number of records can be buffered, default 100000 |
| sink_write_mode | no | STREAMING_UPSERT<br/>BATCH_UPSERT<br/>BATCH_REPLACE | Write mode. |
| stream_load_properties | no | | Stream load parameters that will be append to the stream load url. Format is standard json map. |
| load_contend_type | no | csv<br/>json | Content format of streamload, default json |
| csv_field_delimiter | no | | field delimiter used in csv, default "," |
| csv_line_delimiter | no | | line delimiter used in csv, default "\n" |

## Related documents

Configuration examples: [Doris connector example](./doris-example.md)
