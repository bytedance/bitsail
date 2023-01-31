# SelectDB connector

Parent document: [connectors](../README.md)

The SelectDB connector supports batch writing of data to the SelectDB cloud data warehouse, and provides flexible
writing request construction.

## Maven dependency

```xml

<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-connector-selectdb</artifactId>
    <version>${revision}</version>
</dependency>
```

## SelectDB writer

### Supported data type

Selectdb write connector uses json or csv format to transfer data, and the supported data types are:

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

Write connector parameters are configured in `job.writer`, please pay attention to the path prefix when actually using
it, for example:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.selectdb.sink.SelectdbSink",
      "cluster_name": "test_cluster",
      "table_identifier": "test_db.test_select_table"
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required  | 	Description                                                                               |
|:-----------------------------|:----------|:-------------------------------------------------------------------------------------------|
| class                        | yes       | Selectdb writer class name,, `com.bytedance.bitsail.connector.selectdb.sink.SelectdbSink`  |
| load_url                     | yes       | Selectdb HTTP upload address                                                               |
| jdbc_url                     | yes       | Selectdb JDBC query address                                                                |
| cluster_name                 | yes       | Selectdb cluster name                                                                      |
| user                         | yes       | Selectdb account user                                                                      |
| password                     | yes       | Selectdb account password                                                                  |
| table_identifier             | yes       | Write to the database table of Selectdb，like：test_db.test_select_table                     |

#### Optional parameters

| Param name                    | Required  | Optional value                        | Description                                                                                    |
|:------------------------------|:----------|:--------------------------------------|:-----------------------------------------------------------------------------------------------|
| writer_parallelism_num        | no        |                                       | Writer parallelism num                                                                         |
| sink_flush_interval_ms        | no        |                                       | Flush interval in upsert mode, default 5000 ms                                                 |
| sink_max_retries              | no        |                                       | Max retry times, default 3                                                                     |
| sink_buffer_size              | no        |                                       | Max size of buffer, default 1048576 bytes (1MB)                                                |
| sink_buffer_count             | no        |                                       | Max number of records can be buffered, default 3                                               | 
| sink_enable_delete            | no        |                                       | enable delete or no                                                                            |
| sink_write_mode               | no        | Currently only supported BATCH_UPSERT | Write mode                                                                                     |
| stream_load_properties        | no        |                                       | Stream load parameters that will be append to the stream load url. Format is standard json map |
| load_contend_type             | no        | csv<br/>json                          | Content format of streamload, default json                                                     |
| csv_field_delimiter           | no        |                                       | field delimiter used in csv, default ","                                                       |
| csv_line_delimiter            | no        |                                       | line delimiter used in csv, default "\n"                                                       |

## Related documents

Configuration examples: [selectdb-connector-example](./selectdb-example.md)

SelectDB Cloud: [selectdb](https://cn.selectdb.com/)
