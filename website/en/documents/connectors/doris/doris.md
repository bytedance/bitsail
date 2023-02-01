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

## Doris Reader
### Supported data types
Doris read connector parses according to the data segment mapping and supports the following data types:

- CHAR
- VARCHAR
- BOOLEAN
- BINARY
- VARBINARY
- INT
- TINYINT
- SMALLINT
- INTEGER
- BIGINT
- FLOAT
- DOUBLE

### Parameters
The following mentioned parameters should be added to `job.reader` block when using, for example:
```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.doris.source.DorisSource",
      "fe_hosts": "127.0.0.1:8030",
      "mysql_hosts": "127.0.0.1:9030",
      "user": "root",
      "password": "",
      "db_name": "test",
      "table_name": "test_doris_table"
    }
  }
}
```

#### Necessary parameters
| Param name      | Required | Default Value | Description                                                                         |
|:----------------|:---------|:--------------|:------------------------------------------------------------------------------------|
| class           | yes  | --            | Doris writer class name, `com.bytedance.bitsail.connector.doris.source.DorisSource` |
| fe_hosts        | yes  | --            | Doris FE address, multi addresses separated by comma                                |
| mysql_hosts     | yes  | --            | Doris jdbc query address , multi addresses separated by comma                       |
| user            | yes  | --            | Doris account user                                                                  |
| password        | yes  | --            | Doris account password, can be empty                                                |
| db_name         | yes  | --            | database to read                                                                    |
| table_name      | yes  | --            | table to read                                                                       |
| columns         | yes  | --            | The name and type of columns to read                                                | 

#### Optional parameters
| Param name                    | Required | Default Value     | Description                                                                                                                                                                                                                                         |
|:------------------------------|:---------|:------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| reader_parallelism_num        | no       | 1                 | reader parallelism                                                                                                                                                                                                                                  |
| sql_filter                    | no       | --                | Column value conditions that need to be queried and filtered                                                                                                                                                                                        |
| tablet_size                   | no       | Integer.MAX_VALUE | The number of Doris Tablets corresponding to an Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the bitSail side, but at the same time will cause greater pressure on Doris. |
| exec_mem_limit                | no       | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                                                                                                                                                                                      |
| request_query_timeout_s       | no       | 3600              | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit                                                                                                                                                                   |
| request_batch_size            | no       | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between bitSail and Doris. Thereby reducing the extra time overhead caused by network delay.                                |
| request_connect_timeouts      | no       | 30 * 1000         | Connection timeout for sending requests to Doris                                                                                                                                                                                                    |
| request_read_timeouts         | no       | 30 * 1000         | Read timeout for sending request to Doris                                                                                                                                                                                                           |
| request_retries               | no       | 3                 | Number of retries to send requests to Doris                                                                                                                                                                                                         |


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
