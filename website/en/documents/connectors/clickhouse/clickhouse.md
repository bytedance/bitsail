# ClickHouse connector

Parent document: [Connectors](../README.md)

**BitSail** ClickHouse connector can be used to read data in ClickHouse, mainly supports the following functions:

- Support batch reading of ClickHouse tables
- JDBC Driver version: 0.3.2-patch11

## Maven dependency

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-clickhouse</artifactId>
    <version>${revision}</version>
</dependency>
```

## ClickHouse reader

### Supported data types

The following basic data types are supported:

- Int8
- Int16
- Int32
- Int64
- UInt8
- UInt16
- UInt32
- UInt64
- Float32
- Float64
- Decimal
- Date
- String

### Parameters

Read connector parameters are configured in `job.reader`, please pay attention to the path prefix when actually using it. Example of parameter configuration:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource",
      "jdbc_url": "jdbc:clickhouse://127.0.0.1:8123",
      "user_name": "default",
      "password": "1234567",
      "db_name": "default",
      "table_name": "test_ch_table",
      "split_field": "id",
      "split_config": "{\"lower_bound\": 0, \"upper_bound\": 10000, \"split_num\": 3}",
      "sql_filter": "( id % 2 == 0 )"
    }
  }
}
```

#### Required parameters

| Parameter name | Required | Optional value                                   | Description      |
|:---------------|:---------|:-------------------------------------------------|:------------------------------|
| class          | yes      | `com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource` | ClickHouse read connector type |
| jdbc_url       | yes      |   | JDBC connection address of ClickHouse |
| db_name        | yes      |   | ClickHouse library to read |
| table_name     | yes      |   | ClickHouse table to read |

<!--AGGREGATE<br/>DUPLICATE-->

#### Optional parameters

| Parameter name     | Required | Optional value | Description                                        |
|:-------------------|:---------|:---------------|:---------------------------------------------------|
| user_name          | no       |                | Username to access ClickHouse services |
| password           | no       |       | The password of the above user  |
| split_field        | no       |       | Batch query fields, only support Int8 - Int64 and UInt8 - UInt32 integer types |
| split_config       | no       |       | The configuration for batch query according to `split_field` field, including initial value, maximum value and query times, <p/> For example: `{"lower_bound": 0, "upper_bound": 10000, "split_num": 3}` |
| sql_filter         | no       |       | The filter condition of the query, such as `( id % 2 == 0 )`, will be spliced into the WHERE clause of the query SQL |
| reader_parallelism_num | no   |       | ClickHouse reader parallelism num |

## Related documents

Configuration example: [ClickHouse connector example](./clickhouse-example.md)
