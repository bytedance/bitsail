# Doris 连接器

上级文档：[连接器](../README.md)

**BitSail** Doris连接器支持批式和流式写doris，其支持的主要功能点如下
 
 - 使用StreamLoad方式写Doris表
 - 支持分区创建再写入
 - 支持多种table mode


## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-doris</artifactId>
   <version>${revision}</version>
</dependency>
```

## Doris写入

### 支持的数据类型

写连接器使用 JSON 或者 CSV 格式传输数据，支持常见的 Doris 数据类型：

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

### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

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

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | Doris写连接器类型, `com.bytedance.bitsail.connector.doris.sink.DorisSink` |
| fe_hosts   | 是  |       | Doris FE地址, 多个地址用逗号分隔 |
| mysql_hosts        | 是  |       | JDBC连接Doris的地址, 多个地址用逗号分隔 |
| user| 是 | | Doris账户 |
| password| 是 | | Doris密码，可为空 |
| db_name| 是 | | 要写入的doris库 |
| table_name| 是 | | 要写入的doris表 |
| partitions | 分区表必需 | | 要写入的分区 |
| table_has_partition | 非分区表必需 | | 非分区表填写true |


<!--AGGREGATE<br/>DUPLICATE-->

注意，partitions格式要求如下:
 1. 可写入多个partition，每个partition
 2. 每个partition内需要有:
    1. `name`: 要写入的分区名
    2. `start_range`, `end_range`: 该分区范围

partitions示例:
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

#### 可选参数

| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| writer_parallelism_num | 否 |       | 指定Doris写并发                       |
| sink_flush_interval_ms | 否 | | Upsert模式下的flush间隔, 默认5000 ms |
| sink_max_retries | 否 | | 写入的最大重试次数，默认3 |
| sink_buffer_size | 否  | | 写入buffer最大值，默认 20971520 bytes (20MB) |
| sink_buffer_count | 否 | | 写入buffer的最大条数，默认100000 | 
| sink_enable_delete | 否 | | 是否支持delete事件同步 |
| sink_write_mode | 否 | 目前只支持以下几种:<br/>STREAMING_UPSERT<br/>BATCH_UPSERT<br/>BATCH_REPLACE | 写入模式 |
| stream_load_properties | 否 | | 追加在streamload url后的参数，map<string,string>格式 |
| load_contend_type | 否 | csv<br/>json | streamload使用的格式，默认json |
| csv_field_delimiter | 否 | | csv格式的行内分隔符, 默认逗号 "," |
| csv_line_delimiter | 否 | | csv格式的行间分隔符, 默认 "\n" |


sink_write_mode 参数可选值：
 - STREAMING_UPSERT: 流式upsert写入
 - BATCH_UPSERT: 批式upsert写入
 - BATCH_REPLACE: 批式replace写入

## 相关文档

配置示例文档：[Doris 连接器示例](./doris-example.md)
