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

## Doris读取
### 支持的数据类型
Doris读连接器根据字段映射进行解析，支持以下数据类型:

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

### 主要参数
写连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:
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
#### 必需参数
| 参数名称        | 是否必填   | 默认值 | 参数含义                                                                    |
|:------------|:-------|:----|:------------------------------------------------------------------------|
| class       | 是      | --  | Doris读连接器类型, `com.bytedance.bitsail.connector.doris.source.DorisSource` |
| fe_hosts    | 是      | --  | Doris FE地址, 多个地址用逗号分隔                                                   |
| mysql_hosts | 是      | --  | JDBC连接Doris的地址, 多个地址用逗号分隔                                               |
| user        | 是      | --  | Doris账户名                                                                |
| password    | 是      | --  | Doris密码，可为空                                                             |
| db_name     | 是      | --  | 要读取的Doris库                                                              |
| table_name  | 是      | --  | 要读取的Doris表                                                              |
| columns     | 是      | --  | 要读取的数据列的列名和类型                                                           |

#### 可选参数
| 参数名称                          | 是否必填 | 默认值               | 参数含义                                                                                                |
|:------------------------------|:-----|:------------------|:----------------------------------------------------------------------------------------------------|
| reader_parallelism_num        | 否    | 1                 | 指定doris读并发                                                                                          |
| sql_filter                    | 否    | --                | 需要查询过滤的列值条件                                                                                         |
| tablet_size                   | 否    | Integer.MAX_VALUE | 一个 Partition 对应的 Doris Tablet 个数。 此数值设置越小，则会生成越多的 Partition。从而提升 bitSail 侧的并行度，但同时会对 Doris 造成更大的压力。 |
| exec_mem_limit                | 否    | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                                                                             |
| request_query_timeout_s       | 否    | 3600              | 查询 Doris 的超时时间，默认值为1小时，-1表示无超时限制                                                                    |
| request_batch_size            | 否    | 1024              | 一次从 Doris BE 读取数据的最大行数。增大此数值可减少 bitSail 与 Doris 之间建立连接的次数。 从而减轻网络延迟所带来的额外时间开销                       |
| request_connect_timeouts      | 否    | 30 * 1000         | 向 Doris 发送请求的连接超时时间                                                                                 |
| request_read_timeouts         | 否    | 30 * 1000         | 向 Doris 发送请求的读取超时时间                                                                                 |
| request_retries               | 否    | 3                 | 向 Doris 发送请求的重试次数                                                                                   |



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
