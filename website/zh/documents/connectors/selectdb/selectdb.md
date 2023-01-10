# SelectDB 连接器

上级文档: [connectors](../README.md)

SelectDB连接器支持批式往SelectDB云数仓写数据，并提供灵活地写入请求构建。

## 依赖引入

```xml

<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-connector-selectdb</artifactId>
    <version>${revision}</version>
</dependency>
```

## SelectDB写入

### 支持的数据类型

Selectdb 写连接器使用json或者csv格式传输数据，支持的数据类型有:

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
      "class": "com.bytedance.bitsail.connector.selectdb.sink.SelectdbSink",
      "cluster_name": "test_cluster",
      "table_identifier": "test_db.test_select_table"
    }
  }
}
```

#### 必需参数

| 参数名称                    | 是否必填 | 参数含义                                                                         |
|:------------------------|:-----|:-----------------------------------------------------------------------------|
| class                   | 是    | Selectdb写连接器类型, `com.bytedance.bitsail.connector.selectdb.sink.SelectdbSink` |
| load_url                | 是    | Selectdb上传数据的HTTP URL                                                        |
| jdbc_url                | 是    | JDBC连接Selectdb的地址                                                            |
| cluster_name            | 是    | Selectdb cluster 的名称                                                         |
| user                    | 是    | Selectdb账户                                                                   |
| password                | 是    | Selectdb密码                                                                   |
| table_identifier        | 是    | 要写入Selectdb的库表，例如：test_db.test_select_table                                  |

#### 可选参数

| 参数名称                                    | 是否必填  | 参数枚举值             | 参数含义                                       |
|:----------------------------------------|:------|:------------------|:-------------------------------------------|
| writer_parallelism_num | 否 |                   | 指定Selectdb写并发                              |
| sink_flush_interval_ms | 否 |                   | Upsert模式下的flush间隔, 默认5000 ms               |
| sink_max_retries | 否 |                   | 写入的最大重试次数，默认3                              |
| sink_buffer_size | 否  |                   | 写入buffer最大值，默认 1048576 bytes (1MB)         |
| sink_buffer_count | 否 |                   | 初始化 buffer 的数量，默认为3                        | 
| sink_enable_delete | 否 |                   | 是否支持delete事件同步                             |
| sink_write_mode | 否 | 目前仅支持BATCH_UPSERT | 写入模式                                       |
| stream_load_properties | 否 |                   | 追加在streamload url后的参数，map<string,string>格式 |
| load_contend_type | 否 | csv<br/>json      | copy-into使用的格式，默认json                      |
| csv_field_delimiter | 否 |                   | csv格式的行内分隔符, 默认逗号 ","                      |
| csv_line_delimiter | 否 |                   | csv格式的行间分隔符, 默认 "\n"                       |

## 相关文档

配置示例文档: [selectdb-connector-example](./selectdb-example.md)

SelectDB Cloud: [selectdb](https://cn.selectdb.com/)
