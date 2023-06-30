# Kudu 连接器

上级文档：[连接器](../README.md)

**BitSail** Kudu 连接器支持批式读写 Kudu 表。

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-kudu</artifactId>
   <version>${revision}</version>
</dependency>
```

-----

## Kudu读取

Kudu通过scanner扫描数据表，支持常见的Kudu数据类型:

- 整型: `int8, int16, int32, int64`'
- 浮点类型: `float, double, decimal`
- 布尔类型: `boolean`
- 日期类型: `date, timestamp`
- 字符类型: `string, varchar`
- 二进制类型: `binary, string_utf8`

### 主要参数

读连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.kudu.source.KuduSource",
      "kudu_table_name": "kudu_test_table",
      "kudu_master_address_list": ["localhost:1234", "localhost:4321"]
    }
  }
}
```

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | Kudu读连接器类型, `com.bytedance.bitsail.connector.kudu.source.KuduSource` |
| kudu_table_name | 是 | | 要读取的Kudu表 |
| kudu_master_address_list | 是 | | Kudu master地址, List形式表示 |
| columns | 是 | | 要读取的数据列的列名和类型 |
| reader_parallelism_num | 否 | | 读并发 |


#### KuduClient相关参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| kudu_admin_operation_timeout_ms | 否 | | Kudu client进行admin操作的timeout, 单位ms, 默认30000ms |
| kudu_operation_timeout_ms | 否 | | Kudu client普通操作的timeout, 单位ms, 默认30000ms |
| kudu_connection_negotiation_timeout_ms | 否  | |  单位ms，默认10000ms |
| kudu_disable_client_statistics | 否  | | 是否启用client段statistics统计 |
| kudu_worker_count | 否 | | client内worker数量 | 
| sasl_protocol_name | 否 | | 默认 "kudu" |
| require_authentication | 否  | | 是否开启鉴权 |
| encryption_policy | 否 | OPTIONAL<br/>REQUIRED_REMOTE<br/>REQUIRED | 加密策略 | 


#### KuduScanner相关参数
| 参数名称              | 是否必填                          | 参数枚举值 | 参数含义                      |
|:------------------|:------------------------------|:------|:--------------------------|
| read_mode | 否                             | READ_LATEST<br/>READ_AT_SNAPSHOT | 读取模式                      |
| snapshot_timestamp_us | read_mode=READ_AT_SNAPSHOT时必需 |  | 指定要读取哪个时间点的snapshot       |
| enable_fault_tolerant | 否                             | | 是否允许fault tolerant        |
| scan_batch_size_bytes | 否                             | | 单batch内拉取的最大数据量           |
| scan_max_count | 否                             | | 最多拉取多少条数据                 |
| enable_cache_blocks| 否                             |  | 是否启用cache blocks, 默认true  |
| scan_timeout_ms | 否                             | | scan超时时间, 单位ms, 默认30000ms |
| scan_keep_alive_period_ms | 否                             | |                           |
| predicates | 否                              | | predicate json 字符串        |

#### predicates 参数说明

列上的查询谓词。与传统的 SQL 语法不同，
简单查询谓词以简单的 JSON 表示
句法。支持三种类型的谓词，包括 'Comparison',
'InList' and 'IsNull'.
* 'Comparison' 支持 <=, <, =, > 以及 >=,
  格式为'[operator, column_name, value]',

  `举例. '[">=", "col1", "value"]'`
* 'InList' 格式 '["IN", column_name, [value1, value2, ...]]'

  `举例. '["IN", "col2", ["value1", "value2"]]'`
* 'IsNull' 表示目标列是否可以为 NULL 或者不可以是 NULL, 格式'[operator, column_name]'

  `举例. '["NULL", "col1"]', or '["NOTNULL", "col2"]'`

谓词可以用如下的方式进行组合
`[operator, predicate, predicate, ..., predicate].`
举例如下,
`["AND", [">=", "col1", "value"], ["NOTNULL", "col2"]]`
目前只支持 `AND` 组合方式
-----

## Kudu写入

### 支持的数据类型

支持写入常见的Kudu数据类型:

- 整型: `int8, int16, int32, int64`'
- 浮点类型: `float, double, decimal`
- 布尔类型: `boolean`
- 日期类型: `date, timestamp`
- 字符类型: `string, varchar`
- 二进制类型: `binary, string_utf8`

### 支持的操作类型

支持以下操作类型:
 - INSERT, INSERT_IGNORE
 - UPSERT
 - UPDATE, UPDATE_IGNORE


### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.kudu.sink.KuduSink",
      "kudu_table_name": "kudu_test_table",
      "kudu_master_address_list": ["localhost:1234", "localhost:4321"],
      "kudu_worker_count": 2
    }
  }
}
```


#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | Kudu写连接器类型, `com.bytedance.bitsail.connector.kudu.sink.KuduSink` |
| kudu_table_name | 是 | | 要写入的Kudu表 |
| kudu_master_address_list | 是 | | Kudu master地址, List形式表示 |
| columns | 是 | | 要写入的数据列的列名和类型 |
| writer_parallelism_num | 否 | | 写并发 |

#### KuduClient相关参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| kudu_admin_operation_timeout_ms | 否 | | Kudu client进行admin操作的timeout, 单位ms, 默认30000ms |
| kudu_operation_timeout_ms | 否 | | Kudu client普通操作的timeout, 单位ms, 默认30000ms |
| kudu_connection_negotiation_timeout_ms | 否  | |  单位ms，默认10000ms |
| kudu_disable_client_statistics | 否  | | 是否启用client段statistics统计 |
| kudu_worker_count | 否 | | client内worker数量 | 
| sasl_protocol_name | 否 | | 默认 "kudu" |
| require_authentication | 否  | | 是否开启鉴权 |
| encryption_policy | 否 | OPTIONAL<br/>REQUIRED_REMOTE<br/>REQUIRED | 加密策略 | 

#### KuduSession相关参数
| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| kudu_session_flush_mode | 否 | AUTO_FLUSH_SYNC<br/>AUTO_FLUSH_BACKGROUND | session的flush模式, 默认AUTO_FLUSH_BACKGROUND |
| kudu_mutation_buffer_size | 否 | | session最多能缓存多少条operation记录 |
| kudu_session_flush_interval | 否 | | session的flush间隔，单位ms | 
| kudu_session_timeout_ms | 否 | | session的operation超时 |
| kudu_session_external_consistency_mode | 否 | CLIENT_PROPAGATED<br/>COMMIT_WAIT | 默认CLIENT_PROPAGATED |
| kudu_ignore_duplicate_rows | 否 | | 是否忽略因duplicate key造成的error, 默认false |

## 相关文档

配置示例文档：[Kudu 连接器示例](./kudu-example.md)
