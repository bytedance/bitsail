# Kudu连接器

上级文档: [connectors](../introduction_zh.md)

***BitSail*** Kudu连接器支持批式读写Kudu表。

## 依赖引入

```text
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-kudu</artifactId>
   <version>${revision}</version>
</dependency>
```

## Kudu读取

Kudu通过scanner扫描数据表，支持常见的Kudu数据类型:

 - 整型: `int8, int16, int32, int64`'
 - 浮点类型: `float, double, decimal`
 - 布尔类型: `boolean`
 - 日期类型: `date, timestamp`
 - 字符类型: `string, varchar`
 - 二进制类型: `binary, string_utf8`

### 主要参数

写连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

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
| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| read_mode | 否 | READ_LATEST<br/>READ_AT_SNAPSHOT | 读取模式 |
