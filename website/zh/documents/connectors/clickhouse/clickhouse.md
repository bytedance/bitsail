# ClickHouse 连接器

上级文档：[连接器](../README.md)

**BitSail** ClickHouse 连接器可用于读取 ClickHouse 中的数据，主要支持如下功能：

 - 支持批式读取 ClickHouse 表
 - 使用的 JDBC Driver 版本：0.3.2-patch11

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-clickhouse</artifactId>
   <version>${revision}</version>
</dependency>
```

## ClickHouse 读取

### 支持的数据类型

支持如下基础数据类型：

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

### 主要参数

读连接器参数在 `job.reader` 中配置，实际使用时请注意路径前缀。参数配置示例：

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

#### 必需参数

| 参数名称     | 是否必填 | 参数枚举值                                      | 参数含义                                 |
|:------------|:------|:-------------------------------------------------|:------------------------------|
| class       | 是    | `com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource` | ClickHouse 读连接器类型 |
| jdbc_url    | 是    |       | ClickHouse 的 JDBC 连接地址 |
| db_name     | 是    |       | 要读取的 ClickHouse 库 |
| table_name  | 是    |       | 要读取的 ClickHouse 表 |

<!--AGGREGATE<br/>DUPLICATE-->

#### 可选参数

| 参数名称       | 是否必填 | 参数枚举值 | 参数含义                                              |
|:-------------|:------|:-----------|:-----------------------------------------------------|
| user_name    | 否    |       | 访问 ClickHouse 服务的用户名 |
| password     | 否    |       | 上述用户的的密码 |
| split_field  | 否    |  | 分批查询的字段，仅支持 Int8 - Int64 和 UInt8 - UInt 32 整数类型  |
| split_config | 否    |  | 按照 `split_field` 字段进行批次查询时的配置，包括初始值、最大值和查询的次数，<p/> 如：`{"lower_bound": 0, "upper_bound": 10000, "split_num": 3}` |
| sql_filter   | 否    |  | 查询的过滤条件，比如 `( id % 2 == 0 )`，会拼接到查询 SQL 的 WHERE 子句中 |
| reader_parallelism_num | 否 |       | 读取 ClickHouse 表的并发                  |

## 相关文档

配置示例文档：[ClickHouse 连接器示例](./clickhouse-example.md)
