# fake连接器

上级文档: [connectors](../introduction_zh.md)

***BitSail*** fake连接器支持批式读，其支持的主要功能点如下

- 随机生成测试数据，支持unique,指定范围等特性

## 依赖引入

```xml

<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-connector-fake</artifactId>
    <version>${revision}</version>
</dependency>

```

## 数据类型

参考 `com.bytedance.bitsail.common.typeinfo.Types`
@Deprecated 未来会删除

- VOID
- SHORT
- INT
- LONG
- BIGINT 等同LONG
- DOUBLE
- BIGDECIMAL
- BIGINTEGER
- BYTE
- BINARY
- DATE
- TIME
- TIMESTAMP
- BOOLEAN
- STRING
- LIST
- MAP
- BYTES(@Deprecated) 等同BINARY
- date.date(@Deprecated)
- date.time(@Deprecated)
- date.datetime(@Deprecated)

## 特性与参数设置

| 参数名称            | 参数含义      | 参数默认值               | 参数枚举值 |  
|:----------------|:----------|:--------------------|:------|
| total_count     | 行总数       | 10000               |       |
| rate            | 速率限制      | 10                  |       |
| lower_limit     | 数字类型的下限   | 0                   |       |
| upper_limit     | 数字类型的上限   | 10000000            |       |
| from_timestamp  | 时间类型的下限   | 1970-01-01 00:00:00 |       |
| to_timestamp    | 时间类型的上限   | 2077-07-07 07:07:07 |       |
| NULL_PERCENTAGE | NULL数据的比率 | 0                   |       |

# 列的properties配置

| 参数名称     | 参数含义   | 默认值 | 
|----------|--------|:----|
| NULLABLE | 可为null |     | 
| NOT_NULL | 不为null |     | 
| UNIQUE   | 不重复唯一  |     |

### NULL_PERCENTAGE

如果一个列是允许null数据的，默认是NOT_NULL，那么按照参数比率随机生成null数据

# UNIQUE

LONG类型是雪花id,其他是结合rowNum生成的递增数据

## 相关文档

配置示例文档[fake-example_zh.md](fake-example_zh.md)





