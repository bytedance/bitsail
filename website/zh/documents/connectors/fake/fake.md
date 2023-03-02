# Fake连接器

上级文档: [connectors](../README.md)

***BitSail*** Fake是一个读连接器，你指定一些列的name、type后，Fake连接器会为你生成指定数量的数据行；他是功能测试的好帮手，通常也是BitSail新手的HelloWorld程序的读连接器。

- 随机生成测试数据，支持unique,指定范围等特性

## 依赖引入

```xml

<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-connector-fake</artifactId>
    <version>${revision}</version>
</dependency>
```

## Fake读取

### 支持数据类型

- 基本类型:
	- 整数类型:
		- short
		- int
		- long
		- biginteger
	- 浮点类型::
		- float
		- double
		- bigdecimal
	- 时间类型:
		- time
		- timestamp
		- date
		- date.date
		- date.time
		- date.datetime
	- 字符类型:
		- string
	- 布尔类型:
		- 不支持
	- 二进制类型:
		- binary
		- bytes

### 主要参数

写连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.fake.source.FakeSource",
      "total_count": 300,
      "rate": 100,
      "random_null_rate": 0.1,
      "columns": [
        {
          "name": "id",
          "type": "long",
          "properties": "unique"
        },
        {
          "name": "id",
          "type": "date"
        },
        {
          "name": "list_value",
          "type": "list<string>"
        },
        {
          "name": "map_value",
          "type": "map<string,string>"
        },
        {
          "name": "local_datetime_value",
          "type": "timestamp"
        },
        {
          "name": "date_value",
          "type": "date.date"
        },
        {
          "name": "datetime_value",
          "type": "date.datetime"
        }
      ]
    }
  }
}
```

#### 必需参数

| 参数名称        | 是否必填 | 参数枚举值 | 参数含义                                                                 |
|:------------|:-----|:------|:---------------------------------------------------------------------|
| class       | 是    |       | Fake读连接器类型, `com.bytedance.bitsail.connector.fake.source.FakeSource` |
| total_count | 是    |       | 生成数据的总条数                                                             |

#### 可选参数

| 参数名称            | 是否必填      | 参数默认值               | 参数含义                                                        |
|:----------------|:----------|:--------------------|:------------------------------------------------------------|
| rate            | 否         | 10                  | 产生数据的频率,数值越大，单位时间生成的数据越多                                    |
| lower_limit     | 否         | 10                  | 与upper_limit一起作为生成 float,double,bigdecimal 类型字段的种子,并非字段值的边界 |
| upper_limit     | 否         | 2077-07-07 07:07:07 | 与lower_limit一起作为生成 float,double,bigdecimal 类型字段的种子,并非字段值的边界 |
| from_timestamp  | 否         | 1970-01-01 00:00:00 | 与 to_timestamp 一起作为生成时间类型字段的种子,并非字段值的边界                     |
| to_timestamp    | 否         |                     | 与 from_timestamp 一起作为生成时间类型字段的种子,并非字段值的边界                   |
| NULL_PERCENTAGE | NULL数据的比率 | 0                   |                                                             |

# 列的properties配置

| 参数名称     | 参数含义   | 默认值 | 
|----------|--------|:----|
| NULLABLE | 可为null |     | 
| NOT_NULL | 不为null |     | 
| UNIQUE   | 不重复唯一  |     |

###### NULL_PERCENTAGE

如果一个列是允许null数据的，默认是NOT_NULL，那么按照参数比率随机生成null数据

##### UNIQUE

LONG类型是雪花id,其他是结合rowNum生成的递增数据

## 相关文档

配置示例文档: [fake-connector-example](./fake-example.md)
