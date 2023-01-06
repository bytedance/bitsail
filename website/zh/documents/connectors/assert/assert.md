# Assert 连接器

上级文档：[连接器](../README.md)

**BitSail** Assert 连接器可以根据用户自定义的规则验证数据的合法性。其功能点主要包括:

- 支持多种自定义校验规则

## 依赖引入

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-assert</artifactId>
    <version>${revision}</version>
    <scope>provided</scope>
</dependency>
```

## 支持的数据类型

- 支持的基础数据类型：
    - 整数类型：
        - tinyint
        - smallint
        - int
        - bigint
    - 浮点类型：
        - float
        - double
        - decimal
    - 时间类型：
        - timestamp
        - date
    - 字符类型：
        - string
        - varchar
        - char
    - 布尔类型：
        - boolean

### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.assertion.sink.AssertSink",
      "columns": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "price",
          "type": "double"
        }
      ],
      "row_rules": {
        "min_row": 10,
        "max_row": 20
      },
      "column_rules": {
        "name": {
          "not_null": true,
          "min_len": 1,
          "max_len": 1000
        },
        "price": {
          "not_null": true,
          "min": 2,
          "max": 180
        }
      }
    }
  }
}
```

#### 必需参数

| 参数名称    | 是否必填 | 参数枚举值 | 参数含义                                                                      |
|:--------|:-----|:------|:--------------------------------------------------------------------------|
| class   | 是    |       | Assert写连接器类型, `com.bytedance.bitsail.connector.assertion.sink.AssertSink` |
| columns | 是    |       | 指定写入的字段名和字段类型                                                             |

#### 可选参数

| 参数名称                   | 是否必填 | 参数枚举值 | 参数含义         |
|:-----------------------|:-----|:------|:-------------|
| writer_parallelism_num | 否    |       | 指定Assert写并发数 |
| row_rules              | 否    |       | 自定义行校验规则     |
| column_rules           | 否    |       | 自定义列校验规则     |


#### 校验规则

| 规则       | 含义      | 参数类型    |
|:---------|:--------|:--------|
| min_row  | 最小行数    | int     |
| max_row  | 最大行数    | int     |
| not_null | 是否非空    | boolean |
| min      | 数据的最小值  | double  |
| max      | 数据的最大值  | double  |
| min_len  | 字符串最小长度 | int     |
| max_len  | 字符串最大长度 | int     |


#### 说明

- 若声明了 `row_rules`，则 `Assert Sink` 的并行度会被强制设置为 `1`，自定义的 `writer_parallelism_num` 参数值会失效。

## 相关文档

配置示例文档：[Assert 连接器示例](./assert-example.md)
