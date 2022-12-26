# Hudi 连接器

上级文档：[连接器](../README.md)

**BitSail** Hudi 连接器支持读写 Hudi 表，主要功能如下

 - 支持流式写入Hudi表。
 - 支持批式写入Hudi表。
 - 支持批式读取Hudi表。

## 支持Hudi版本

- 0.11.1

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hudi</artifactId>
   <version>${revision}</version>
</dependency>
```

## Hudi读取

### 支持数据类型

- 支持的基础数据类型如下:
    - 整数类型:
        - tinyint
        - smallint
        - int
        - bigint
    - 浮点类型:
        - float
        - double
        - decimal
    - 时间类型:
        - timestamp
        - date
    - 字符类型:
        - string
        - varchar
        - char
    - 布尔类型:
        - boolean
    - 二进制类型:
        - binary
- 支持的复杂数据类型包括:
    - map
    - array

### 主要参数

读连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "reader":{
      "hoodie":{
        "datasource":{
          "query":{
            "type":"snapshot"
          }
        }
      },
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder",
      "table":{
        "type":"MERGE_ON_READ"
      }
    }
  }
}
```

#### 必需参数

| 参数名称                         | 是否必填 | 参数枚举值 | 参数描述                                                                                       |
|:-----------------------------|:-----|:------|:-------------------------------------------------------------------------------------------|
| class                        | Yes  |       | Hudi读连接器类名, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder` |
| path                         | Yes  |       | 表的路径，可以是HDFS，S3，或者其他文件系统。                                                                  |
| table.type                   | Yes  |       | Hudi表的类型，可以是 `MERGE_ON_READ` 或者 `COPY_ON_WRITE`                                            |
| hoodie.datasource.query.type | Yes  |       | 查询类型，可以是 `snapshot` 最新视图 或者 `read_optimized` 读优化视图                                         | 


#### 可选参数

| 参数名称                   | 是否必填 | 参数枚举值 | 参数描述  |
|:-----------------------|:-----|:------|:------|
| reader_parallelism_num | No   |       | 读取并发度 |

## Hudi写入

### 支持数据类型
- 支持的基础数据类型如下:
    - 整数类型:
        - tinyint
        - smallint
        - int
        - bigint
    - 浮点类型:
        - float
        - double
        - decimal
    - 时间类型:
        - timestamp
        - date
    - 字符类型:
        - string
        - varchar
        - char
    - 布尔类型:
        - boolean
    - 二进制类型:
        - binary
- 支持的复杂数据类型包括:
    - map
    - array

### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "hoodie": {
        "bucket": {
          "index": {
            "num": {
              "buckets": "4"
            },
            "hash": {
              "field": "id"
            }
          }
        },
        "datasource": {
          "write": {
            "recordkey": {
              "field": "id"
            }
          }
        },
        "table": {
          "name": "test_table"
        }
      },
      "path": "/path/to/table",
      "index": {
        "type": "BUCKET"
      },
      "class": "com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder",
      "write": {
        "operation": "upsert"
      },
      "table": {
        "type": "MERGE_ON_READ"
      },
      "source_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]",
      "sink_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]"
    }
  }
}
```

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | Yes  |       | Hudi写连接器类型, `com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder` |
| write.operation   | Yes  |       | `upsert` `insert` `bulk_insert`                                                           |
| table.type        | Yes  |       | Hudi表类型，`MERGE_ON_READ`或者 `COPY_ON_WRITE`                                                 |
| path              | Yes  |       | 表的路径，可以是HDFS，S3，或者其他文件系统。 如果该路径没有Hudi表，则会创建一张新表。                                          |
| format_type       | Yes  |       | 输入的数据类型，当前支持 `json`                                                                       |
| source_schema     | Yes  |       | 用于解析输入数据的字段类型。                                                                            |
| sink_schema       | Yes  |       | 用于写入Hudi表的字段类型。                                                                           |
| hoodie.table.name | Yes  |       | Hudi表的名字。                                                                                 |


#### 可选参数

如需了解更多Hudi的高级参数, 请查看代码 `FlinkOptions.java`

| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| hoodie.datasource.write.recordkey.field | false |       | 对于 `upsert` 操作, 此参数用于指定主键字段.                         |
| index.type                              | false |       | 对于 `upsert` 操作, 此参数用于指定索引类型. 可以是 `STATE` 或者 `BUCKET` |
| hoodie.bucket.index.num.buckets         | false |       | 如果我们使用了BUCKET索引, 我们需要指定桶的数量。                         |
| hoodie.bucket.index.hash.field          | false |       | 如果我们使用了BUCKET索引, 我们需要指定用于计算桶ID的字段。                   |

## Hudi压缩

### 主要参数

Compaction参数包含了reader和writer部分。

```json
{
  "job":{
    "reader":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder"
    },
    "writer":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder"
    }
  }
}
```

#### 必填参数

| Param name       | Required | Optional value | Description                                                                                |
|:-----------------|:---------|:---------------|:-------------------------------------------------------------------------------------------|
| job.reader.class | Yes      |                | Hudi压缩读取器类名, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder` |
| job.writer.class | Yes      |                | Hudi压缩写入器类名, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder`   |
| job.reader.path  | Yes      |                | 表的路径，可以是HDFS，S3，或者其他文件系统。                                                                  |
| job.writer.path  | Yes      |                | 表的路径，可以是HDFS，S3，或者其他文件系统。                                                                  |


#### 选填参数

| Param name             | Required | Optional value | Description |
|:-----------------------|:---------|:---------------|:------------|
| writer_parallelism_num | No       |                | 执行压缩的并发度    |


## 相关文档

配置示例文档：[Hudi 连接器示例](./hudi-example.md)
