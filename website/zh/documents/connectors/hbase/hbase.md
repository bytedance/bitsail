# HBase 连接器

上级文档：[连接器](../README.md)

**BitSail** HBase连接器可用于支持读写HBase，支持的主要功能如下:

- 支持scan方式读取 HBase table 中的数据
- 支持在写 HBase table 时根据列数据设置 RowKey


## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hbase</artifactId>
   <version>${revision}</version>
</dependency>
```

## HBase读连接器

### 支持数据类型

支持将HBase中的binary数据转化为如下格式：

- string
- boolean
- int
- short
- long
- bigint
- double
- float
- date
- bytes



### 参数

读连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.hbase.source.HBaseInputFormat",
      "table": "test_table",
      "hbase_conf":{
        "hbase.zookeeper.quorum":"127.0.0.1",
        "hbase.zookeeper.property.clientPort":"2181",
        "hbase.mapreduce.splittable": "test_table"
      },
      "columns": [
        {
          "index": 0,
          "name": "cf1:str1",
          "type": "bigint"
        },
        {
          "index": 1,
          "name": "cf1:int1",
          "type": "string"
        }
      ]
    }
  }
}
```

#### 必需参数

| 参数名称                  | 是否必须 | 参数枚举值 | 参数描述                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class                        | 是      |                | HBase读连接器名, `com.bytedance.bitsail.connector.legacy.hbase.source.HBaseInputFormat` |
| table | 是 |  | 要读取的HBase表名 | 
| columns | 是 | | 描述字段名称和字段类型。字段名格式为: `columnFamily:columnName`. |
| hbase_conf | 是 | | 用于创建HBase连接的配置。 |


#### 可选参数

| 参数名称                  | 是否必须 | 参数枚举值 | 参数描述                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| reader_parallelism_num | 否       |                | 读并发                                               |
| encoding | 否 | | 处理HBase中的binary数据时使用的编码方式。默认为utf-8.|

## HBase写连接器

### 支持数据类型

支持将以下数据类型转化为bytes后写入HBase:

- varchar
- string
- boolean
- short
- int
- long
- bigint
- double
- decimal
- float
- date
- timestamp
- binary

### 参数

读连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.hbase.sink.HBaseOutputFormat",
      "table": "test_table",
      "hbase_conf":{
        "hbase.zookeeper.quorum":"127.0.0.1",
        "hbase.zookeeper.property.clientPort":"2181"
      },
      "row_key_column": "id_$(cf1:str1)",
      "columns": [
        {
          "index": 0,
          "name": "cf1:str1",
          "type": "string"
        },
        {
          "index": 1,
          "name": "cf1:int1",
          "type": "bigint"
        }
      ]
    }
  }
}
```

#### 必需参数

| 参数名称                  | 是否必须 | 参数枚举值 | 参数描述                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class                        | 是      |                | HBase写连接器名, `com.bytedance.bitsail.connector.legacy.hbase.sink.HBaseOutputFormat` |
| table | 是 |  | 要写入的HBase表名 | 
| columns | 是 | | 描述字段名称和字段类型。字段名格式为: `columnFamily:columnName`. |
| hbase_conf | 是 | | 用于创建HBase连接的配置。 |
| row_key_column | 是 | | 用于指定写入列的RowKey |

row_key_column的格式说明如下:
 - `$(XX)` 表示使用columns中定义的XX列的值来填充
 - `md5(...)` 表示对括号中的内容进行md5计算

例如: `$(cf:name)_md5($(cf:id)_split_$(cf:age))`

#### 可选参数

| 参数名称                  | 是否必须 | 参数枚举值 | 参数描述                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| writer_parallelism_num | 否       |                | 写并发,最大不超过表的region数量。                                              |
| encoding | 否 | | 将数据转化为bytes时使用的编码方式。默认为utf-8.|
| null_mode | 否 | | 遇到null数据时的处理方式。<br/>"skip"表示跳过此数据, "empty"表示置为空的bytes。<br/> 默认为"empty".|
| wal_flag| 否 | |  是否使用Write-ahead logging。默认false。|
| write_buffer_size | 否 | | mutate操作的buffer大小。默认8MB。 |
| version_column | 否  | | 决定写入数据的时间戳。|

version_column的使用说明如下:
 
 - 若不设置此选项，则默认以运行时间作为写入时间戳。
 - 若设置为 `{"index":x}`，则使用第 x 列的值（需要能转化为整型时间戳）作为写入时间戳。例如:
    ```json
      {
        "version_column": {
            "index": 1      // 表示使用第1列的值作为时间戳（从0开始）
        }
      }
    ```
- 若设置为 `{"value":xxx}`或者`{"value":"xxx"}`，则固定使用XXX（需要能转化为整型时间戳）作为写入时间戳。例如:
  ```json
    {
      "version_column": {
          "value": "1234567890"       // 写入时间戳固定为1234567890
      }
    }
  ```

## 相关文档

配置示例文档：[HBase 连接器示例](./hbase-example.md)