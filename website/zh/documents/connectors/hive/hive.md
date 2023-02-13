# Hive 连接器

上级文档：[连接器](../README.md)

**BitSail** hive连接器支持对hive表进行读写。主要功能点如下:

   - 支持读取分区表和非分区表
   - 支持写入分区表
   - 支持读写多种格式 hive 表，比如parquet、orc、text

## 支持的hive版本

 - 1.2
    - 1.2.0
    - 1.2.1
    - 1.2.2
 - 2.0
    - 2.0.0
    - 2.1.0
    - 2.1.1
    - 2.3.0
    - 2.3.9
 - 3.0
    - 3.1.0
    - 3.1.2

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hive</artifactId>
   <version>${revision}</version>
</dependency>
```

## Hive读连接器

### 支持的数据类型

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

hive读连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "reader": {
      "db_name": "test_database",
      "table_name": "test_table"
    }
  }
}
```

#### 必需参数

| 参数名称                 | 参数是否必需 | 参数枚举值 | 参数含义                                                                               |
|:---------------------|:-------|:------|:-----------------------------------------------------------------------------------|
| class                | 是      |       | Hive读连接器类名，只能为`com.bytedance.bitsail.connector.legacy.hive.source.HiveInputFormat` |
| db_name              | 是      |       | 读取的hive库名                                                                          |
| table_name           | 是      |       | 读取的hive表名                                                                          |
| metastore_properties | 是      |       | 标准json格式的metastore属性字符串                                                            | 


#### 可选参数

| 参数名称                   | 参数是否必需 | 参数枚举值 | 参数含义                         |
|:-----------------------|:-------|:------|:-----------------------------|
| partition              | 否      |       | 要读取的hive分区。若不设置，则读取整张hive表   |
| columns                | 否      |       | 数据字段名称及类型。若不设置，则从metastore获取 |
| reader_parallelism_num | 否      |       | 读并发数                         |

## Hive写连接器

### 支持的数据类型
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

hive写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "db_name": "test_database",
      "table_name": "test_table"
    }
  }
}
```

#### 必需参数

| 参数名称       | 参数是否必需 | 参数枚举值 | 参数含义                         |
|:-----------|:-------|:------|:-----------------------------|
| db_name    | 是      |       | 写入的hive库名                    |
| table_name | 是      |       | 写入的hive表名                    |
| partition  | 是      |       | 写入的hive分区名                   |
| columns    | 否      |       | 数据字段名称及类型，若不设置，则从metastore获取 |


#### 可选参数

| 参数名称                         | 参数默认值  | 参数枚举值                 | 参数含义                                   |
|:-----------------------------|:-------|:----------------------|:---------------------------------------|
| date_to_string_as_long       | false  |                       | 是否将日期数据转化为整数时间戳                        |
| null_string_as_null          | false  |                       | 是否将null数据转化为null。若为false，则转化为空字符串 `""` |
| date_precision               | second | second<br>millisecond | 日期数据转化为整数时间戳时的精度                       |
| convert_error_column_as_null | false  |                       | 是否将转化出错的字段置为null。若为false，则在转化出错时抛出异常   |
| hive_parquet_compression     | gzip   |                       | 当hive文件为parquet格式时，指定文件的压缩方式           |

## 相关文档

配置示例文档：[Hive 连接器示例](hive-example.md)
