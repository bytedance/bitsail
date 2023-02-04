# Hadoop连接器

上级文档: [connectors](../../README.md)


## 主要功能

Hadoop连接器可用于批式场景下的hdfs文件读取。其功能点主要包括:

 - 支持同时读取多个hdfs目录下的文件
 - 支持读取多种格式的hdfs文件

## 依赖引入

```text
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-hadoop</artifactId>
   <version>${revision}</version>
</dependency>
```

## 支持的数据类型
 - 支持的基础数据类型如下:
    - 整数类型:
        - short
        - int
        - long
        - bitinteger
    - 浮点类型:
        - float
        - double
        - bigdecimal
    - 时间类型:
        - timestamp
        - date
        - time
    - 字符类型:
        - string
    - 布尔类型:
        - boolean
    - 二进制类型:
        - binary
 - 支持的复杂数据类型包括:
    - map
    - list
    
## 主要参数

以下参数使用在`job.reader`配置中，实际使用时请注意路径前缀。示例:
```json
{
   "job": {
      "reader": {
         "class": "com.bytedance.bitsail.connector.hadoop.source.HadoopSource",
         "content_type":"json",
         "reader_parallelism_num": 1,
         "columns": [
            {
               "name":"id",
               "type": "int"
            },
            {
               "name": "string_type",
               "type": "string"
            },
            {
               "name": "map_string_string",
               "type": "map<string,string>"
            },
            {
               "name": "array_string",
               "type": "list<string>"
            }
         ]
      },
   }
}
```

### 必需参数

| 参数名称         | 参数是否必需 | 参数枚举值  | 参数含义                                                     |
|:-------------| :----------- | :---------- | :----------------------------------------------------------- |
| class        | 是           |             | Hadoop读连接器类名，v1 connector为为`com.bytedance.bitsail.connector.hadoop.source.HadoopSource` |                                      |
| path_list    | 是           |             | 指定读入文件的路径。可指定多个路径，使用`','`分隔            |
| content_type | 是           | JSON<br>CSV | 指定读入文件的格式                                           |
| columns      | 是           |             | 数据字段名称及类型                                           |

### 可选参数
| 参数名称                             | 参数是否必需 | 参数枚举值 | 参数含义                          |
| :----------------------------------- | :----------- | :--------- | :-------------------------------- |
| default_hadoop_parallelism_threshold | 否           |            | 每个reader读取的文件数目，默认为2 |
| reader_parallelism_num               | 否           |            | 读并发数，无默认值                |


## <span id="jump_format">支持的文件格式</span>

支持对以下格式的文件进行解读:

- [JSON](#jump_json)
- [CSV](#jump_csv)

### <span id="jump_json">JSON</span>
支持对json格式的文本文件进行解析，要求每行均为标准的json字符串。

### <span id="jump_csv">CSV</span>
支持对csv格式的文本文件进行解析，要求每行均为标准的csv字符串。
支持以下参数对csv解析方式进行调整:


| 参数名称                              | 参数默认值 | 参数说明               |
|-----------------------------------|-------|--------------------|
| `job.common.csv_delimiter`        | `','` | csv分隔符             |
| `job.common.csv_escape`           |       | escape字符           |
| `job.common.csv_quote`            |       | quote字符            |
| `job.common.csv_with_null_string` |       | 指定null字段的转化值，默认不转化 |

----


## 相关文档

配置示例文档 [hadoop v1连接器示例](./hadoop-v1-example.md)

