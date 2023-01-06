# FTP/SFTP 连接器

上级文档：[连接器](../README.md)

## 主要功能

连接器可用于批式场景下的FTP/SFTP服务器上文件读取。其功能点主要包括:

- 支持同时读取多个目录下的文件
- 支持读取多种格式的文件

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-ftp</artifactId>
   <version>${revision}</version>
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
  - 二进制类型：
    - binary
- 支持的复杂数据类型包括：
  - map
  - array

## 主要参数

以下参数在 `job.reader` 中配置，配置示例请参考 [FTP/SFTP 连接器示例](./ftp-example.md)

### 必需参数

| 参数名称              | 参数是否必需 | 参数枚举值    | 参数含义                                                                          |
|:------------------|:-------|:---------|:------------------------------------------------------------------------------|
| class             | 是      |          | 读连接器类名，只能为 `com.bytedance.bitsail.connector.legacy.ftp.source.FtpInputFormat` |
| path_list         | 是      |          | 指定读入文件的路径。可指定多个路径，使用 `','`分隔                                                  |
| content_type      | 是      | JSON/CSV | 指定读入文件的格式，详情参考[支持的文件格式](#jump_format)                                         |
| columns           | 是      |          | 数据字段名称及类型                                                                     |
| port              | 是      |          | 服务器端口，FTP通常为21，SFTP 为22                                                       |
| host              | 是      |          | 服务器主机地址                                                                       |
| user              | 是      |          | 用户名                                                                           |
| password          | 是      |          | 密码                                                                            |
| protocol          | 是      | FTP/SFTP | 文件传输协议                                                                        |
| success_file_path | 是      |          | SUCCESS 标签文件路径(检查默认开启，文件存在才会执行任务)                                             |

### 可选参数

| 参数名称                      | 参数是否必需 | 默认值                  | 参数枚举值          | 参数含义                                      |
|:--------------------------|:-------|----------------------|:---------------|:------------------------------------------|
| connect_pattern           | 否      | PASV(FTP)/NULL(SFTP) | PASV/PORT/NULL | 连接模式，FTP 协议下可为 PASV 或 PORT，SFTP 协议下为 NULL |
| time_out                  | 否      | 5000ms               |                | 连接超时                                      |
| enable_success_file_check | 否      | True                 |                | 默认开启，必须有 SUCCESS 标签文件存在才会执行任务             |
| max_retry_time            | 否      | 60                   |                | 检查 SUCCESS 标签文件次数                         |
| retry_interval_ms         | 否      | 60s                  |                | 检查 SUCCESS 标签文件间隔                         |
| charset                   | 否      | utf-8                |                | 文件编码方式                                    |

## <span id="jump_format">支持的文件格式</span>

支持对以下格式的文件进行解读(配置参数`content_type`):

- [JSON](#jump_json)
- [CSV](#jump_csv)


### <span id="jump_json">JSON</span>

支持对json格式的文本文件进行解析，要求每行均为标准的json字符串。
支持以下参数对json解析方式进行调整:

| 参数名称                                      | 参数默认值 | 参数说明                                                                           |
|-------------------------------------------|-------|--------------------------------------------------------------------------------|
| `job.common.case_insensitive`             | true  | 是否对json字段中的key大小写敏感                                                            |
| `job.common.json_serializer_features`     |       | 指定 `FastJsonUtil`进行解析时的模式，格式为`','`分隔的字符串，如 `"QuoteFieldNames,UseSingleQuotes"` |
| `job.common.convert_error_column_as_null` | false | 是否将解析出错的字段置为null                                                               |

### <span id="jump_csv">CSV</span>

支持对 CSV 格式的文本文件进行解析，要求每行均为标准的 CSV 字符串。
支持以下参数对 CSV 解析方式进行调整：

| 参数名称                              | 参数默认值 | 参数说明               |
|-----------------------------------|-------|--------------------|
| `job.common.csv_delimiter`        | `','` | csv分隔符             |
| `job.common.csv_escape`           |       | escape字符           |
| `job.common.csv_quote`            |       | quote字符            |
| `job.common.csv_with_null_string` |       | 指定null字段的转化值，默认不转化 |

## 相关文档

配置示例文档：[FTP/SFTP 连接器示例](./ftp-example.md)
