# LocalFileSystem 连接器

上级文档：[连接器](../README.md)

**BitSail** LocalFileSystem 连接器可以从本地文件系统读取文件。其功能点主要包括：

- 支持一次从单个文件（csv/json 内容类型）批量读取。

## 依赖引入

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-localfilesystem</artifactId>
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

写连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。例如：


```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.localfilesystem.source.LocalFileSystemSource",
      "file_path": "src/test/resources/data/csv/test.csv",
      "content_type": "csv",
      "columns": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "date",
          "type": "date"
        },
        {
          "name": "localdatetime_value",
          "type": "timestamp"
        },
        {
          "name": "last_name",
          "type": "string"
        },
        {
          "name": "bool_value",
          "type": "boolean"
        }
      ]
    }
  }
}
```

#### 必需参数

| 参数名称         | 是否必填 | 参数枚举值      | 参数含义                                                                                                  |
|:-------------|:-----|:-----------|:------------------------------------------------------------------------------------------------------|
| class        | yes  |            | LocalFileSystem读连接器类型， `com.bytedance.bitsail.connector.localfilesystem.source.LocalFileSystemSource` |
| file_path    | yes  |            | 读取本地文件的存储路径                                                                                           |
| content_type | yes  | csv / json | 指定解析的文件类型                                                                                             |
| columns      | yes  |            | 指定读取的字段名和字段类型                                                                                         |

#### 可选参数（`all`, `csv` 和 `json` 表示适用的内容类型）

| 参数名称                                   | 是否必填 | 参数枚举值        | 参数含义               |
|:---------------------------------------|:-----|:-------------|:-------------------|
| skip_first_line `all`                  | no   | true / false | 是否跳过第一行            |
| convert_error_column_as_null `all`     | no   | true / false | 是否将解析出错列视为空值列      |
| csv_delimiter `csv`                    | no   |              | 指定两个字段之间的分隔符       |
| csv_escape `csv`                       | no   |              | 指定任意转义字符           |
| csv_quote `csv`                        | no   |              | 指定任意引号字符           |
| csv_with_null_string `csv`             | no   |              | 指定字符串作为空值字面量       |
| csv_multi_delimiter_replace_char `csv` | no   |              | 指定一字符替换含超过一个字符的分隔符 |
| case_insensitive `json`                | no   | true / false | 是否对大写或小写不敏感        |

## 相关文档

配置示例文档： [LocalFileSystem 连接器示例](./localfilesystem-example.md)