# Hadoop connector

Parent document: [Connectors](../README.md)

## Main function

Hadoop connector can be used to read hdfs files in batch scenarios. Its function points mainly include:

 - Support reading files in multiple hdfs directories at the same time
 - Support reading hdfs files of various formats

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hadoop</artifactId>
   <version>${revision}</version>
</dependency>
```

## Supported data types
 - Basic data types supported by Hadoop connectors:
      - Integer type:
        - short
        - int
        - long
        - biginterger
      - Float type:
          - float
          - double
          - bigdecimal
      - Time type:
          - timestamp
          - date
          - time
      - String type:
          - string
      - Bool type:
          - boolean
      - Binary type:
          - binary
- Composited data types supported by Hadoop connectors:
    - map
    - list

## Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

```json
{
  "job": {
    "reader": {
      "path_list": "hdfs://test_path/test.csv"
    }
  }
}
```

###  Necessary parameters

| Param name   | Required | Optional value | Description                                                                                       |
|:-------------|:---------|:---------------|:--------------------------------------------------------------------------------------------------|
| class        | Yes      |                | Class name of hadoop connector, `com.bytedance.bitsail.connector.hadoop.source.HadoopInputFormat` |
| path_list    | Yes      |                | Specifies the path of the read in file. Multiple paths can be specified, separated by `','`       |
| content_type | Yes      | JSON<br>CSV    | Specify the format of the read in file. For details, refer to[支持的文件格式](#jump_format)              |
| columns      | Yes      |                | Describing fields' names and types                                                                |

### Optional parameters
| Param name             | Required | Optional value | Description                                                                 |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------------|
| hadoop_conf            | No       |                | Specify the read configuration of hadoop in the standard json format string |
| reader_parallelism_num | No       |                | Reader parallelism                                                          |


## <span id="jump_format">Supported format</span>

Support the following formats:

- [JSON](#jump_json)
- [CSV](#jump_csv)

<!-- - [PROTOBUF]&#40;#jump_protobuf&#41; ) -->

### <span id="jump_json">JSON</span>
It supports parsing text files in json format. Each line is required to be a standard json string. 

The following parameters are supported to adjust the json parsing stype:


| Parameter name                            | Default value | Description                                                                                                                          |
|-------------------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `job.common.case_insensitive`             | true          | Whether to be sensitive to the case of the key in the json field                                                                     |
| `job.common.json_serializer_features`     |               | Specify the mode when 'FastJsonUtil' is parsed. The format is `','` separated string, for example`"QuoteFieldNames,UseSingleQuotes"` |
| `job.common.convert_error_column_as_null` | false         | Whether to set the field with parsing error to null                                                                                  |

### <span id="jump_csv">CSV</span>
Support parsing of text files in csv format. Each line is required to be a standard csv string.

The following parameters are supported to adjust the csv parsing style:


| Parameter name                    | Default value | Description                                                                |
|-----------------------------------|---------------|----------------------------------------------------------------------------|
| `job.common.csv_delimiter`        | `','`         | csv delimiter                                                              |
| `job.common.csv_escape`           |               | escape character                                                           |
| `job.common.csv_quote`            |               | quote character                                                            |
| `job.common.csv_with_null_string` |               | Specify the conversion value of null field. It is not converted by default |

<!--
### <span id="jump_protobuf">PROTOBUF</span>

支持对protobuf格式文件进行解析。

解析protobuf格式文件时，必需以下参数:


| 参数名称 | 参数是否必需   | 参数说明 |
|--------|----------|---------|
|`job.common.proto.descriptor`| 是 |base64方式存储protobufm描述文件|
|`job.common.proto.class_name`| 是 |指定protobuf描述文件中用于解析的类名|
-->

## Related documents

Configuration examples: [Hadoop connector example](./hadoop-example.md)
