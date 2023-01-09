# Hadoop connector

Parent document: [connectors](../../README.md)


## Main function

Hadoop connector can be used to read hdfs files in batch scenarios. Its function points mainly include:

 - Support reading files in multiple hdfs directories at the same time
 - Support reading hdfs files of various formats

## Maven dependency

```text
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-hadoop</artifactId>
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

###  Necessary parameters

| Param name   | Required | Optional value | Description                                                  |
| :----------- | :------- | :------------- | :----------------------------------------------------------- |
| class        | Yes      |                | Class name of hadoop connector, v1 connector is `com.bytedance.bitsail.connector.hadoop.source.HadoopSource` |
| path_list    | Yes      |                | Specifies the path of the read in file. Multiple paths can be specified, separated by `','` |
| content_type | Yes      | JSON<br>CSV    | Specify the format of the read in file.                      |
| columns      | Yes      |                | Describing fields' names and types                           |

### Optional parameters
| Param name                           | Required | Optional value | Description                                                |
| :----------------------------------- | :------- | :------------- | :--------------------------------------------------------- |
| default_hadoop_parallelism_threshold | No       |                | The number of splits read by each reader, the default is 2 |
| reader_parallelism_num               | No       |                | Reader parallelism                                         |


## <span id="jump_format">Supported format</span>

Support the following formats:

- [JSON](#jump_json)
- [CSV](#jump_csv)

### <span id="jump_json">JSON</span>
It supports parsing text files in json format. Each line is required to be a standard json string. 

### <span id="jump_csv">CSV</span>
Support parsing of text files in csv format. Each line is required to be a standard csv string.

The following parameters are supported to adjust the csv parsing style:


| Parameter name                    | Default value | Description                                                                |
|-----------------------------------|---------------|----------------------------------------------------------------------------|
| `job.common.csv_delimiter`        | `','`         | csv delimiter                                                              |
| `job.common.csv_escape`           |               | escape character                                                           |
| `job.common.csv_quote`            |               | quote character                                                            |
| `job.common.csv_with_null_string` |               | Specify the conversion value of null field. It is not converted by default |

----


## Related document

Configuration examples: [hadoop-v1-connector-example](./hadoop-v1-example.md)
