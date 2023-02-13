# LocalFileSystem connector

Parent document: [Connectors](../README.md)

**BitSail** LocalFileSystem connector supports reading file from local file system.

The main function points are as follows:

- Support batch read from single file(csv/json content type) at once.

## Maven dependency

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-localfilesystem</artifactId>
    <version>${revision}</version>
    <scope>provided</scope>
</dependency>
```

## Supported data types

- Basic data types supported:
    - Integer type:
        - tinyint
        - smallint
        - int
        - bigint
    - Float type:
        - float
        - double
        - decimal
    - Time type:
        - timestamp
        - date
    - String type:
        - string
        - varchar
        - char
    - Bool type:
        - boolean

### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:


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

#### Necessary parameters

| Param name   | Required | Optional value | Description                                                                                                         |
|:-------------|:---------|:---------------|:--------------------------------------------------------------------------------------------------------------------|
| class        | yes      |                | LocalFileSystem reader's class name, `com.bytedance.bitsail.connector.localfilesystem.source.LocalFileSystemSource` |
| file_path    | yes      |                | The storage path of local file to read                                                                              |
| content_type | yes      | csv / json     | The content type of local file to deserialize                                                                       |
| columns      | yes      |                | The name and type of columns to read                                                                                |

#### Optional parameters(`all`, `csv` and `json` mean applicable content type)

| Param name                             | Required | Optional value | Description                                        |
|:---------------------------------------|:---------|:---------------|:---------------------------------------------------|
| skip_first_line `all`                  | no       | true / false   | whether to skip first line                         |
| convert_error_column_as_null `all`     | no       | true / false   | whether to treat error column as null when parsing |
| csv_delimiter `csv`                    | no       |                | indicate delimiter between two fields              |
| csv_escape `csv`                       | no       |                | indicate arbitrary escape character                |
| csv_quote `csv`                        | no       |                | indicate arbitrary quote character                 |
| csv_with_null_string `csv`             | no       |                | indicate string literal treat as "null" object     |
| csv_multi_delimiter_replace_char `csv` | no       |                | indicate multiple delimiter replacer               |
| case_insensitive `json`                | no       | true / false   | whether to be insensitive to upper or lower case   |

## Related documents

Configuration examples: [LocalFileSystem connector example](./localfilesystem-example.md)