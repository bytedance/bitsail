# Hive connector

Parent document: [Connectors](../README.md)

The **BitSail** hive connector supports reading and writing to hive tables. The main function points are as follows:

 - Support reading partitioned and non-partitioned tables
 - Support writing to partition table
 - Support reading and writing hive tables in multiple formats, such as parquet, orc and text

## Supported hive versions

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

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hive</artifactId>
   <version>${revision}</version>
</dependency>
```

## Hive reader

### Supported data types
- 
- Basic Data types:
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
  - Binary type:
      - binary
- Composited data types:
    - map
    - array

### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

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

#### Necessary parameters

| Param name           | Required | Optional value | Description                                                                                          |
|:---------------------|:---------|:---------------|:-----------------------------------------------------------------------------------------------------|
| class                | Yes      |                | Hive read connector class name, `com.bytedance.bitsail.connector.legacy.hive.source.HiveInputFormat` |
| db_name              | Yes      |                | hive database name                                                                                   |
| table_name           | Yes      |                | hive table name                                                                                      |
| metastore_properties | Yes      |                | Metastore property string in standard json format                                                    | 


#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| partition              | No       |                | The hive partition to read. If not set, read the entire hive table    |
| columns                | No       |                | Describing fields' names and types. If not set, get it from metastore |
| reader_parallelism_num | No       |                | Read parallelism num                                                  |

## Hive writer

### Supported data type
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
    - Binary type:
        - binary
- Composited data types supported:
    - map
    - array

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

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

#### Necessary parameters

| Param name | Is necessary | Optional value | Description                                                           |
|:-----------|:-------------|:---------------|:----------------------------------------------------------------------|
| db_name    | Yes          |                | hive database name                                                    |
| table_name | Yes          |                | hive table name                                                       |
| partition  | Yes          |                | hive partition to write                                               |
| columns    | No           |                | Describing fields' names and types. If not set, get it from metastore |


#### Optional parameters

| Param name                   | Is necessary | Optional value        | Description                                                                                             |
|:-----------------------------|:-------------|:----------------------|:--------------------------------------------------------------------------------------------------------|
| date_to_string_as_long       | false        |                       | Whether to convert date data to integer timestamp                                                       |
| null_string_as_null          | false        |                       | Whether to convert null data to null. If false, convert it to an empty string `""`                      |
| date_precision               | second       | second<br>millisecond | Precision when converting date data to integer timestamps                                               |
| convert_error_column_as_null | false        |                       | Whether to set the conversion error field to null. If false, throw an exception if the conversion fails |
| hive_parquet_compression     | gzip         |                       | When the hive file is in parquet format, specify the compression method of the file                     |

## Related documents

Configuration examples: [Hive connector example](./hive-example.md)
