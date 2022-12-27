# HBase connector

Parent document: [Connectors](../README.md)

**BitSail** HBase can be used to read and write HBase tables.
The main function points are as follows:

 - Support scanning HBase tables.
 - Support set RowKey while writing HBase tables.

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hbase</artifactId>
   <version>${revision}</version>
</dependency>
```

## HBase reader

### Supported data types


HBase reader supports transform binary data from HBase into following formats of data:

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



### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

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

#### Necessary parameters


| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class                        | Yes     |                | HBase reader class name, `com.bytedance.bitsail.connector.legacy.hbase.source.HBaseInputFormat` |
| table | Yes |  | Target HBase table to read. | 
| columns | Yes | | Describing fields' names and types. The format should be: `columnFamily:columnName`. |
| hbase_conf | Yes | | Configurations for creating HBase connection. |


#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| reader_parallelism_num | No       |                | Read parallelism num                                                  |
| encoding | No | | The encoding style when decoding binary data from HBase. Default utf-8. |


## HBase writer

### Supported data types

HBase writer supports transform the following formats of data into binary data:

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

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

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

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class                        | Yes      |                | HBase writer class name, `com.bytedance.bitsail.connector.legacy.hbase.sink.HBaseOutputFormat` |
| table | Yes |  | Target table to write. | 
| columns | Yes | | Describing fields' names and types. The format should be: `columnFamily:columnName`. |
| hbase_conf | Yes | | Configurations for creating HBase connection. |
| row_key_column | Yes | | Determine the RowKey for each row. |

The format of `row_key_column` is as follows:
- `$(XX)` means using the value of `XX` defined in `columns`.
- `md5(...)` means the md5 operation.

For example: `$(cf:name)_md5($(cf:id)_split_$(cf:age))`

#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num | No       |                | Writer parallelism num. No larger than the region number of table.                                                  |
| encoding | No | | The encoding style when encoding data. Default utf-8. |
| null_mode | No | | How to process null value. <br/>"skip" means this value will not be inserted.<br/>"empty" means set the row to empty bytes.<br/>Default "empty". |
| wal_flag| No | |  If enable Write-ahead logging. Default false.|
| write_buffer_size | No | | The buffer size of mutate operation. Default 8MB. |
| version_column | No  | | Determine the timestamp of inserted rows.|

The usage of version_column is as follow:

- If not set, use the runtime timestamp as cells' timestamp.
- If `"version_column" = {"index":x}`, then use the value of the x-th (begin from 0) column as cells' timestamp. For example:
   ```json
     {
       "version_column": {
           "index": 1      // Use the second column defined in `job.writer.columns`
       }
     }
   ```
- If `"version_column" = {"value":xxx}` or `"version_column" = {"value":"xxx"}`, then use the give value as cells' timestamp. For example:
  ```json
    {
      "version_column": {
          "value": "1234567890"       // Cells' timestamp: 1234567890
      }
    }
  ```




## Related documents

Configuration examples: [HBase connector example](./hbase-example.md)