# Hudi connector

Parent document: [Connectors](../README.md)

The **BitSail** hudi connector supports reading and writing to hudi tables. The main function points are as follows:

 - Support streaming write to Hudi table.
 - Support batch write to Hudi table.
 - Support batch read from Hudi table.

## Supported hudi versions

- 0.11.1

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-hudi</artifactId>
   <version>${revision}</version>
</dependency>
```

## Hudi reader

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
    "reader":{
      "hoodie":{
        "datasource":{
          "query":{
            "type":"snapshot"
          }
        }
      },
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder",
      "table":{
        "type":"MERGE_ON_READ"
      }
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class                        | Yes      |                | Hudi read connector class name, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder` |
| path                         | Yes      |                | the path of the table, could be HDFS, S3, or other file systems.                                               |
| table.type                   | Yes      |                | The type of the Hudi table, MERGE_ON_READ or COPY_ON_WRITE                                                     |
| hoodie.datasource.query.type | Yes      |                | Query type, could be `snapshot` or `read_optimized`                                                            | 


#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| reader_parallelism_num | No       |                | Read parallelism num                                                  |

## Hudi writer

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
      "hoodie": {
        "bucket": {
          "index": {
            "num": {
              "buckets": "4"
            },
            "hash": {
              "field": "id"
            }
          }
        },
        "datasource": {
          "write": {
            "recordkey": {
              "field": "id"
            }
          }
        },
        "table": {
          "name": "test_table"
        }
      },
      "path": "/path/to/table",
      "index": {
        "type": "BUCKET"
      },
      "class": "com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder",
      "write": {
        "operation": "upsert"
      },
      "table": {
        "type": "MERGE_ON_READ"
      },
      "source_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]",
      "sink_schema": "[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"test\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]"
    }
  }
}
```

#### Necessary parameters

| Param name        | Is necessary | Optional value | Description                                                                                                                  |
|:------------------|:-------------|:---------------|:-----------------------------------------------------------------------------------------------------------------------------|
| class             | Yes          |                | Hudi write class name, `com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder`                         |
| write.operation   | Yes          |                | `upsert` `insert` `bulk_insert`                                                                                              |
| table.type        | Yes          |                | `MERGE_ON_READ` `COPY_ON_WRITE`                                                                                              |
| path              | Yes          |                | path to the Hudi table, could be HDFS, S3, or other file system. If path not exists, the table will be created on this path. |
| format_type       | Yes          |                | format of the input data source, currently only support `json`                                                               |
| source_schema     | Yes          |                | schema used to deserialize source data.                                                                                      |
| sink_schema       | Yes          |                | schema used to write hoodie data                                                                                             |
| hoodie.table.name | Yes          |                | the name of the hoodie table                                                                                                 |


#### Optional parameters

For more advance parameter, please checkout `FlinkOptions.java` class.

| Param name                              | Is necessary | Optional value        | Description                                                                            |
|:----------------------------------------|:-------------|:----------------------|:---------------------------------------------------------------------------------------|
| hoodie.datasource.write.recordkey.field | false        |                       | For `upsert` operation, we need to define the primary key.                             |
| index.type                              | false        |                       | For `upsert` operation, we need to define the index type. could be `STATE` or `BUCKET` |
| hoodie.bucket.index.num.buckets         | false        |                       | If we use Bucket index, we need to define the bucket number.                           |
| hoodie.bucket.index.hash.field          | false        |                       | If we use Bucket index, we need to define a field to determine hash index.             |

## Hudi Compaction

### Parameters

Compaction has well-defined reader and writer parameters

```json
{
  "job":{
    "reader":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder"
    },
    "writer":{
      "path":"/path/to/table",
      "class":"com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder"
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                              |
|:-----------------------------|:---------|:---------------|:-------------------------------------------------------------------------------------------------------------------------|
| job.reader.class             | Yes      |                | Hudi compaction read connector class name, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder` |
| job.writer.class             | Yes      |                | Hudi compaction writer connector class name, `com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder` |
| job.reader.path              | Yes      |                | the path of the table, could be HDFS, S3, or other file systems.                                                         |
| job.writer.path              | Yes      |                | the path of the table, could be HDFS, S3, or other file systems.                                                         |


#### Optional parameters

| Param name             | Required | Optional value | Description                           |
|:-----------------------|:---------|:---------------|:--------------------------------------|
| writer_parallelism_num | No       |                | parallelism to process the compaction |

## Related documents

Configuration examples: [Hudi connector example](./hudi-example.md)
