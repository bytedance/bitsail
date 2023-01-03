# Hive connector example

Parent document: [Hive connector](./hive.md)

The following shows test hive table with user parameters and how to read/write it with hive connectors.

## Test hive table information
- Example hive info:
    - hive database name: test_db
    - hive table name: test_table
    - metastore uri: `thrift://localhost:9083`
    - partition: p_date
    - DDL:

      | filed name | field type | description     |
      |-------|-----------------| ---- |
      | id | bigint | -               |
      | state | string | -               |
      | county | string | -               |
      | p_date | string | partition tield |

## Hive reader example

Configuration for reading the above test hive table:

```json
{
   "job": {
      "reader": {
         "class": "com.bytedance.bitsail.connector.legacy.hive.source.HiveInputFormat",
         "columns": [
            {
               "name": "id",
               "type": "bigint"
            },
            {
               "name": "state",
               "type": "string"
            },
            {
               "name": "county",
               "type": "string"
            }
         ],
         "db_name": "test_db",
         "table_name": "test_table",
         "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}",
         "partition": "p_date=20220101",
         "reader_parallelism_num": 1
      }
   }
}
```

## Hive writer example

Configuration for writing the above test hive table:

```json
{
   "job": {
      "writer": {
         "class": "com.bytedance.bitsail.connector.legacy.hive.sink.HiveOutputFormat",
         "columns": [
            {
               "name": "id",
               "type": "bigint"
            },
            {
               "name": "state",
               "type": "string"
            },
            {
               "name": "county",
               "type": "string"
            }
         ],
         "db_name": "test_db",
         "table_name": "test_table",
         "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}",
         "partition": "p_date=20220101",
         "writer_parallelism_num": 1
      }
   }
}
```
