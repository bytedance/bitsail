# Hive 连接器示例

上级文档：[Hive 连接器](./hive.md)

下面展示如何使用用户参数配置读取测试hive表:

## 测试hive表信息
 - 示例hive信息：
     - hive库名: test_db
     - hive表名: test_table
     - metastore uri地址: `thrift://localhost:9083`
     - 分区: p_date
     - 表结构:

         | 字段名 | 字段类型 | 说明 |
         |-------|--------| ---- |
         | id | bigint | |
         | state | string | |
         | county | string | |
         | p_date | string | 分区字段 |

## Hive 读连接器

读取上述测试hive表的用户配置:

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

## Hive 写连接器

写入上述测试 Hive 表的用户配置:

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
