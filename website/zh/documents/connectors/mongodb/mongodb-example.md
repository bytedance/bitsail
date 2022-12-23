# MongoDB 连接器示例

父目录：[MongoDB 连接器](./mongodb.md)

假设在本地启动了一个 MongoDB，连接地址为 mongodb://localhost:1234。在其中创建了名为 `test_db` 的database和名为 `test_collection` 的文档集合。

## MongoDB 读连接器 

假设想在文档包含 _id, string_field, int_field 三个字段，则可用如下配置读取。

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.mongodb.source.MongoDBInputFormat",
      "split_key": "_id",
      "host": "localhost",
      "port": 1234,
      "db_name": "test_db",
      "collection_name": "test_collection",
      "columns": [
        {
          "index": 0,
          "name": "_id",
          "type": "objectid"
        },
        {
          "index": 1,
          "name": "string_field",
          "type": "string"
        },
        {
          "index": 2,
          "name": "int_field",
          "type": "long"
        }
      ],
      "reader_parallelism_num":1
    }
  }
}
```

## MongoDB写连接器

假设想在文档中写入 id, string_field, integer_field 三个字段，那么可以用如下配置进行写入。


```json
{
   "job": {
     "writer": {
       "class": "com.bytedance.bitsail.connector.legacy.mongodb.sink.MongoDBOutputFormat",
       "unique_key": "id",
       "client_mode": "url",
       "mongo_url": "mongodb://localhost:1234/test_db",
       "db_name": "test_db",
       "collection_name": "test_collection",
       "columns": [
         {
           "index": 0,
           "name": "id",
           "type": "string"
         },
         {
           "index": 1,
           "name": "string_field",
           "type": "string"
         },
         {
           "index": 2,
           "name": "integer_field",
           "type": "long"
         }
       ]
     }
   }
}
```