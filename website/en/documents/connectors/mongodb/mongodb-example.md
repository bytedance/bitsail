# MongoDB connector example

Parent document: [MongoDB connector](./mongodb.md)

Suppose starting a local MongoDB with connection url `mongodb://localhost:1234`.
We create a database `test_db` and a collection `test_collection` on it.

## MongoDB Reader

If the documents contains (_id, string_field, int_field) these three fields, we can use the following configuration to read.

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

## MongoDB Writer

If you want to write (id, string_field, integer_field) these three fields into document, then you can use the following configuration.


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