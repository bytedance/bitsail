# MongoDB connector

Parent document: [Connectors](../README.md)

**BitSail** MongoDB connector supports reading and writing MongoDB. The main function points are as follows:

 - Support batch read documents from give collection. 
 - Support batch write to target collection.


## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-mongodb</artifactId>
   <version>${revision}</version>
</dependency>
```

## MongoDB Reader

### Supported data types

MongoDB parse data according to schema. The following data types are supported:

#### Basic data type

- string, character
- boolean
- short, int, long, float, double, bigint
- date, time, timestamp

#### Complex data type

- array, list
- map

### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.mongodb.source.MongoDBInputFormat",
      "host": "localhost",
      "port": 1234,
      "db_name": "test_db",
      "collection_name": "test_collection"
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | yes |       | Class name of MongoDB reader, `com.bytedance.bitsail.connector.legacy.mongodb.source.MongoDBInputFormat` |
| db_name | Yes | | database to read| 
| collection_name| yes| | collection to read |
| hosts_str |  | | Address of MongoDB, multi addresses are separated by comma |
| host | | | host of MongoDB |  
| port | | | port of MongoDB |
| split_pk | yes| | Field for splitting |


- Note, You need only set either (hosts_str) or (hosts_str).  (hosts_str) has higher priority.
- Format of hosts_str: `host1:port1,host2:port2,...`


#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| reader_parallelism_num | no |       | MongoDB reader parallelism num                  |
| user_name |  no | | user name for authentication |
| password | no | | password for authentication |
| auth_db_name |  no | | db for authentication |
| reader_fetch_size | no | | Max number of documents fetched once .Default 100000 |
|  filter | no | | Filter for collections. |


-----

## MongoDB Writer

### Supported data types

MongoDB writer build a document for each record according to schema, and then insert it into collection.

Supported data types are:


#### Basic data type

- undefined
- string
- objectid
- date
- timestamp
- bindata
- bool
- int
- long
- object
- javascript
- regex
- double
- decimal

#### Complex data type

- array


### Parameters


The following mentioned parameters should be added to `job.writer` block when using, for example:


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
        }
      ]
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | yes |       | Class name for MongoDB writer, `com.bytedance.bitsail.connector.legacy.mongodb.sink.MongoDBOutputFormat` |
| db_name | yes| | database to write | 
| collection_name| yes| | collection to write |
| client_mode | yes| url<br/>host_without_credential<br/>host_with_credential | how to create mongo client |
| url | Yes if client_mode=url | | Url for connecting MongoDB, like "mongodb://localhost:1234" |
| mongo_hosts_str |  | | Address of MongoDb, multi addresses are separated by comma |
| mongo_host | | | host of MongoDB |  
| mongo_port | | | port of MongoDB |
| user_name | Yes if client_mode=host_with_credential | | user name  for authentication |
| password | Yes if client_mode=host_with_credential| | password for authentication |

- Note, when client_mode为host_without_credential or host_with_credential, you have to set either (mongo_hosts_str) or (mongo_host, mongo_port).



#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num | No       |                | Writer parallelism num                           |
| pre_sql | no | | Sql executed before inserting collections. |
| auth_db_name |  no | | db name for authentication |
| batch_size | no | | Batch write number of documents, Default 100 |
| unique_key | no | | Field for determining if document is unique |
|  connect_timeout_ms | no | | connection timeout，default 10000 ms |
|  max_wait_time_ms | no | | timeout when getting connection from connection pool，default 120000 ms |
|  socket_timeout_ms | no | | socket timeout，default 0 (means infinity) | 
| write_concern | no | 0, 1, 2, 3 | Data writing guarantee level, default 1 |


## Related documents

Configuration examples: [MongoDB connector example](./mongodb-example.md)
