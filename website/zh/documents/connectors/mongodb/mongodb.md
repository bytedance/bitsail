# MongoDB 连接器

上级文档：[连接器](../README.md)

**BitSail** MongoDB 连接器支持读写 MongoDB 的 Collection，主要功能点如下:

- 支持批式读取Collection中文档
- 支持批式写入Collection


## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-mongodb</artifactId>
   <version>${revision}</version>
</dependency>
```

## MongoDB读取

### 支持数据类型

MongoDB读连接器根据字段映射进行解析，支持以下数据类型:

#### 基本数据类型

 - string, character
 - boolean
 - short, int, long, float, double, bigint
 - date, time, timestamp

#### 复合数据类型

 - array, list
 - map

### 主要参数

写连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

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

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | MongoDB读连接器类型, `com.bytedance.bitsail.connector.legacy.mongodb.sink.MongoDBInputFormat` |
| db_name | 是 | | 要读取的database | 
| collection_name| 是 | | 要读取的collection |
| hosts_str |  | | MongoDB的连接地址，多个地址用逗号分隔 |
| host | | | MongoDB的单个host | 
| port | | | MongoDB的连接port |
| split_pk | 是 | | 用于分片的字段 |

- 注意，上述 (hosts_str) 和 (host, port) 二选一组合即可，优先使用hosts_str.
- hosts_str格式为: `host1:port1,host2:port2,...`

#### 可选参数


| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| reader_parallelism_num | 否 |       | 指定MongoDB读并发                  |
| user_name |  否 | | 用于鉴权的user name |
| password | 否 | | 用于鉴权的password |
| auth_db_name |  否 | | 用于鉴权的db名 |
| reader_fetch_size | 否 | | 单次最多获取的文档数, 默认100000 |
|  filter | 否 | | 过滤collection中的document |


-----

## MongoDB写入

### 支持数据类型

MongoDB写连接器将用户定义的一行数据写入到一个document中，然后插入collection。
支持的数据类型包括:

#### 基本数据类型

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

#### 复合数据类型

 - array


### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.mongodb.source.MongoDBOutputFormat",
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

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | MongoDB写连接器类型, `com.bytedance.bitsail.connector.legacy.mongodb.sink.MongoDBOutputFormat` |
| db_name | 是 | | 要写入的database | 
| collection_name| 是 | | 要插入的collection |
| client_mode | 是 | url<br/>host_without_credential<br/>host_with_credential | 指定如何创建mongo client |
| url | 如果client_mode=url，则必需 | | MongoDB连接url, 例如 "mongodb://localhost:1234" |
| mongo_hosts_str |  | | mongo连接地址，多个地址用逗号分隔 |
| mongo_host | | | mongo单个连接地址 |
| mongo_port | | | mongo单个连接端口 |
| user_name | 如果client_mode=host_with_credential，则必需 | | 用于鉴权的user name |
| password | 如果client_mode=host_with_credential，则必需 | | 用于鉴权的password |

- 注意，当client_mode为host_without_credential或者host_with_credential时，需要从上述 (mongo_hosts_str) 和 (mongo_host, mongo_port) 二选一组合进行设置，优先使用mongo_hosts_str.


#### 可选参数

| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| writer_parallelism_num | 否 |       | 指定redis写并发                       |
| pre_sql | 否 | | 数据写入前执行的sql |
| auth_db_name |  否 | | 用于鉴权的db名 |
| batch_size | 否 | | 单次写入文档数, 默认100 |
| unique_key | 否 | | 用于判断document是否唯一的字段 |
|  connect_timeout_ms | 否 | | 连接超时，默认10000 ms |
|  max_wait_time_ms | 否 | | 从连接池获取超时，默认120000 ms |
|  socket_timeout_ms | 否 | | Socket超时，默认0不设置 | 
| write_concern | 否 | 0, 1, 2, 3 | 数据写入保障级别, 默认为1 |


## 相关文档

配置示例文档：[MongoDB 连接器示例](./mongodb-example.md)
