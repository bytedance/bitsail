# Redis connector

Parent document: [Connectors](../README.md)

**BitSail** Redis connector supports writing Redis. The main function points are as follows:

 - Support batch write to Redis.
 - Support 4 kinds of data type in Redis.


## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-redis</artifactId>
   <version>${revision}</version>
</dependency>
```

## Redis writer

### Supported data types

We support four kinds of data types in Redis: 
 - `String, Set, Hash, Sorted Set`.

Each data type has its own requirement for input data:


| Data type | Required column numbers | 1st column | 2nd column | 3rd column |
| ------- | ------- | ----- | ---- | ------ |
| String | 2 | key | value | |
| Set | 2 | key of set | value to insert into set |
| Hash | 3 | key of hash | key to insert to hash | value to insert to hash |
| Sorted Set (Zset) | 3 | key of Sorted set | score | value to insert to set |

Tips: If there are more than 3 columns, `Mhash` can be used.

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.redis.sink.RedisOutputFormat",
      "redis_data_type": "string",
      "redis_host": "localhost",
      "redis_port": 6379
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | Yes  |       | Class name of redis writer, `com.bytedance.bitsail.connector.legacy.redis.sink.RedisOutputFormat` |
| redis_host   | Yes  |       | host of redis |
| redis_port        | Yes  |       | port of redis |



#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num | No       |                | Writer parallelism num                                                  |
| client_timeout_ms | No | | Timeout of redis connection. Default 60000 ms |
| ttl | No | | Ttl of inserted data. Default -1 means not setting ttl |
| ttl_type | No  | "DAY", "HOUR", "MINUTE", "SECOND" |  Time unit of ttl. Default "DAY" |
| write_batch_interval | No | | Redis instruction write batch size. Default 50 | 
| redis_data_type | No | "string"<br/>"set"<br/>"hash"<br/>"sorted_set"<br/>"mhash" | Data type to insert. Default "string" | 
| password | No | | Password of redis | 


## Related documents

Configuration examples: [Redis connector example](./redis-example.md)
