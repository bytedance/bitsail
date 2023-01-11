# Redis-v1 connector

Parent document: [Connectors](../../README.md)

**BitSail** Redis connector supports writing Redis. The main function points are as follows:

- Support batch write to Redis.
- Support 4 kinds of data type in Redis.


## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-redis</artifactId>
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

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

```json
"writer": {
      "class": "com.bytedance.bitsail.connector.redis.sink.RedisSink",
      "redis_data_type": "hash",
      "redis_host": "localhost",
      "redis_port": 6379,
      "columns": [
        {
          "index": 0,
          "type": "string"
        },
        {
          "index": 1,
          "type": "string"
        },
        {
          "index": 2,
          "type": "string"
        }
      ]
    }
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | Yes  |       | Class name of redis writer, `com.bytedance.bitsail.connector.legacy.redis.sink.RedisOutputFormat` |
| redis_host   | Yes  |       | host of redis |
| redis_port        | Yes  |       | port of redis |
| columns | Yes | | Describing fields' name and type, type can be `binary` or `string` or other basic types.   The `binary` type is `byte[]`. |

#### Optional parameters

| Param name                       | Required | Optional value | Description                                                           |
|:---------------------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num           | No       |                | Writer parallelism num                                                  |
| client_timeout_ms                | No | | Timeout of redis connection. Default 60000 ms |
| ttl                              | No | | Ttl of inserted data. Default -1 means not setting ttl |
| ttl_type                         | No  | "DAY", "HOUR", "MINUTE", "SECOND" |  Time unit of ttl. Default "DAY" |
| write_batch_interval             | No | | Redis instruction write batch size. Default 50 |
| redis_data_type                  | No | "string"<br/>"set"<br/>"hash"<br/>"sorted_set"<br/>"mhash" | Data type to insert. Default "string" |
| password                         | No | | Password of redis |
| connection_pool_max_total        | No | | Jedis pool max total connection |
| connection_pool_max_idle         | No | | jedis pool max idle connection |
| connection_pool_min_idle         | No | | Jedis pool min idle connection |
| connection_pool_max_wait_time_ms | No | | Jedis pool max wait time in millis |
| max_attempt_count | No | | Retryer retry count |


## Related documents

Configuration examples: [Redis-v1 connector example](./redis-v1-example.md)
