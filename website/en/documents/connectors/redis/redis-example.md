# Redis connector example

Parent document: [Redis connector](./redis.md)

## Redis Writer example

Suppose we start a local Redis with port 6379.

Configuration for writing the Redis cluster is:

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