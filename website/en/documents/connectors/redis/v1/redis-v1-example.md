# Redis-v1 connector example

Parent document: [Redis-v1 connector](./redis-v1.md)

## Redis Writer example

Suppose we start a local Redis with port 6379.

Configuration for writing the Redis cluster is:

String

```json
{
  "job": {
    "common": {
      "job_id": -2413,
      "job_name": "bitsail_fake_to_redis_test",
      "instance_id": -20413,
      "user_name": "user"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource",
      "total_count": 300,
      "rate": 100000,
      "random_null_rate": 0,
      "unique_fields": "fake_key",
      "columns": [
        {
          "index": 0,
          "name": "fake_key",
          "type": "string"
        },
        {
          "index": 1,
          "name": "fake_value",
          "type": "string"
        }
      ]
    },
    "writer": {
      "class": "com.bytedance.bitsail.connector.redis.sink.RedisSink",
      "redis_data_type": "string",
      "redis_host": "localhost",
      "redis_port": 6379,
      "columns": [
        {
          "index": 0,
          "name": "fake_key",
          "type": "string"
        },
        {
          "index": 1,
          "name": "fake_value",
          "type": "string"
        }
      ]
    }
  }
}
```

Hash

```json
{
  "job": {
    "common": {
      "job_id": -2413,
      "job_name": "bitsail_fake_to_redis_test",
      "instance_id": -20413,
      "user_name": "user"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource",
      "total_count": 300,
      "rate": 100000,
      "random_null_rate": 0,
      "unique_fields": "fake_key",
      "columns": [
        {
          "index": 0,
          "name": "fake_key",
          "type": "string"
        },
        {
          "index": 1,
          "name": "fake_value",
          "type": "string"
        },
        {
          "index": 2,
          "name": "fake_hash_value",
          "type": "string"
        }
      ]
    },
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
  }
}
```



