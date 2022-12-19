# Redis-v1 连接器示例

父目录：[Redis-v1 连接器](./redis-v1.md)

## Redis 写连接器

假设在本地起了一个端口为6379的Redis。用于写入该Redis的配置如下:

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

