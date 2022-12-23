# Redis 连接器示例

父目录：[Redis 连接器](./redis.md)

## Redis 写连接器

假设在本地起了一个端口为6379的Redis。用于写入该Redis的配置如下:

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