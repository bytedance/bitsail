# Redis 连接器

上级文档：[连接器](../README.md)

**BitSail** Redis 连接器支持写 Redis 库，主要功能点如下:

- 支持批式写入Redis
- 支持写入多种格式


## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-redis</artifactId>
   <version>${revision}</version>
</dependency>
```

## Redis写入

### 支持数据类型

目前支持以Redis中的String, Set, Hash, Sorted Set四种格式写入。
每种格式对要写入的数据的具体要求如下：

| 数据类型 | 要求的列数 | 第一列 | 第二列 | 第三列 |
| ------- | ------- | ----- | ---- | ------ |
| String | 2 | key | value | |
| Set | 2 | key | 要插入到Set中的value |
| Hash | 3 | key | hash中的key | hash中的value |
| Sorted Set (Zset) | 3 | key | score | 要插入到Set中的value |

注意: 如果字段超过3个可以使用 `Mhash`。
### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

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

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | Redis写连接器类型, `com.bytedance.bitsail.connector.legacy.redis.sink.RedisOutputFormat` |
| redis_host   | 是  |       | Redis连接地址                                                           |
| redis_port        | 是  |       | Redis连接端口                                               |



#### 可选参数

| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| writer_parallelism_num | 否 |       | 指定redis写并发                       |
| client_timeout_ms | 否 | | Redis的连接/请求超时, 默认60000ms |
| ttl | 否 | | 写入数据的ttl, 默认-1表示不设置 |
| ttl_type | 否  | "DAY", "HOUR", "MINUTE", "SECOND" |  上面指定的ttl单位, 默认"DAY" |
| write_batch_interval | 否 | | Redis指令攒批写入的大小, 默认50 | 
| redis_data_type | 否 | "string"<br/>"set"<br/>"hash"<br/>"sorted_set"<br/>"mhash" | 写入Redis的数据格式, 默认 string | 
| password | 否 | | Redis连接密码 | 


## 相关文档

配置示例文档：[Redis 连接器示例](./redis-example.md)
