# RocketMQ 连接器

上级文档：[连接器](../README.md)

**BitSail** RocketMQ 连接器支持写指定的 RocketMQ topic。

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-rocketmq</artifactId>
   <version>${revision}</version>
</dependency>
```

## RocketMQ写入

### 支持数据类型

- int, bigint
- float, double, decimal
- timestamp, date
- string, char
- boolean
- binary

### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.rocketmq.sink.RocketMQOutputFormat",
      "name_server_address": "127.0.0.1:9876",
      "producer_group": "test_producer_group",
      "topic": "test_topic"
    }
  }
}
```

#### 必需参数

| 参数名称              | 是否必填 | 参数枚举值 | 参数含义                                                                                      |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | 是  |       | RocketMQ写连接器类型, `com.bytedance.bitsail.connector.legacy.rocketmq.sink.RocketMQOutputFormat` |
| name_server_address   | 是  |       | RocketMQ的name server地址 |
| topic        | 是  |       | 要写入的topic |
|columns| 是 | | 指定写入的字段名和字段类型 |



#### 可选参数

| 参数名称                                    | 是否必填  | 参数枚举值 | 参数含义                                                 |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| writer_parallelism_num | 否       |                | 指定RocketMQ写并发  |
| producer_group | 否 | | 任务的生产组。若不指定，则为一随机字符串 |
| tag | 否 | | 生产消息的tag | 
| enable_batch_flush | 否 | | 是否开启batch发送。默认 true |
| batch_size | 否 | | Batch发送的数量，默认100 |
| log_failures_only | 否 | | 当send出错时:<br/>1. true: 仅日志打印错误<br/>2. false: 抛出异常<br/>默认 false |
| enable_sync_send | 否 | | 是否使用同步发送。默认 false |
| access_key | 否 | | 用于鉴权的Access key |
| secret_key | 否 | | 用于鉴权的Secret key  |
| send_failure_retry_times | 否 | | 最大失败重试次数, 默认 3 |
| send_message_timeout_ms | 否 | | 消息发送的最大超时, 默认 3000 ms |
| max_message_size_bytes | 否 | | 最大消息体积, 默认 4194304 bytes |
| key | 否 | | 指定column中的一个或几个字段作为消息的key |
| partition_fields | 否 | | 指定column中的一个或几个字段用于选择消息发送到的queue  |

## 相关文档

配置示例文档：[RocketMQ 连接器示例](./rocketmq-example.md)
