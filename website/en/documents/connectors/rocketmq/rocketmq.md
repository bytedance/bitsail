# RocketMQ connector

Parent document: [Connectors](../README.md)

**BitSail** RocketMQ connector supports writing in batch mode.


## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-rocketmq</artifactId>
   <version>${revision}</version>
</dependency>
```

## RocketMQ writer

### Supported data types

 - int, bigint
 - float, double, decimal
 - timestamp, date
 - string, char
 - boolean
 - binary

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

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

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | Yes  |       | Class name of RocketMQ writer, `com.bytedance.bitsail.connector.legacy.rocketmq.sink.RocketMQOutputFormat` |
| name_server_address   | Yes  |       | Name server address of rocketmq |
| topic        | Yes  |       | Topic to write |
|columns| Yes | | Describing fields' names and types |



#### Optional parameters

| Param name             | Required | Optional value | Description                                                           |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------|
| writer_parallelism_num | No       |                | Writer parallelism num |
| producer_group | No | | Producer group for the task. If not defined, it will use a random string |
| tag | No | | Tags of the produced messages | 
| enable_batch_flush | No | | true: send a batch of messages at once;<br/>false: send single message at once.<br/> Default true. |
| batch_size | No | | The number of messages to send in a batch. Default 100 |
| log_failures_only | No | | When send failure happens, it will:<br/>true: only log this failure,<br/>false: throw exception.<br/>Default false. |
| enable_sync_send | No | | If use sync send. Default false. |
| access_key | No | | Access key for authorization. |
| secret_key | No | | Secret key for authorization. |
| send_failure_retry_times | No | | Max retry times for a send failure, default 3 |
| send_message_timeout_ms | No | | Timeout for sending a message, default 3000 ms |
| max_message_size_bytes | No | | Max message size, default 4194304 bytes |
| key | No | | Specify which field(s) is used as the message key.|
| partition_fields | No | | Specify which field(s) is used to select queue. |




## Related documents

Configuration examples: [RocketMQ connector example](./rocketmq-example.md)
