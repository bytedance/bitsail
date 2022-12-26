# RocketMQ 连接器示例

父目录：[RocketMQ 连接器](./rocketmq.md)

## RocketMQ 写连接器

假设我们在本地启动了一个RocketMQ服务，其name server地址为 "127.0.0.1:9876", 并且我们在其中创建了一个名为 "test_topic" 的topic。

那么我们可以使用如下的配置文档写入上述topic：

```json
{
   "job": {
     "writer": {
       "class": "com.bytedance.bitsail.connector.legacy.rocketmq.sink.RocketMQOutputFormat",
       "name_server_address": "127.0.0.1:9876",
       "topic": "test_topic",
       
       "producer_group": "test_producer_group",
       "tag": "itcase_test",
       "key": "id",
       "partition_fields": "id,date_field",
       
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
         },
         {
           "index": 2,
           "name": "int_field",
           "type": "bigint"
         },
         {
           "index": 3,
           "name": "double_field",
           "type": "double"
         },
         {
           "index": 4,
           "name": "date_field",
           "type": "date"
         }
       ]
     }
   }
}
```