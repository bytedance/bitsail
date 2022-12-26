# RocketMQ connector example

Parent document: [RocketMQ connector](./rocketmq.md)

## RocketMQ Writer example

Suppose we start a local rocketmq with name service "127.0.0.1:9876", and we create a topic "test_topic" in it.

Configuration for writing the rocketmq topic is:

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