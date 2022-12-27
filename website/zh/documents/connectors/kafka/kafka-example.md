# Kafka 连接器示例

上级文档：[Kafka 连接器](./kafka.md)

## 测试 Kafka 配置示例

假设测试使用的 Kafka 配置如下：

 - `bootstrap.servers`: PLAINTEXT://localhost:9092
 - `topic`: test_topic
 - `group_id`: test_consumer_group

## Kafka 读连接器

```json
{
  "job": {
    "reader":{
      "connector":{
        "connector":{
          "bootstrap.servers":"PLAINTEXT://localhost:9092",
          "topic":"test_topic",
          "startup-mode":"earliest-offset",
          "group":{
            "id":"test_consumer_group"
          }
        }
      },
      "child_connector_type":"kafka",
      "format_type": "json",
      "columns": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "state",
          "type": "string"
        },
        {
          "name": "county",
          "type": "string"
        }
      ],
      "class":"com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceFunctionDAGBuilder"
    }
  }
}
```

## Kafka 写连接器

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.kafka.sink.KafkaOutputFormat",
      "kafka_servers": "PLAINTEXT://localhost:9092",
      "topic_name": "test_topic",
      "writer_parallelism_num": 3,
      "log_failures_only": true,
      "columns": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "state",
          "type": "string"
        },
        {
          "name": "county",
          "type": "string"
        }
      ]
    }
  }
}
```

