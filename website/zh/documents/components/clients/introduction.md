# bitsail-component-clients

-----

上级文档: [bitsail-components](../README.md)

## 内容

在应用开发过程中，应用往往需要和各种各样的组件进行连接交互，例如Kafka、rds数据库等。
本模块用于提供连接多种大数据组件的客户端。
开发者可通过引入相应依赖后方便地创建客户端。

目前本模块提供如下客户端的创建：

| 子模块                               | 相关的大数据组件 | 支持的功能           | 链接                  |
|-----------------------------------|----------|-----------------|---------------------|
| `bitsail-components-client-kafka` | `Kafka`  | 创建KafkaProducer | [link](#jump_kafka) |

-----

## <span id="jump_kafka">子模块: bitsail-components-client-kafka</span>

### 依赖

```j
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-component-client-kafka</artifactId>
    <version>${revision}</version>
</dependency>
```

### 功能介绍

本模块基于`org.apache.kafka.clients.producer.KafkaProducer`提供了一个封装过的`KafkaProducer`，支持连接到指定的kafka集群，并以同步或者异步的方式发送数据到指定topic。

#### 构造函数

构造函数接受以下参数：
 1. `bootstrap.servers`: 连接kafka集群的地址
 2. `topic`: 要写入的topic
 3. `userConfigs`（可选）: 用户自定义的producer构建属性

此构造函数支持以下默认属性 (具体属性定义可查看org.apache.kafka.clients.producer.ProducerConfig):
 1. `acks`: all
 2. `retries`: 0
 3. `batch_size`: 16384
 4. `linger.ms`: 1000
 5. `buffer.memory`: 33554432
 6. `key.serializer`: org.apache.kafka.common.serialization.StringSerializer
 7. `value.serializer`: org.apache.kafka.common.serialization.StringSerializer

使用示例如下:
```
String bootstrapAddr = "localhost:9092";
String topic = "testTopic";
Map<String, String> userConfigs = ImmutableMap.of(
  "group.id", "test_group",
  "batch.size", 16384
);
KafkaProducer kafkaProducer = new KafkaProducer(bootstrapAddr, topic, userConfigs);
```

#### 同步/异步发送

开发者可使用同步发送，或者传入回调方法使用异步发送。方法分别如下：

 ```
// 同步发送
public Future<RecordMetadata> send(String value);
public Future<RecordMetadata> send(String value, int partitionId);
public Future<RecordMetadata> send(String key, String value);

// 异步发送
public Future<RecordMetadata> send(String value, Callback callback);
public Future<RecordMetadata> send(String value, int partitionId, Callback callback);
public Future<RecordMetadata> send(String key, String value, Callback callback);
 ```

#### 示例代码

如下是一份完整的、可运行的使用此KafkaProducer的示例代码，发送300条指定数据到topic中。
你可以在替换相关kafka集群参数后在本地测试运行。

```java
package com.bytedance.bitsail.component.client.kafka;

public class KafkaProducerExample {

  public static void main(String[] args) {
    assert args.length >= 2;
    String bootstrapServer = args[0];
    String topic = args[1];
    KafkaProducer producer = new KafkaProducer(bootstrapServer, topic);

    for (int i = 0; i < 300; ++i) {
      String key = "key_" + i;
      String value = "value_" + i;
      producer.send(key, value);
    }
    
    producer.close();
  }
}

```


