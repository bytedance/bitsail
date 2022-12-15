# bitsail-component-clients

-----

Parent document: [bitsail-components](../README.md)

## Content

In the process of application development, applications often need to connect and interact with various components, such as Kafka, rds, <i>etc.</i>. This module is used to provide clients that connect to various big data components. Developers can easily create clients by introducing corresponding dependencies.

Currently, this module provides the following clients:

| Name                              | Component | Function             | Link                |
|-----------------------------------|-----------|----------------------|---------------------|
| `bitsail-components-client-kafka` | `Kafka`   | Create KafkaProducer | [link](#jump_kafka) |

-----

## <span id="jump_kafka">bitsail-components-client-kafka</span>

### Maven dependency

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-component-client-kafka</artifactId>
    <version>${revision}</version>
</dependency>
```

### Features

This module is based on `org.apache.kafka.clients.producer.KafkaProducer` to providing a wrapped KafkaProducer that supports connecting to a specified kafka cluster and sending data to a specified topic in a synchronous or asynchronous manner.


#### Constructor

The constructor accepts the following parameters:
1. `bootstrap.servers`: The address to connect to the kafka cluster
2. `topic`: kafka topic
3. `userConfigs`(optional): User-defined producer build properties

This constructor supports the following default properties (Refer to `org.apache.kafka.clients.producer.ProducerConfig` for detailed property definitions):

1. `acks`: all
2. `retries`: 0
3. `batch_size`: 16384
4. `linger.ms`: 1000
5. `buffer.memory`: 33554432
6. `key.serializer`: org.apache.kafka.common.serialization.StringSerializer
7. `value.serializer`: org.apache.kafka.common.serialization.StringSerializer

The usage example is as follows:
```
String bootstrapAddr = "localhost:9092";
String topic = "testTopic";
Map<String, String> userConfigs = ImmutableMap.of(
  "group.id", "test_group",
  "batch.size", 16384
);
KafkaProducer kafkaProducer = new KafkaProducer(bootstrapAddr, topic, userConfigs);
```

#### Synchronous/Asynchronous send

Developers can use synchronous send, or pass in a callback method to use asynchronous send. The send methods are as follows:

 ```
// Sync
public Future<RecordMetadata> send(String value);
public Future<RecordMetadata> send(String value, int partitionId);
public Future<RecordMetadata> send(String key, String value);

// Async
public Future<RecordMetadata> send(String value, Callback callback);
public Future<RecordMetadata> send(String value, int partitionId, Callback callback);
public Future<RecordMetadata> send(String key, String value, Callback callback);
 ```

#### Example code

The following is a complete and runnable sample code using this KafkaProducer to send 300 pieces of specified data to a topic. You can test the run locally after substituting the relevant kafka cluster parameters.

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


