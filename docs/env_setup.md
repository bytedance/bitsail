# Environment Setup

-----

## Prerequisite

**Bitsail** supports run integration tests on local IDE. To achieve that, you need:

 - JDK8
 - maven 3.6+
 - [Docker desktop](https://www.docker.com/products/docker-desktop/)

After correctly installing the above required components, we are able to run integration tests on your local IDE.


## Example

In [`bitsail-connector-test`](https://github.com/bytedance/bitsail/tree/master/bitsail-test/bitsail-connector-test) module, we provide the [EmbeddedFlinkCluster](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-connector-test/src/main/java/com/bytedance/bitsail/test/connector/test/EmbeddedFlinkCluster.java) class that can be used to start a job in local Flink MiniCluster.


For example, we build an integration test [KafkaSourceITCase](https://github.com/bytedance/bitsail/blob/master/bitsail-connectors/bitsail-connectors-legacy/bitsail-connector-kafka/src/test/java/com/bytedance/bitsail/connector/legacy/kafka/source/KafkaSourceITCase.java) for Kafka source connector.
It uses [test container](https://www.testcontainers.org/modules/kafka/) to start kafka service in local docker.
We can run the `testKafkaSource` method to start a kafka2print task in local flink MiniCluster.

```java
public class KafkaSourceITCase {
  // ...

  @Test
  public void testKafkaSource() throws Exception {
    BitSailConfiguration configuration = JobConfUtils.fromClasspath("kafka_to_print.json");
    updateConfiguration(configuration);
    EmbeddedFlinkCluster.submitJob(configuration);
  }
  
  // ...
}
```