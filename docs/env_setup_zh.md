# 环境配置

-----

## 前置条件

**Bitsail**支持在本地IDE运行集成测试，为此需要:

- JDK8
- maven 3.6+
- [Docker desktop](https://www.docker.com/products/docker-desktop/)

在安装上述必需组件后，您可以在本地的IDE上直接运行已有的集成测试。



## 示例

在 [`bitsail-connector-test`](https://github.com/bytedance/bitsail/tree/master/bitsail-test/bitsail-connector-test) 模块中，我们提供了 [EmbeddedFlinkCluster](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-connector-test/src/main/java/com/bytedance/bitsail/test/connector/test/EmbeddedFlinkCluster.java) 类用于启动一个本地运行的flink作业。

例如，我们为Kafka读连接器构建了一个集成测试 [KafkaSourceITCase](https://github.com/bytedance/bitsail/blob/master/bitsail-connectors/bitsail-connectors-legacy/bitsail-connector-kafka/src/test/java/com/bytedance/bitsail/connector/legacy/kafka/source/KafkaSourceITCase.java) 。
在这个测试中，首先会使用 [test container](https://www.testcontainers.org/modules/kafka/) 在本地docker中启动Kafka服务。此后便可以通过 `testKafkaSource` 方法来启动一个本地的 kafka2print flink作业。

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