---
order: 2
---

# Develop Environment Setup

English | [简体中文](../../../zh/documents/start/env_setup.md)

-----

## Prerequisite

**Bitsail** supports run integration tests on local IDE. To achieve that, you need:

- JDK1.8
- maven 3.6+
- [Docker desktop](https://www.docker.com/products/docker-desktop/)

After correctly installing the above required components, we are able to run integration tests on your local IDE.

## Build From Source Code

### Change Hive Version

BitSail uses a shaded module, namely bitsail-shaded-hive, to import hive dependencies.
By default, BitSail uses **3.1.0** as hive version.
Therefore, if you want to deploy BitSail in different hive environment, you can modify the `hive.version` property in [bitsail-shaded-hive](https://github.com/bytedance/bitsail/blob/master/bitsail-shade/bitsail-shaded-hive/pom.xml).

![](../../../images/change-hive-version.png)


### Package & Structure

- Run the build script to package with flink embedded.
  `bash build.sh`
  If you have your own flink package provided by the cluster, you can also package without flink.
  `mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true`

After building the project, the output jar files are in the folder `bitsail-dist/target/`.

The project production file structure is as follows:

``` simple
bitsail-archive-${version}-SNAPSHOT    
    /bin  
        /bitsail #Startup script
    /conf
        /bitsail.conf #bitsail system config
    /embedded
        /flink #embedded flink
    /examples #examples configuration files
        /example-datas #examples data
        /Fake_xx_Example.json #Fake source to xx examples config files
        /xx_Print_Example.json #xx to print sink examples config files
    /libs #jar libs
        /bitsail-core.jar #entering jar package
        /connectors #connector plugin jars
            /mapping #connector plugin config files
        /components #components jars，such as metric、dirty-collector
        /clients #bitsail client jar
```

## Run Local Integration Tests

In [`bitsail-test-integration`](https://github.com/bytedance/bitsail/tree/master/bitsail-test/bitsail-test-integration) module, we provide the [Flink11Engine](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-test-integration/bitsail-test-integration-base/src/main/java/com/bytedance/bitsail/test/integration/engine/flink/Flink11Engine.java) class that can be used to start a job in local Flink MiniCluster.


For example, we build an integration test [KafkaSourceITCase](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-test-integration/bitsail-test-integration-connector-legacy/bitsail-test-integration-kafka-legacy/src/test/java/com/bytedance/bitsail/test/integration/legacy/KafkaSourceITCase.java) for Kafka source connector.
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