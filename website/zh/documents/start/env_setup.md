---
order: 2
---

# 开发环境配置

[English](../../../en/documents/start/env_setup.md) | 简体中文

-----

## 前置条件

**Bitsail**支持在本地IDE运行集成测试，为此需要:

- JDK1.8
- maven 3.6+
- [Docker desktop](https://www.docker.com/products/docker-desktop/)

在安装上述必需组件后，您可以在本地的IDE上直接运行已有的集成测试。

## 从源代码编译

### 适配hive环境

BitSail使用`bitsail-shaded-hive`模块来管理hive依赖，在其中使用**3.1.0**作为默认hive版本。
因此，用户如果想在其他版本的hive环境中部署和使用BitSail，需要先修改 [bitsail-shaded-hive](https://github.com/bytedance/bitsail/blob/master/bitsail-shade/bitsail-shaded-hive/pom.xml) 中的hive版本信息（如下图所示）。

![](../../../images/change-hive-version.png)

### 打包&产物结构

- 运行此脚本在编译时将Flink嵌入到BitSail的包中。`bash build.sh`。如果Flink已经在你们的集群中提供，则可以打包时只包含BitSail相关代码 `mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true`

完成打包后，输出的文件在此目录下`bitsail-dist/target/`.

产物的目录结构如下:

``` simple
bitsail-archive-${version}-SNAPSHOT    
    /bin  
        /bitsail #提交启动脚本
    /conf
        /bitsail.conf #bitsail 系统配置
    /embedded
        /flink #内嵌flink
    /examples #e运行样例配置
        /example-datas #运行样例数据
        /Fake_xx_Example.json #Fake source 导入到 xx 的样例配置文件
        /xx_Print_Example.json #xx 导入到 print sink 的样例配置文件
    /libs #运行需要的jar包
        /bitsail-core.jar #入口 jar 包
        /connectors #connector plugin实现jar包
            /mapping #connector plugin 配置文件
        /components #各个组件的实现包，例如metric、dirty-collector
        /clients #bitsail client jar 包
```

## 运行本地集成测试

在 [`bitsail-test-integration`](https://github.com/bytedance/bitsail/tree/master/bitsail-test/bitsail-test-integration) 模块中，我们提供了 [Flink11Engine](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-test-integration/bitsail-test-integration-base/src/main/java/com/bytedance/bitsail/test/integration/engine/flink/Flink11Engine.java) 类用于启动一个本地运行的flink作业。

例如，我们为Kafka读连接器构建了一个集成测试 [KafkaSourceITCase](https://github.com/bytedance/bitsail/blob/master/bitsail-test/bitsail-test-integration/bitsail-test-integration-connector-legacy/bitsail-test-integration-kafka-legacy/src/test/java/com/bytedance/bitsail/test/integration/legacy/KafkaSourceITCase.java) 。
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
