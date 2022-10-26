# 环境配置

-----

## 前置条件

**Bitsail**支持在本地IDE运行集成测试，为此需要:

- JDK1.8
- maven 3.6+
- [Docker desktop](https://www.docker.com/products/docker-desktop/)

在安装上述必需组件后，您可以在本地的IDE上直接运行已有的集成测试。

## 从源代码编译
- Run the build script to package with flink embedded.
- 运行此脚本在编译时将Flink嵌入到BitSail的包中。
  `bash build.sh`
  If you have your own flink package provided by the cluster, you can also package without flink.
- 如果Flink已经在你们的集群中提供，则可以打包时只包含BitSail相关代码
  `mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true`

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