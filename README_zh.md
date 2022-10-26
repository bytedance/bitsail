# BitSail

## 介绍
BitSail是字节跳动开源的基于分布式架构的高性能数据集成引擎, 支持多种异构数据源间的数据同步，并提供离线、实时、全量、增量场景下的全域数据集成解决方案，目前服务于字节内部几乎所有业务线，包括抖音、今日头条等，每天同步数百万亿数据

## 为什么我们要使用BitSail
BitSail目前已被广泛使用,并支持数百万亿的大流量场景。同时在火山引擎云原生环境、客户私有云环境等多种场景下得到验证。

我们积累了很多经验，并做了多项优化，以完善数据集成的功能

- 全域数据集成解决方案, 覆盖离线、实时、增量场景
- 分布式以及云原生架构, 支持水平扩展
- 在准确性、稳定性、性能上，成熟度更好
- 丰富的基础功能，例如类型转换、脏数据处理、流控、数据湖集成、自动并发度推断等
- 完善的任务运行状态监控，例如流量、QPS、脏数据、延迟等

## BitSail使用场景
- 异构数据源海量数据同步
- 流批一体数据处理能力
- 湖仓一体数据处理能力
- 高性能、高可靠的数据同步
- 分布式、云原生架构数据集成引擎

## BitSail主要特点
- 简单易用，灵活配置
- 流批一体、湖仓一体架构，一套框架覆盖几乎所有数据同步场景
- 高性能、海量数据处理能力
- DDL自动同步
- 类型系统，不同数据源类型之间的转换
- 独立于引擎的读写接口，开发成本低
- 任务进度实时展示，正在开发中
- 任务状态实时监控

## BitSail架构
![](docs/images/bitsail_arch.png)

 ```
 Source[Input Sources] -> Framework[Data Transmission] -> Sink[Output Sinks]
 ```
数据处理流程如下，首先通过 Input Sources 拉取源端数据，然后通过中间框架层处理，最后通过 Output Sinks 将数据写入目标端

在框架层，我们提供了丰富的基础功能，并对所有同步场景生效，比如脏数据收集、自动并发度计算、流控、任务监控等

在数据同步场景上，全面覆盖批式、流式、增量场景

在Runtime层，支持多种执行模式，比如yarn、local，k8s在开发中

## 支持插件列表
<table>
  <tr>
    <th>DataSource</th>
    <th>Sub Modules</th>
    <th>Reader</th>
    <th>Writer</th>
  </tr>
  <tr>
    <td>Hive</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Hadoop</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td>Hbase</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Hudi</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Kafka</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>RocketMQ</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>StreamingFile (Hadoop Streaming mode.)</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Redis</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Doris</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>MongoDB</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Doris</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td rowspan="4">JDBC</td>
    <td>MySQL</td>
    <td rowspan="4">✅</td>
    <td rowspan="4">✅</td>
  </tr>
  <tr>
    <td>Oracle</td>
  </tr>
  <tr>
    <td>PostgreSQL</td>
  </tr>
  <tr>
    <td>SqlServer</td>
  </tr>
  <tr>
    <td>Fake</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td>Print</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
</table>

## 如何编译

下载代码后，可以执行

```
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true
```

然后可以在目录下`bitsail-dist/target/`找到相应的产物。
默认情况下产物中不会内嵌flink，如需内嵌flink，可以使用命令：

```
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true -Pflink-embedded
```

打包完成后，产物的目录结构如下:

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

## 环境配置

参考 [环境配置](docs/env_setup_zh.md).

## 部署指南

Link to [部署指南](docs/deployment_zh.md).

## 开发指南

Link to [开发指南](docs/developer_guide_zh.md).

## 联系方式

## 开源协议
[Apache 2.0 License](LICENSE).