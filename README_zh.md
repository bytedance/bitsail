# BitSail

![](docs/images/bitsail.png)


## 介绍

*BitSail*是一款基于Flink引擎，同时支持流批数据同步的数据集成框架。
目前*BitSail*主要采用ELT的模型进行设计，承担了Bytedance内部EB量级的数据同步需求。<br/>

## 主要特点

- 内置类型系统，支持不同数据类型之间的转换。
- 基础功能插件化，可以按照需求定义自己场景的各类插件，如监控、脏数据收集等功能。
- 批式场景下支持DDL的同步操作，能够自动感知上游数据变化并反应到下游数据中。
- 流式场景下支持自动响应hive表DDL变化，无需作业重启。
- 流式场景下托管checkpoint，启动时自动选择符合要求的checkpoint路径。
- ...

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

## 部署

 - 在本地IDE运行BitSail，请参考[本地集成测试](docs/local_integration_test_zh.md)。
 - 在Yarn上部署BitSail，请参考[Yarn部署](docs/yarn_deployment_zh.md)。

## 开发

开发者可参考 [开发指南](docs/developer_guide_zh.md) 获取更好地开发指导和体验。

## 联系方式

## 开源协议

Apache 2.0 License

## 感谢

本项目的中参考了部分业界已经开源数据集成工具的优秀代码，特此向其表示感谢。<br/>
[DataX](https://github.com/alibaba/DataX)<br/>
[chunjun](https://github.com/DTStack/chunjun)<br/>




