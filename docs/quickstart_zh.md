# 快速开始

***BitSail*** 是一款基于Flink引擎，同时支持流批数据同步的数据集成框架。
目前 ***BitSail*** 主要采用ELT的模型进行设计，承担了Bytedance内部EB量级的数据同步需求。

## 下载 BitSail

***BitSail*** 运行在类 Unix 环境中，也就是 Linux、Mac OS X 中。本地需要安装 Java 8。下载 ***BitSail*** 最新的二进制发布包并解压压缩包：

``` shell
tar -xzf bitsail-*.tar.gz
```

## 查看项目产物目录

解压文件夹后，项目产物文件结构如下：

``` simple
-- bin  
    \-- bitsail  提交启动脚本
-- conf
    \-- bitsail.conf bitsail 系统配置
-- embedded
    \-- flink 内嵌flink
-- examples 运行样例配置
    \-- example-datas 运行样例数据
    \-- Fake_xx_Example.json Fake source 导入到 xx 的样例配置文件
    \-- xx_Print_Example.json xx 导入到 print sink 的样例配置文件
-- libs 运行需要的jar包
    \-- bitsail-core.jar 入口 jar 包
    \-- connectors 各个connector plugin实现jar包
        \-- mapping connector plugin 配置文件
    \-- components 各个组件的实现包，例如metric、dirty-collector
    \-- clients bitsail client jar 包
```

## 提交脚本使用说明

***BitSail*** 目前只支持 yarn 模式提交，故下面介绍 yarn 模式下的提交使用。

### 配置相关组件信息

在路径 `conf/bitsail.conf` 中配置系统配置，包含 flink 路径以及系统使用的默认参数等配置。

``` json
BITSAIL {
  sys {
    flink {
      flink_home: ${BITSAIL_HOME}/embedded/flink
      checkpoint_dir: "hdfs://opensource/bitsail/flink-1.11/checkpoints/"
      flink_default_properties: {
        classloader.resolve-order: "child-first"
        akka.framesize: "838860800b"
        rest.client.max-content-length: 838860800
        rest.server.max-content-length: 838860800
        slot.request.timeout: 28800000
        slotmanager.request-timeout: 28800000
        heartbeat.timeout: 180000
        akka.watch.heartbeat.pause: "181s"
        akka.ask.timeout: "182s"
        akka.client.timeout: "183s"
        akka.lookup.timeout: "10min"
        web.timeout: 600000
        blob.client.socket.timeout: 60000
        flink-client-classpath-include-user-jar: "A"
        blob.fetch.num-concurrent: 32
        resourcemanager.maximum-workers-failure-rate-ratio: 2
        resourcemanager.maximum-workers-failure-rate: 50
        resourcemanager.workers-failure-interval: 28800000
        taskmanager.network.request-backoff.max: 40000
        task.cancellation.timeout: 600000
        taskmanager.network.netty.client.readTimeout.enabled: false
        yarn.application-attempts: 1
      }
    }
  }
}
```

### 配置任务配置

构造任务配置，详见 [任务配置说明](config_zh.md)。

### 提交任务
> 注意，当前代码仅支持了yarn的`yarn-per-job`模式，其余的运行模式(包含`native kubernetes`)还在支持当中，会在近期进行release发布

可以使用启动脚本 `bin/bitsail` 来向 yarn 提交 flink 作业，具体命令如下：

提交作业前，需要首先设置环境变量`HADOOP_HOME`。

``` bash
bash ./bin/bitsail run --engine flink --conf [job_conf_path] --execution-mode run --queue [queue_name] --deployment-mode yarn-per-job [--priority [yarn_priority] -p/--props [name=value]] 
```

参数说明（目前仅支持按顺序读取参数）

* 必需参数
  * queue_name：目标yarn队列
  * job_conf_path：任务配置文件地址
* 可选参数
  * yarn_priority： yarn 优先级
  * name=value：示例  classloader.resolve-order=child-first
    * name: property 名称，可配置 flink 参数，会透传到 flink 任务
    * value: property 值.