# 快速开始

***BitSail*** 是一款基于Flink引擎，同时支持流批数据同步的数据集成框架。
目前 ***BitSail*** 主要采用ELT的模型进行设计，承担了Bytedance内部EB量级的数据同步需求。

## 从源代码编译
- 运行编译脚本
```
bash build.sh
```
- 编译完成后，产出文件储存在此目录下 `bitsail-dist/target/bitsail-dist-1.0.0-SNAPSHOT-bin/bitsail-archive-1.0.0-SNAPSHOT`

## 运行本地集成测试
### 前置要求
- JDK8
- maven 3.6+
- Docker desktop: https://www.docker.com/products/docker-desktop/

After the prerequisite package be installed correctly. We will be able to run the integration tests on your Local IDE.
前置要求安装完毕后，我们可以在本地IDE中运行集成测试
比如, `com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceITCase`. 这个测试在container中模拟了kafka集群，
并运行BitSail任务消费数据，通过Print Sink打印出来。


## 查看项目产物目录

解压文件夹后，项目产物文件结构如下：

``` simple
bitsail-archive-1.0.0-SNAPSHOT    
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

## 提交脚本使用说明

***BitSail*** 目前只支持 yarn 模式提交，故下面介绍 yarn 模式下的提交使用。

### 环境变量
在启动任务前，我们需要确认环境变量 `HADOOP_HOME` 已经配置好。如果没有，请运行
`export HADOOP_HOME=xxx/xxx/xxx`

### 配置BitSail任务参数

构造任务配置，详见 [任务配置说明](config_zh.md)。

样本配置文件在此目录下 `/examples/XXX_XXX_Example.json`

### 配置Flink引擎参数

在路径 `conf/bitsail.conf` 中配置系统配置，包含 flink 路径以及系统使用的默认参数等配置。

### 提交任务
> 注意，当前代码仅支持了yarn的`yarn-per-job`模式，其余的运行模式(包含`native kubernetes`)还在支持当中，会在近期进行release发布

可以使用启动脚本 `bin/bitsail` 来向 yarn 提交 flink 作业，具体命令如下：

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

## Debugging
### Client日志文件
`embedded/flink/log/` 目录下可以找到client侧的运行日志。
### Yarn日志文件
在Yarn WebUI中可以找到Flink JobManager和TaskManager的日志。

## 提交一个示例任务
提交一个Fake source到Print sink的任务到yarn集群。
``` bash
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-1.0.0-SNAPSHOT/examples/Fake_Hudi_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```