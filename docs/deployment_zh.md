## 部署指南

> 目前 BitSail 仅支持在Yarn上部署。
> 其他平台上的部署（例如原生kubernetes）将在不久后支持。

-----

下面是这部分的目录:

- [配置Hadoop](#jump_configure_hadoop)
- [配置Flink](#jump_configure_flink)
- [提交到Yarn](#jump_submit_to_yarn)
- [提交一个示例作业](#jump_submit_example)
- [调试日志](#jump_log)


下面各部分详细介绍了如何将BitSail在Yarn上进行部署。

## <span id="jump_configure_hadoop">配置Hadoop</span>

为了支持Yarn部署，需要在环境变量中配置`HADOOP_CLASSPATH`。目前有两种方式设置:

1. 直接手动设置 `HADOOP_CLASSPATH`。

2. 设置环境变量 `HADOOP_HOME`。此环境变量指向环境中使用的hadoop目录。根据此环境变量，[bitsail](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/bin/bitsail) 脚本可生成 `HADOOP_CLASSPATH`。

  ```shell
  if [ -n "$HADOOP_HOME" ]; then
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
  fi
  ```

## <span id="jump_configure_flink">配置Flink</span>

打包完成后，产物中包含可配置flink的文件 [conf/bitsail.conf](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/resources/bitsail.conf) 。
这个文件描述了系统中使用的flink环境，包括flink所在目录以及其他默认参数。

下面是一些常用的配置项:


<table>
  <tr>
    <th>参数前缀</th>
    <th>参数名称</th>
    <th>参数描述</th>
    <th>示例</th>
  </tr>

  <tr>
    <td rowspan="3">sys.flink.</td>
    <td>flink_home</td>
    <td>使用的flink所在目录.</td>
    <td>${BITSAIL_HOME}/embedded/flink</td>
  </tr>

  <tr>
    <td>checkpoint_dir</td>
    <td>存储flink checkpoint元数据和数据文件的路径。详情参考:<a href="https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/">Flink Checkpoints</a></td>
    <td>"hdfs://opensource/bitsail/flink-1.11/checkpoints/"</td>
  </tr>

  <tr>
    <td>flink_default_properties</td>
    <td>通用的flink运行参数，以 "-D xxx=xxx" 方式传递。</td>
    <td>{<br/>
        classloader.resolve-order: "child-first"<br/>
        akka.framesize: "838860800b"<br/>
        rest.client.max-content-length: 838860800<br/>
        rest.server.max-content-len<br/>}
    </td>
  </tr>
</table>


## <span id="jump_submit_to_yarn">提交到Yarn</span>
 
> ***BitSail*** 目前仅支持flink的 `yarn-per-job` 模式提交。

你可以使用 `bin/bitsail` 脚本将flink作业提交到yarn上。具体的执行指令如下:

``` bash
bash ./bin/bitsail run --engine flink --conf [job_conf_path] --execution-mode run --queue [queue_name] --deployment-mode yarn-per-job [--priority [yarn_priority] -p/--props [name=value]] 
```

上面中括号内的参数说明如下：

 - 必需参数: 
    - **queue_name**: 要提交的yarn队列
    - **job_conf_path**: 作业的配置文件
 - 可选参数:
    - **yarn_priority**: 作业在队列上的优先级
    - **name=value**: flink运行属性，以 "-D name=value" 方式添加在flink run命令后
        - **name**: 要添加的属性名
        - **value**: 要添加的属性值
        - 例如 `classloader.resolve-order=child-first`

## <span id="jump_submit_example">提交一个示例作业</span>

可以使用如下指令提交一个 Fake2Print 作业到测试队列。

``` bash
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-1.0.0-SNAPSHOT/examples/Fake_Proint_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```

## <span id="jump_log">调试日志</span>

### client端日志

可以在 `${FLINK_HOME}/log/` 中找到BitSail client端的执行日志。

### Yarn作业日志

可以通过Yarn的WebUI来查看Flink JobManager和TaskManager的日志。

