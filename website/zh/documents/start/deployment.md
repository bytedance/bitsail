---
order: 1
---

# <span id="jump_deployment_guide">部署指南</span>

[English](../../../en/documents/start/deployment.md) | 简体中文

-----

> 目前 BitSail 支持本地和Yarn和原生kubernetes部署。

本部分目录:

- [部署指南](#jump_deployment_guide)
  - [环境配置](#jump_pre_configure)
    - [配置Hadoop](#jump_configure_hadoop)
    - [配置Flink](#jump_configure_flink)
  - [提交到Yarn](#jump_submit_to_yarn)
    - [提交一个示例作业](#jump_submit_example)
    - [调试日志](#jump_log)
  - [远程提交](#jump_remote_submit)
  - [本地提交](#jump_local_submit)
  - [部署原生Kubernetes](#cn_native_kubernetes_deployment)
    - [要求](#cn_jump_prerequisites_k8s)
    - [前置作业](#cn_jump_pre_configuration_k8s)
        - [建立RBAC鉴权](#cn_jump_configure_RBAC)
    - [Application模式](#cn_jump_application_mode)
        - [自定义 Flink Docker 镜像](#cn_jump_build_custom_flink_image)
        - [启动Application](#cn_jump_start_application)
        - [停止Application](#cn_jump_stop_application)
        - [Kubernetes日志文件](#cn_jump_kubernetes_logs)
        - [History Server](#cn_jump_history_server)


下面各部分详细介绍BitSail的部署。

-----

## <span id="jump_pre_configure">环境配置</span>

### <span id="jump_configure_hadoop">配置Hadoop</span>

为了支持Yarn/Kubernetes部署，需要在环境变量中配置`HADOOP_CLASSPATH`。目前有两种方式设置:

1. 直接手动设置 `HADOOP_CLASSPATH`。

2. 设置环境变量 `HADOOP_HOME`。此环境变量指向环境中使用的hadoop目录。根据此环境变量，[bitsail](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/bin/bitsail) 脚本可生成 `HADOOP_CLASSPATH`。

  ```shell
  if [ -n "$HADOOP_HOME" ]; then
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
  fi
  ```

### <span id="jump_configure_flink">配置Flink</span>

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

-----

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

### <span id="jump_submit_example">提交一个示例作业</span>

可以使用如下指令提交一个 Fake2Print 测试作业到default队列。

``` bash
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-0.1.0-SNAPSHOT/examples/Fake_Proint_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```

### <span id="jump_log">调试日志</span>

#### client端日志

可以在 `${FLINK_HOME}/log/` 中找到BitSail client端的执行日志。

#### Yarn作业日志

可以通过Yarn的WebUI来查看Flink JobManager和TaskManager的日志。

-----

## Flink提交


假设BitSail的安装路径为: `${BITSAIL_HOME}`。打包BitSail后，我们可以在如下路径中找到可运行jar包以及示例作业配置文件:

```shell
cd ${BITSAIL_HOME}/bitsail-dist/target/bitsail-dist-0.1.0-SNAPSHOT-bin/bitsail-archive-0.1.0-SNAPSHOT/
```

### <span id="jump_remote_submit">远程提交</span>

用户可以通过 `--deployment-mode remote` 选项来将作业提交到指定的flink session。以 [examples/Fake_Print_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Print_Example.json) 为例，可以通过如下指令进行提交:

- `<job-manager-address>`: 要连接的的JobManager地址，格式为host:port，例如`localhost:8081`。

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode remote \
  --conf examples/Fake_Print_Example.json \
  --jm-address <job-manager-address>
```

例如，使用`bitsail-archive-0.1.0-SNAPSHOT/embedded/flink/bin/start-cluster.sh`脚本可以在本地启动一个flink standalone集群，此时 `<job-manager-address>` 就是 `localhost:8081`。

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode remote \
  --conf examples/Fake_Print_Example.json \
  --jm-address localhost:8081
```
执行命令后，可以在Flink WebUI中查看运行的Fake_to_Print作业。在task manager的stdout文件中可以看到作业输出。

### <span id="jump_local_submit">本地提交</span>

用户可以通过 `--deployment-mode local` 选项在本地运行作业。以 [examples/Fake_Print_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Print_Example.json) 为例，可以通过如下指令进行提交:

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode local \
  --conf examples/Fake_Print_Example.json
```


#### 运行Fake_to_Hive示例作业
以 [examples/Fake_hive_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Hive_Example.json) 为例:
- 在运行前补充完整配置文件中的hive信息:
    - `job.writer.db_name`: 要写入的hive库.
    - `job.writer.table_name`: 要写入的hive表.
    - `job.writer.metastore_properties`: hive的连接信息，包括metastore地址等:
    ```shell
       {
          "job": {
            "writer": {
              "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}"
            }
          }
       }
    ```

执行如下命令，便可以在本地启动一个Fake_to_Hive作业:

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode local \
  --conf examples/Fake_Hive_Example.json
  ```

#### 运行hadoop相关任务

如果读或者写数据源与hadoop相关，例如`hive_to_print`任务，那么需要向本体的flink mini cluster提供hadoop lib。
下面介绍两种提供hadoop lib的方法:

1. 如果你的环境已经部署了hadoop，那么直接通过`$HADOOP_HOME`环境变量指向本地的hadoop目录即可，例如:

```bash
export HADOOP_HOME=/usr/local/hadoop-3.1.1
```

2. 如果本地没有hadoop环境，可以通过`flink-shaded-hadoop-uber` jar包提供hadoop lib。例如，假设flink的目录为 `/opt/flink`，那么可以通过如下命令添加`flink-shaded-hadoop-uber`包:

```bash
# download flink-shaded-hadoop-uber jar
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

# move to flink libs
mv flink-shaded-hadoop-2-uber-2.7.5-10.0.jar /opt/flink/lib/flink-shaded-hadoop-uber.jar
```



-----
# <span id="cn_native_kubernetes_deployment">部署原生Kubernetes</span>

> 目前***BitSail***仅支持在Flink 1.11引擎上的原生Kubernetes.<br>
>
以下的内容将一步步带领你部署BitSail到原生Kubernetes。目前BitSail支持Application模式：让使用者去创建一个单一镜像去包住任务和Flink runtime。这个镜像将会在kubernetes cluster自动创建及退出删除。

## <span id="cn_jump_prerequisites_k8s">要求</span>
1. Kubernetes 版本 1.9 或以上。
2. KubeConfig 可以查看、创建、删除 pods 和 services，可以通过~/.kube/config 配置。你可以通过运行 kubectl auth can-i <list|create|edit|delete> pods 来验证权限。
3. 启用 Kubernetes DNS。

若您有创见kubernetes集群相关的问题，可以查看[如何创建Kubernetes集群](https://kubernetes.io/zh-cn/docs/setup/).

## <span id="cn_jump_pre_configuration_k8s">前置作业</span>

### <span id="cn_jump_configure_RBAC">建立 [RBAC鉴权](https://kubernetes.io/zh-cn/docs/reference/access-authn-authz/rbac/)</span>
基于角色的访问控制（RBAC）是一种在企业内部基于单个用户的角色来调节对计算或网络资源的访问的方法。 用户可以配置 RBAC 角色和服务账户，JobManager 使用这些角色和服务帐户访问 Kubernetes 集群中的 Kubernetes API server。

如果你不想使用默认服务账户，使用以下命令创建一个新的 <自定义帐户名称> 服务账户并设置角色绑定。然后使用配置项 -p kubernetes.jobmanager.service-account=<自定义账户名称> 来使 JobManager pod 使用 <自定义帐户名称> 服务账户去创建和删除 TaskManager pod。
每个命名空间有默认的服务账户，但是默认服务账户可能没有权限在 Kubernetes 集群中创建或删除 pod。用户可能需要更新默认服务账户的权限或指定另一个绑定了正确角色的服务账户。

```bash
$ kubectl create serviceaccount <self-defined-service-account> # 请把<self-defined-service-account>替换成实际的帐户名称
$ kubectl create clusterrolebinding <self-defined-cluster-role-binding> --clusterrole=edit --serviceaccount=default:<self-defined-service-account> # 请把<self-defined-service-account>和<self-defined-cluster-role-binding>替换成实际的名称
```

## <span id="cn_jump_application_mode"> Application模式</span>
Application 模式允许用户创建单个镜像，其中包含他们的作业和 Flink 运行时，该镜像将按需自动创建和销毁集群组件。Flink 社区提供了可以构建多用途自定义镜像的基础镜像。
### <span id="cn_jump_build_custom_flink_image">[在第一次跑BitSail或当BitSail Jar改动时] 自定义 Flink Docker 镜像</span>

用`${BITSAIL_HOME}/output/Dockerfile`创建你的 `<CustomImage>` :

把你的`<CustomImage>`发表到Dockerhub，让Kubernetes集群可以下载下来:

[How to create and manage docker repository](https://docs.docker.com/docker-hub/repos/#:~:text=To%20push%20an%20image%20to,docs%2Fbase%3Atesting%20))
```bash
docker build -t <your docker repository>:<tag>
docker push <your docker repository>:<tag>
```

### <span id="cn_jump_start_application">启动Application</span>
```bash
bash ${BITSAIL_HOME}/bin/bitsail run \
   --engine flink \
   --target kubernetes-application \
   --deployment-mode kubernetes-application \
   --execution-mode run-application \
   -p kubernetes.jobmanager.service-account=<self-defined-service-account> \
   -p kubernetes.container.image=<CustomImage> \
   -p kubernetes.jobmanager.cpu=0.25 \
   -p kubernetes.taskmanager.cpu=0.5 \
   --conf-in-base64 <base64 conf>
```
使用者可以在BitSail 指令集用`-p key=value`配置参数

配置参数:

<table>
  <tr>
    <th>Key</th>
    <th>Required or Optional</th>
    <th>Default</th>
    <th>Type</th>
    <th>Description</th>
  </tr>

  <tr>
    <td>kubernetes.cluster-id</td>
    <td>Optional</td>
    <td>bitsail-&#60;instance-id&#62;</td>
    <td>String</td>
    <td>The cluster-id, which should be no more than 45 characters, is used for identifying a unique Flink cluster. If not set, the client will automatically generate it with a random numeric ID with 'bitsail-' prefix.</td>
  </tr>

  <tr>
    <td>kubernetes.cluster.jar.path</td>
    <td>Optional</td>
    <td>"/opt/bitsail/bitsail-core.jar"</td>
    <td>String</td>
    <td>The BitSail jar path in kubernetes cluster.</td>
  </tr>

  <tr>
    <td>kubernetes.container.image</td>
    <td>Required</td>
    <td>The default value depends on the actually running version. In general it looks like "flink:&#60;FLINK_VERSION&#62;-scala_&#60;SCALA_VERSION&#62;"</td>
    <td>String</td>
    <td>Image to use for BitSail containers. The specified image must be based upon the same Apache Flink and Scala versions as used by the application. Visit https://hub.docker.com/_/flink?tab=tags for the images provided by the Flink project.</td>
  </tr>

  <tr>
    <td>kubernetes.container.image.pull-policy</td>
    <td>Optional</td>
    <td>IfNotPresent</td>
    <td>Enum. Possible values: [IfNotPresent, Always, Never]</td>
    <td>The Kubernetes container image pull policy (IfNotPresent or Always or Never). The default policy is IfNotPresent to avoid putting pressure to image repository.</td>
  </tr>

  <tr>
    <td>kubernetes.container.image.pull-secrets</td>
    <td>Optional</td>
    <td>(none)</td>
    <td>List &#60;String&#62;</td>
    <td>A semicolon-separated list of the Kubernetes secrets used to access private image registries.</td>
  </tr>

  <tr>
    <td>kubernetes.hadoop.conf.config-map.name</td>
    <td>Optional</td>
    <td>(none)</td>
    <td>String</td>
    <td>Specify the name of an existing ConfigMap that contains custom Hadoop configuration to be mounted on the JobManager(s) and TaskManagers.</td>
  </tr>

  <tr>
    <td>kubernetes.jobmanager.cpu</td>
    <td>Optional</td>
    <td>1.0</td>
    <td>Double</td>
    <td>The number of cpu used by job manager</td>
  </tr>

  <tr>
    <td>kubernetes.jobmanager.service-account</td>
    <td>Required</td>
    <td>"default"</td>
    <td>String</td>
    <td>Service account that is used by jobmanager within kubernetes cluster. The job manager uses this service account when requesting taskmanager pods from the API server.</td>
  </tr>

  <tr>
    <td>kubernetes.namespace</td>
    <td>Optional</td>
    <td>"default"</td>
    <td>String</td>
    <td>The namespace that will be used for running the jobmanager and taskmanager pods.</td>
  </tr>

  <tr>
    <td>kubernetes.taskmanager.cpu</td>
    <td>Optional</td>
    <td>-1.0</td>
    <td>Double</td>
    <td>The number of cpu used by task manager. By default, the cpu is set to the number of slots per TaskManager</td>
  </tr>
</table>

### <span id="cn_jump_stop_application">停止Application</span>
用户可以去 Flink WebUI 取消正在运行的作业。

或者，用户可以运行以下 bitsail 命令来取消作业。请注意，`<jobId>` 应该从 Flink JobManager 中检索，可以从日志或 WebUI 中检索。
```bash
bash ${BITSAIL_HOME}/bin/bitsail stop \
   --engine flink \
   --target kubernetes-application \
   --deployment-mode kubernetes-application \
   --execution-mode cancel \
   -p kubernetes.cluster-id=<cluster-id> \
   --job-id <jobId>
```
当 Application 停止时，所有 Flink 集群资源都会自动销毁。 与往常一样，作业可能会在手动取消或执行完的情况下停止。
使用者也可以跑 `kubectl` 指令集来删除整个部署的Application
```bash
kubectl delete deployments bitsail-job
```

### <span id="cn_jump_kubernetes_logs">Kubernetes日志文件</span>
以下三种日志文件可以使用：
1. BitSail 客戶端日志： `${FLINK_HOME}/log/flink-xxx.log` on client end
2. BitSail JobManager日志： `/opt/flink/log/jobmanager.log` on Kubernetes JobManager pod
3. BitSail TaskManager日志： `/opt/flink/log/taskmanager.log` on Kubernetes TaskManager pod

如果要使用 `kubectl logs <PodName>` 查看日志，必须执行以下操作：

1. 在 Flink 客户端的 log4j.properties 中增加新的 appender。 
2. 在 log4j.properties 的 rootLogger 中增加如下 ‘appenderRef’，`rootLogger.appenderRef.console.ref = ConsoleAppender`。 
3. 停止并重启你的 session。现在你可以使用 kubectl logs 查看日志了。
4. 已准备好编译 BitSail（使用 `${BITSAIL_HOME}/build.sh` 构建后，工件将位于 `${BITSAIL_HOME}/output/` 中）

```bash
# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```
使用者可以使用以下`kubectl` 指令集来查看日志
```bash
# During job running
kubectl get pods # Will return jobmanager pod and taskmanager pod

kubectl logs -f <jobmanagerPod> # Will dump jobManager log

kubectl logs -f <taskmanagerPod>  # Will dump taskManager log
```


### <span id="cn_jump_history_server">History Server</span>
Flink 提供了 history server，可以在相应的 Flink 集群关闭之后查询已完成作业的统计信息。 此外，它暴露了一套 REST API，该 API 接受 HTTP 请求并返回 JSON 格式的数据。
更多相关讯息在 https://nightlies.apache.org/flink/flink-docs-release-1.11/monitoring/historyserver.html

启动及停止HistoryServer
```bash
${FLINK_HOME}/bin/historyserver.sh (start|start-foreground|stop)
```

使用BitSail指令集去配置history server。
```bash
bash ${BITSAIL_HOME}/bin/bitsail run \
   --engine flink \
   --target kubernetes-application \
   --deployment-mode kubernetes-application \
   --execution-mode run-application \
   -p kubernetes.cluster-id=<cluster-id> \
   -p kubernetes.jobmanager.service-account=<self-defined-service-account> \
   -p kubernetes.container.image=<CustomImage> \
   -p kubernetes.jobmanager.cpu=0.25 \
   -p kubernetes.taskmanager.cpu=0.5 \
   -p jobmanager.archive.fs.dir=hdfs:///completed-jobs/ \
   -p historyserver.web.address=0.0.0.0 \
   -p historyserver.web.port 8082 \
   -p historyserver.archive.fs.dir hdfs:///completed-jobs/ \
   -p historyserver.archive.fs.refresh-interval 10000 \
   --conf-in-base64 <base64 conf>
```

