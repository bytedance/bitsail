---
order: 1
---

# Deployment Guide

English | [简体中文](../../../zh/documents/start/deployment.md)

-----

> At present, ***BitSail*** supports flink deployment on Yarn and native Kubernetes.<br>

Here are the contents of this part:

- [Yarn Deployment](#yarn_deployment)
  - [Pre Configuration](#jump_pre_configure)
      - [Configure Hadoop Environment](#jump_configure_hadoop)
      - [Configure Flink Cluster](#jump_configure_flink)
  - [Submit to Yarn](#jump_submit_to_yarn)
      - [Submit an example job](#jump_submit_example)
      - [Log for Debugging](#jump_log)
  - [Submit to Local Flink Session](#jump_submit_local)
      - [Run in Remote Flink Session](#jump_flink_remote)
      - [Run Locally](#jump_flink_local)
- [Native Kubernetes Deployment](#native_kubernetes_deployment)
  - [Prerequisites](#jump_prerequisites_k8s)
  - [Pre Configuration](#jump_pre_configuration_k8s)
    - [Setup RBAC](#jump_configure_RBAC)
  - [Application Mode](#jump_application_mode)
    - [Build Custom Flink Image](#jump_build_custom_flink_image)
    - [Start Application](#jump_start_application)
    - [Stop Application](#jump_stop_application)
    - [Kubernetes Logs](#jump_kubernetes_logs)
    - [History Server](#jump_history_server)

-----
# <span id="yarn_deployment">Yarn Deployment</span>

Below is a step-by-step guide to help you effectively deploy it on Yarn.
## <span id="jump_pre_configure">Pre configuration</span>

### <span id="jump_configure_hadoop">Configure Hadoop Environment</span>


To support Yarn deployment, `HADOOP_CLASSPATH` has to be set in system environment properties. There are two ways to set this environment property:

1. Set `HADOOP_CLASSPATH` directly.

2. Set `HADOOP_HOME` targeting to the hadoop dir in deploy environment. The [bitsail](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/bin/bitsail) scripts will use the following command to generate `HADOOP_CLASSPATH`.

  ```shell
  if [ -n "$HADOOP_HOME" ]; then
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
  fi
  ```

### <span id="jump_configure_flink">Configure Flink Cluster</span>

After packaging, the project production contains a file [conf/bitsail.conf](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/resources/bitsail.conf).
This file describes the system configuration of deployment environment, including the flink path and some other default parameters.

Here are some frequently-used options in the configuration file:


<table>
  <tr>
    <th>Prefix</th>
    <th>Parameter name</th>
    <th>Description</th>
    <th>Example</th>
  </tr>

  <tr>
    <td rowspan="3">sys.flink.</td>
    <td>flink_home</td>
    <td>The root dir of flink.</td>
    <td>${BITSAIL_HOME}/embedded/flink</td>
  </tr>

  <tr>
    <td>checkpoint_dir</td>
    <td>The path storing the meta data file and data files of checkpoints.<br/>Reference: <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/">Flink Checkpoints</a></td>
    <td>"hdfs://opensource/bitsail/flink-1.11/checkpoints/"</td>
  </tr>

  <tr>
    <td>flink_default_properties</td>
    <td>General flink runtime options configued by "-D".</td>
    <td>{<br/>
        classloader.resolve-order: "child-first"<br/>
        akka.framesize: "838860800b"<br/>
        rest.client.max-content-length: 838860800<br/>
        rest.server.max-content-len<br/>}
    </td>
  </tr>
</table>

-----

## <span id="jump_submit_to_yarn">Submit to Yarn</span>

You can use the startup script `bin/bitsail` to submit flink jobs to yarn.

The specific commands are as follows:


``` bash
bash ./bin/bitsail run --engine flink --conf [job_conf_path] --execution-mode run --queue [queue_name] --deployment-mode yarn-per-job [--priority [yarn_priority] -p/--props [name=value]] 
```

Parameter description

* Required parameters
    * **queue_name**: Target yarn queue
    * **job_conf_path**: Path of job configuration file
* Optional parameters
    * **yarn_priority**: Job priority on yarn
    * **name=value**: Flink properties, for example `classloader.resolve-order=child-first`
        * **name**: Property key. Configurable flink parameters that will be transparently transmitted to the flink task.
        * **value**: Property value.

### <span id="jump_submit_example">Submit an example job</span>
Submit a fake source to print sink test to yarn.
``` bash
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-0.2.0-SNAPSHOT/examples/Fake_Print_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```

### <span id="jump_log">Log for Debugging</span>

#### Client side log file
Please check `${FLINK_HOME}/log/` folder to read the log file of BitSail client.

#### Yarn task log file
Please go to Yarn WebUI to check the logs of Flink JobManager and TaskManager.

-----

## Submit to Flink

Suppose that BitSail install path is: `${BITSAIL_HOME}`.

After building BitSail, we can enter the following path and find runnable jars and example job configuration files:
```shell
cd ${BITSAIL_HOME}/bitsail-dist/target/bitsail-dist-0.2.0-SNAPSHOT-bin/bitsail-archive-0.2.0-SNAPSHOT/
```

### <span id="jump_flink_remote">Run in Remote Flink Session</span>

Users can use commands `--deployment-mode remote` to submit a BitSail job to remote flink session.
Use [examples/Fake_Print_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Print_Example.json) as example to start a BitSail job:

- `<job-manager-address>`: the address of job manager, should be host:port, _e.g._ `localhost:8081`.

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode remote \
  --conf examples/Fake_Print_Example.json \
  --jm-address <job-manager-address>
```

For example, we can use the script `bitsail-archive-0.1.0-SNAPSHOT/embedded/flink/bin/start-cluster.sh` to start a standalone session. Then we can run the example with following commands:

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode remote \
  --conf examples/Fake_Print_Example.json \
  --jm-address localhost:8081
```
Then you can visit Flink WebUI to see the running job.
In task manager, we can see the output of the Fake_to_Print job in its stdout.

### <span id="jump_flink_local">Run in Local Mini-Cluster</span>


Users can use commands `--deployment-mode local` to run a BitSail job locally.
Use [examples/Fake_Print_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Print_Example.json) as example to start a BitSail job:

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode local \
  --conf examples/Fake_Print_Example.json
```

#### Run Fake_to_Print example

Take [examples/Fake_hive_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Hive_Example.json) as another example:
- Remember fulfilling the job configuration with an available hive source before run the command:
    - `job.writer.db_name`: the hive database to write.
    - `job.writer.table_name`: the hive table to write.
    - `job.writer.metastore_properties`: add hive metastore address to it, like:
    ```shell
       {
          "job": {
            "writer": {
              "metastore_properties": "{\"hive.metastore.uris\":\"thrift://localhost:9083\"}"
            }
          }
       }
    ```

Then you can use the similar command to submit a BitSail job to specified Flink session:

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode local \
  --conf examples/Fake_Hive_Example.json
  ```

#### Run hadoop related job

When any of the reader or writer data source is relate to hadoop, _e.g._, `hive_to_print` job, the hadoop libs are needed.
There are two ways to offer hadoop libs for local minicluster:

 1. If you already have local hadoop environment, then you can directly set `$HADOOP_HOME` to the folder of your hadoop libs. For example:

```bash
export HADOOP_HOME=/usr/local/hadoop-3.1.1
```

 2. If there is no hadoop environment, you can use `flink-shaded-hadoop`. Remember moving the uber jar to your flink lib dir.
  For example, suppose the flink root dir is `/opt/flink`:

```bash
# download flink-shaded-hadoop-uber jar
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

# move to flink libs
mv flink-shaded-hadoop-2-uber-2.7.5-10.0.jar /opt/flink/lib/flink-shaded-hadoop-uber.jar
```


-----
# <span id="native_kubernetes_deployment">Native Kubernetes Deployment</span>

> At present, ***BitSail*** supports native Kubernetes via Flink 1.11 engine.<br>
> 
Below is a step-by-step guide to help you effectively deploy it on native Kubernetes. Currently, BitSail support Application deployment mode: Allows users to create a single image containing their Job and the Flink runtime, which will automatically create and destroy cluster components as needed.

## <span id="jump_prerequisites_k8s">Prerequisites</span>
1. Kubernetes >= 1.9
2. KubeConfig, which has access to list, create, delete pods and services, configurable via `~/.kube/config`. You can verify permissions by running `kubectl auth can-i <list|create|edit|delete> pods` 
3. Kubernetes DNS enabled
4. Have compiled BitSail ready (After building with `${BITSAIL_HOME}/build.sh`, the artifacts will be located in `${BITSAIL_HOME}/output/`)

If you have problems setting up a Kubernetes cluster, then take a look at [how to setup a Kubernetes cluster](https://kubernetes.io/docs/setup/).

## <span id="jump_pre_configuration_k8s">Pre Configuration</span>

### <span id="jump_configure_RBAC">Setup RBAC</span>
Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise. Users can configure RBAC roles and service accounts used by JobManager to access the Kubernetes API server within the Kubernetes cluster.

Every namespace has a default service account. However, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster. Users can instead use the following command to create a new service account `<self-defined-service-account>` and set the role binding. Then use the config option `kubernetes.service-account=<self-defined-service-account>` to make the JobManager pod use the `<self-defined-service-account>` service account to create/delete TaskManager pods and leader ConfigMaps. Also this will allow the TaskManager to watch leader ConfigMaps to retrieve the address of JobManager and ResourceManager.

```bash
$ kubectl create serviceaccount <self-defined-service-account> # Please replace <self-defined-service-account> with a custom name
$ kubectl create clusterrolebinding <self-defined-cluster-role-binding> --clusterrole=edit --serviceaccount=default:<self-defined-service-account> # Please replace <self-defined-service-account> and <self-defined-cluster-role-binding> with custom names
```

## <span id="jump_application_mode"> Application Mode</span>
Application mode allows users to create a single image containing their Job and the Flink runtime, which will automatically create and destroy cluster components as needed. The Flink community provides base docker images [customized](https://nightlies.apache.org/flink/flink-docs-release-1.11/ops/deployment/docker.html#customize-flink-image) for any use case.
### <span id="jump_build_custom_flink_image">Build Custom Flink Image [First Time or Per BitSail JAR Executable Update]</span>

Build your `<CustomImage>` using the [`Dockerfile`](https://docs.docker.com/engine/reference/builder/) from `${BITSAIL_HOME}/output/Dockerfile`:

Publish your `<CustomImage>` onto Dockerhub so that Kubernetes cluster can download:

[How to create and manage docker repositories.](https://docs.docker.com/docker-hub/repos/#:~:text=To%20push%20an%20image%20to,docs%2Fbase%3Atesting%20)
```bash
docker build -t <your docker repository>:<tag>
docker push <your docker repository>:<tag>
```

### <span id="jump_start_application">Start Application</span>
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

User can specify more configurations by adding more `-p key=value` in bitsail command lines. 

Configurations:


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

### <span id="jump_stop_application">Stop Application</span>
Users can go to Flink WebUI to cancel running jobs. 

Alternatively, users can run the following bitSail command to cancel a job. 

Noted that 
- `<jobId>` can be retrieved from Flink JobManager, either from logs or WebUI.
- `<cluster-id>` can be retrieved from `kubectl get deployment`
```bash
kubectl get deployment
# expected output
NAME           READY   UP-TO-DATE   AVAILABLE   AGE
<cluster-id>   1/1     1            1           22s
```
```bash
bash ${BITSAIL_HOME}/bin/bitsail stop \
   --engine flink \
   --target kubernetes-application \
   --deployment-mode kubernetes-application \
   --execution-mode cancel \
   -p kubernetes.cluster-id=<cluster-id> \
   --job-id <jobId>
```
If users want to delete the whole application, users can run `kubectl` commands to delete the whole deployment in order to stop the application
```bash
kubectl delete deployments bitsail-job
```

### <span id="jump_kubernetes_logs">Kubernetes Logs</span>
There are three types of logs:
1. BitSail client log: `${FLINK_HOME}/log/flink-xxx.log` on client end
2. BitSail JobManager log: `/opt/flink/log/jobmanager.log` on Kubernetes JobManager pod
3. BitSail TaskManager log: `/opt/flink/log/taskmanager.log` on Kubernetes TaskManager pod


If you want to use `kubectl logs <PodName>` to view the logs, you must perform the following:

1. Add a new appender to the log4j.properties in the Flink client. 
2. Add the following ‘appenderRef’ the rootLogger in log4j.properties `rootLogger.appenderRef.console.ref = ConsoleAppender`. 
3. Stop and start your Application again. Now you could use kubectl logs to view your logs.

```bash
# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```
User can dump JobManager/TaskManager logs on client end by running `kubectl` commands
```bash
# During job running
kubectl get pods # Will return jobmanager pod and taskmanager pod

kubectl logs -f <jobmanagerPod> # Will dump jobManager log

kubectl logs -f <taskmanagerPod>  # Will dump taskManager log
```


### <span id="jump_history_server">History Server</span>
Flink has a history server that can be used to query the statistics of completed jobs after the corresponding Flink cluster has been shut down.
Furthermore, it exposes a REST API that accepts HTTP requests and responds with JSON data. More information in https://nightlies.apache.org/flink/flink-docs-release-1.11/monitoring/historyserver.html

Start or stop the HistoryServer
```bash
${FLINK_HOME}/bin/historyserver.sh (start|start-foreground|stop)
```

Run BitSail command line to configure history server.
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
