---
order: 1
---

# Deployment Guide

English | [简体中文](../../../zh/documents/start/config.md)

-----

> At present, ***BitSail*** only supports flink deployment on Yarn.<br>
Other platforms like `native kubernetes` will be release recently.

Here are the contents of this part:

- [Pre Configuration](#jump_pre_configure)
    - [Configure Hadoop Environment](#jump_configure_hadoop)
    - [Configure Flink Cluster](#jump_configure_flink)
- [Submit to Yarn](#jump_submit_to_yarn)
    - [Submit an example job](#jump_submit_example)
    - [Log for Debugging](#jump_log)
- [Submit to Local Flink Session](#jump_submit_local)

Below is a step-by-step guide to help you effectively deploy it on Yarn.

-----

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

> ***BitSail*** only support resource provider `yarn's yarn-per-job` mode until now, others like `native kubernetes` will be release recently.

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
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-0.1.0-SNAPSHOT/examples/Fake_Print_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```

### <span id="jump_log">Log for Debugging</span>

#### Client side log file
Please check `${FLINK_HOME}/log/` folder to read the log file of BitSail client.

#### Yarn task log file
Please go to Yarn WebUI to check the logs of Flink JobManager and TaskManager.

-----

## Submit to Local Flink Session

Suppose that BitSail install path is: `${BITSAIL_HOME}`.

After building BitSail, we can enter the following path and find runnable jars and example job configuration files:
```shell
cd ${BITSAIL_HOME}/bitsail-dist/target/bitsail-dist-0.1.0-SNAPSHOT-bin/bitsail-archive-0.1.0-SNAPSHOT/
```

### Run Fake_to_Print example

Use [examples/Fake_Print_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Print_Example.json) as example to start a BitSail job:

- `<job-manager-address>`: the address of job manager, should be host:port, _e.g._ `localhost:8081`.

```shell
bash bin/bitsail run \
  --engine flink \
  --execution-mode run \
  --deployment-mode local \
  --conf examples/Fake_Print_Example.json \
  --jm-address <job-manager-address>
```

Then you can visit Flink WebUI to see the running job.
In task manager, we can see the output of the Fake_to_Print job in its stdout.


### Run Fake_to_Hive example

Use [examples/Fake_hive_Example.json](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/examples/Fake_Hive_Example.json) as an example:
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
  --conf examples/Fake_Hive_Example.json \
  --jm-address <job-manager-address>
  ```