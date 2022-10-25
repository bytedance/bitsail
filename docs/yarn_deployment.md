# Deployment

> At present, ***BitSail*** only supports flink deployment on Yarn.<br>
Other platforms like `native kubernetes` will be release recently.

-----

## Deployment on Yarn

### Configure Hadoop Environment

Here is a step-by-step guide to help you effectively deploy it on Yarn.

To support Yarn deployment, `HADOOP_CLASSPATH` has to be set in system environment properties. There are two ways to set this environment property:

1. Set `HADOOP_CLASSPATH` directly.

2. Set `HADOOP_HOME` targeting to the hadoop dir in deploy environment. The [bitsail](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/archive/bin/bitsail) scripts will use the following command to generate `HADOOP_CLASSPATH`.

  ```shell
  if [ -n "$HADOOP_HOME" ]; then
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
  fi
  ```

### Configure Flink Cluster

As mentioned in [quickstart](./quickstart.md), the project production contains a file [conf/bitsail.conf](https://github.com/bytedance/bitsail/blob/master/bitsail-dist/src/main/resources/bitsail.conf).
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


### Submit to Yarn

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









