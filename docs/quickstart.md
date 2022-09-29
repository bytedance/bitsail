# Quickstart

***BitSail*** is a data integration framework that is based on the Flink engine and supports streaming and batch data synchronization。

At present, ***BitSail*** is mainly designed with the ELT model, and bears the data synchronization requirements of Bytedance's internal EB level.

## Download BitSail

***BitSail*** runs in a Unix-like environment, i.e. Linux, Mac OS X. Java 8 needs to be installed locally. Download the latest ***BitSail*** binary distribution and extract the tarball:

``` shell
tar -xzf bitsail-*.tar.gz
```

## Project product catalog

After decompressing the folder, the project product file structure is as follows:

``` simple
-- bin  
    \-- bitsail // Startup script
-- conf
    \-- bitsail.conf // bitsail system config
-- embedded
    \-- flink // embedded flink
-- examples // examples config
    \-- example-datas // examples data
    \-- Fake_xx_Example.json // Fake source to xx examples config files
    \-- xx_Print_Example.json // xx to print sink examples config files
-- libs // jar libs
    \-- bitsail-core.jar // entering jar package
    \-- connectors // connector plugin jars
        \-- mapping // connector plugin config files
    \-- components // components jars，such as metric、dirty-collector
    \-- clients // bitsail client jar
```

## Startup script instructions

At present, ***BitSail*** only support yarn mode submission, so the following describes how to submit in yarn mode.

### Configure related component information

Configure the system configuration in the path `conf/bitsail.conf`, including the flink path and the default parameters used by the system.
Before we run the command, please make sure the `HADOOP_HOME` exists in your environment parameters.

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

### Task configuration

Construct task configuration, see details [config](config.md)。

### Submit tasks
> ***BitSail*** only support resource provider `yarn's yarn-per-job` mode until now, others like `native kubernetes` will be release recently.

You can use the startup script `bin/bitsail` to submit flink jobs to yarn. The specific commands are as follows:

``` bash
bash ./bin/bitsail run --engine flink --conf [job_conf_path] --execution-mode run --queue [queue_name] --deployment-mode yarn-per-job [--priority [yarn_priority] -p/--props [name=value]] 
```

Parameter description (currently only supports reading parameters in order)

* Required parameters
  * queue_name: target yarn queue
  * job_conf_path: Task configuration file address
* Optional parameters
  * yarn_priority: yarn priority
  * name=value: for example classloader.resolve-order=child-first
    * name: property key. Configurable flink parameters will be transparently transmitted to the flink task
    * value: property value.
