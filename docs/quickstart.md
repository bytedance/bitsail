# Quickstart

***BitSail*** is a data integration framework that is based on the Flink engine and supports streaming and batch data synchronization。

At present, ***BitSail*** is mainly designed with the ELT model, and bears the data synchronization requirements of Bytedance's internal EB level.

## Build from source code
- Run the build script to package
```
bash build.sh
```
- After the build completed, you can find the jar file under the path `bitsail-dist/target/bitsail-dist-1.0.0-SNAPSHOT-bin/bitsail-archive-1.0.0-SNAPSHOT`

## Run Local Integration Tests
### Prerequisite
- JDK8
- maven 3.6+
- Docker desktop: https://www.docker.com/products/docker-desktop/

After the prerequisite package be installed correctly. We will be able to run the integration tests on your Local IDE.
For example, `com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceITCase`. This test simulates a Kafka test container and consume the data into a print sink connector.

## Produced file

After build the project, the project production file structure is as follows:

``` simple
bitsail-archive-1.0.0-SNAPSHOT    
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

## Submit BitSail job

At present, ***BitSail*** only support yarn mode submission, so the following describes how to submit in yarn mode.

### Configure Environment Variable
Before we run the command, please make sure the `HADOOP_HOME` exists in your environment parameters.
`export HADOOP_HOME=xxx/xxx/xxx`

### Configure BitSail Task configuration

Construct task configuration, see details [config](config.md)。

You can also check the example file under `/examples/XXX_XXX_Example.json`

### Configure flink cluster information

Configure the system configuration in the path `conf/bitsail.conf`, including the flink path and the default parameters used by the system.

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

## Debugging
### Client side log file
Please check `embedded/flink/log/` folder to read the log file of BitSail client.
### Yarn task log file
Please go to Yarn WebUI to check the logs of Flink JobManager and TaskManager

## Submit an example task
Submit a fake source to print sink test to yarn.
``` bash
bash ./bin/bitsail run --engine flink --conf ~/bitsail-archive-1.0.0-SNAPSHOT/examples/Fake_Hudi_Example.json --execution-mode run -p 1=1  --deployment-mode yarn-per-job  --queue default
```