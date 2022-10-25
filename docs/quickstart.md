# Quickstart

***BitSail*** is a data integration framework that is based on the Flink engine and supports streaming and batch data synchronization。

At present, ***BitSail*** is mainly designed with the ELT model, and bears the data synchronization requirements of Bytedance's internal EB level.

## Build from source code
- Run the build script to package
```
bash build.sh
```
- After the build completed, you can find the jar file under the path `bitsail-dist/target/bitsail-dist-${version}-SNAPSHOT-bin/bitsail-archive-${version}-SNAPSHOT`

## <span id="jump_product_structure">Produced file</span>

After build the project, the project production file structure is as follows:

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

## Run Local Integration Tests
### Prerequisite
- JDK8
- maven 3.6+
- [Docker desktop](https://www.docker.com/products/docker-desktop/)

Details can be seen [how to run local integration tests?](./local_integration_test.md)

## Submit BitSail job

At present, ***BitSail*** only supports flink deployment on Yarn.
Other platforms like native kubernetes will be release recently.

Details of deployment can be seen [here](./yarn_deployment.md).

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