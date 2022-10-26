# BitSail

## Introduce
BitSail is ByteDance's open source data integration engine which is based on distributed architecture and provides high performance. It supports data synchronization between multiple heterogeneous data sources, and provides global data integration solutions in offline, real-time, and incremental scenarios. At present, it serves almost all business lines in ByteDance, such as Douyin, Toutiao, etc., and synchronizes hundreds of trillions data every day.

## Why do we use BitSail
BitSail has been widely used and supports hundreds of trillions of large traffic. At the same time, it has been verified in various scenarios such as the cloud native environment of the volcano engine and the customer's private cloud environment.

We have accumulated a lot of experience and made a number of optimizations to improve the function of data integration

- Global Data Integration, covering offline、streaming and incremental scenarios

- Distributed and cloud-native architecture, supporting horizontal scaling

- High maturity in terms of accuracy、stability and performance

- Rich basic functions, such as type conversion、dirty data processing、flow control、streaming archiving、data lake integration, automatic concurrency inference, etc.

- Task running status monitoring, such as traffic、QPS、dirty data、delay, etc.

## BitSail use scenarios
- Mass data synchronization in heterogeneous data sources

- Stream-batch integrated data processing capability

- Lake and warehouse integration data processing capability

- High-performance, highly reliable data synchronization

- Distributed, cloud-native architecture data integration engine

## Features of BitSail

- Low start-up cost and high flexibility

- Stream-batch integration and lake-warehouse integration architecture, one framework covers almost all data synchronization scenarios

- High-performance, massive data processing capabilities

- DDL automatic synchronization

- Type system, conversion between different data source types

- Engine-independent read-write interface, low development cost

- Real-time display of task progress percentage, in development

- Real-time monitoring of task status

## Architecture of BitSail
 ![](docs/images/bitsail_arch.png)
 
 ```
 Source[Input Sources] -> Framework[Data Transmission] -> Sink[Output Sinks]
 ```
The data processing pipeline is as follows. First, pull the source data through Input Sources, then process it through the intermediate framework layer, and finally write the data to the target through Output Sinks

At the framework layer, we provide rich functions and take effect for all synchronization scenarios, such as dirty data collection, auto parallelism calculation, task monitoring, etc.

In data synchronization scenarios, it covers batch, streaming, and incremental data synchronization

In the Runtime layer, it supports multiple execution modes, such as yarn, local, and k8s is under development

## Requirements

The latest version of bitsail has the following minimal requirements:

- Java 8 and higher for the build is required. For usage Java 8 is a minimum requirement;
- Maven 3.6 and higher;
- Operating system: no specific requirements (tested on Windows and Linux).

## Support Connectors

<table>
  <tr>
    <th>DataSource</th>
    <th>Sub Modules</th>
    <th>Reader</th>
    <th>Writer</th>
  </tr>
  <tr>
    <td>Hive</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Hadoop</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td>Hbase</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Hudi</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Kafka</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>RocketMQ</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>StreamingFile (Hadoop Streaming mode.)</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Redis</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Doris</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
  <tr>
    <td>MongoDB</td>
    <td>-</td>
    <td>✅</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>Doris</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td rowspan="4">JDBC</td>
    <td>MySQL</td>
    <td rowspan="4">✅</td>
    <td rowspan="4">✅</td>
  </tr>
  <tr>
    <td>Oracle</td>
  </tr>
  <tr>
    <td>PostgreSQL</td>
  </tr>
  <tr>
    <td>SqlServer</td>
  </tr>
  <tr>
    <td>Fake</td>
    <td>-</td>
    <td>✅</td>
    <td> </td>
  </tr>
  <tr>
    <td>Print</td>
    <td>-</td>
    <td> </td>
    <td>✅</td>
  </tr>
</table>

Connector introduce [Connector](./docs/connectors/introduction.md)

## How to build from source code.

First, use `git clone` to download the code.
Then, use follow command to package

```
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true
```

Final, you will find output under the folder `bitsail-dist/target/`

We also prepare a profile for `flink-embedded`, you can use follow command:

```
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true -Pflink-embedded
```

After building the project, the project production file structure is as follows:

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

## Environment Setup

Link to [Environment Setup](docs/env_setup.md).

## Deployment Guide

Link to [Deployment Guide](docs/deployment.md).

## Developer Guide

Link to [Developer Guide](docs/developer_guide.md).

## Contact

## License
[Apache 2.0 License](LICENSE).
