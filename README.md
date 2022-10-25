# BitSail

![](docs/images/bitsail.png)

## Introduce

***BitSail*** is a data integration framework that is based on the Flink engine and both support streaming and batch mode.
At present, ***BitSail*** is mainly designed with the ELT model, which have EB data size and use for Bytedance。<br/>

## Feature

- Middle Data types, Support convert between the difference data types.
- plug-in, can implement difference plugins by user according the difference situation.
- In Batch mode, support the auto alignment the schema between upstream and downstream.
- In Streaming mode, support alignment the hive schemas automatically.
- In Streaming mode, auto-detect the exists checkpoint and apply when job restart.
- ...

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

Apache 2.0 License

## Thanks

This project refers to some excellent codes of open source data integration tools in the industry, and I would like to
express my thanks to them
<br/>
[DataX](https://github.com/alibaba/DataX)<br/>
[chunjun](https://github.com/DTStack/chunjun)<br/>




