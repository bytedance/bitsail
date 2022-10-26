# BitSail
[Chinese Version](README_zh.md)

![](docs/images/bitsail.png)

## Introduction

***BitSail*** is a data integration framework that is based on the Flink engine and both support streaming and batch mode.
At present, ***BitSail*** is mainly designed with the ELT model, which have EB data size and use for Bytedance。<br/>

## Features

- Middle Data types, Support convert between the difference data types.
- plug-in, can implement difference plugins by user according the difference situation.
- In Batch mode, support the auto alignment the schema between upstream and downstream.
- In Streaming mode, support alignment the hive schemas automatically.
- In Streaming mode, auto-detect the exists checkpoint and apply when job restart.
- ...

## Supported Connectors
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

Documentation for [Connectors](./docs/connectors/introduction.md).

## Community Support
### Slack
Join BitSail Slack channel via this [link](https://join.slack.com/t/slack-ted3816/shared_invite/zt-1inff2sip-u7Ej_o73sUgdpJAvqwlEwQ)

### Mailing List
Currently, BitSail community use Google Group as the mailing list provider.
You need to subscribe to the mailing list before starting a conversation

Subscribe: Email to this address `bitsail+subscribe@googlegroups.com`

Start a conversation: Email to this address `bitsail@googlegroups.com`

Unsubscribe: Email to this address `bitsail+unsubscribe@googlegroups.com`

### WeChat Group
If you have WeChat account, please scan this QR code to connect with the BitSail community assistant
and join the WeChat group chat.

<img src="docs/images/wechat_QR.png" alt="qr" width="100"/>

## Environment Setup
Link to [Environment Setup](docs/env_setup.md).

## Deployment Guide
Link to [Deployment Guide](docs/deployment.md).

## BitSail Configuration
Link to [configuration guide](docs/config.md)

## Contributing Guide
Link to [Contributing Guide](docs/contributing.md)

## License
Apache 2.0 License
