# BitSail

![](docs/images/bitsail.png)

## 介绍

*BitSail*是一款基于Flink引擎，同时支持流批数据同步的数据集成框架。
目前*BitSail*主要采用ELT的模型进行设计，承担了Bytedance内部EB量级的数据同步需求。<br/>

## 主要特点

- 内置类型系统，支持不同数据类型之间的转换。
- 基础功能插件化，可以按照需求定义自己场景的各类插件，如监控、脏数据收集等功能。
- 批式场景下支持DDL的同步操作，能够自动感知上游数据变化并反应到下游数据中。
- 流式场景下支持自动响应hive表DDL变化，无需作业重启。
- 流式场景下托管checkpoint，启动时自动选择符合要求的checkpoint路径。
- ...

## 支持连接器列表
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

## 社区支持
### Slack
通过此链接可以直接下载并加入BitSail的Slack频道 [link](https://join.slack.com/t/slack-ted3816/shared_invite/zt-1inff2sip-u7Ej_o73sUgdpJAvqwlEwQ)

### 邮件列表
当前，BitSail社区通过谷歌群组作为邮件列表的提供者，邮件列表可以在绝大部分地区正常收发邮件。
在订阅BitSail小组的邮件列表后可以通过发送邮件发言

开启一个话题: 发送Email到此地址 `bitsail@googlegroups.com`

订阅: 发送Email到此地址 `bitsail+subscribe@googlegroups.com`

取消订阅: 发送Email到此地址 `bitsail+unsubscribe@googlegroups.com`

## 环境配置
跳转[环境配置](docs/env_setup_zh.md).

## 如何部署
跳转[部署指南](docs/deployment_zh.md).

## BitSail参数指引
跳转[参数指引](docs/config_zh.md)

## 如何贡献
跳转[贡献者指引](docs/contributing_zh.md)

## 开源协议
Apache 2.0 License