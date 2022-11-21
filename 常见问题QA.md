1、Q：有支持flink1.13的版本吗？

   #### A：目前BitSail是基于Flink 1.11开源的，有计划做Flink底层引擎升级，也欢迎提PR共建

2、Q：流式支不支持ddl同步？

   #### A：MQ2Hive 场景，支持DDL同步，Hive表Schema变更时，会实时抽取

3、Q：BitSail能当同步数据用吗？mysql->ck mysql->tidb 
   
   #### A：可以的

4、Q：Hive的cdc接入是通过什么方式？

   #### A：目前是周期去Hive metastore获取的

5、Q：BitSail 在cdc这块有做什么优化么？

   #### A：后续会接入flinkcdc作为source，可以通过flink cdc写入hudi，BitSail提供了集成引擎的能力
  
6、Q：BitSail有前端页面操作吗？

   #### A：目前是专注在数据集成引擎，提供通用的数据同步能力，之后会计划和其它平台打通，包括开发、调度平台，提供一站式的数据开发体验
   
7、Q：支持上游表结构变更同步到下游吗？

   #### A：目前还不支持，可以提个issue共建
  
8、Q：有没有想过用搞一个页面，SQL来定义数据ETL的整个流程，我看目前是用JSON来定义数据处理的整个流程的，对吗？

#### A：
#### * 目前数据集成的处理模式，已经从传统的 ETL 模式开始转变为 EL（T）模式，目前中心也是先解决数据同步的问题，还不支持T操作
#### * 用JSON配置更多是考虑使用成本的问题，SQL本质上也一种代码，对用户的接入成本是比较高的

9、Q：请问 hive 读取这块会像jdbc 那样，支持自定义sql吗？

#### A：这个暂时还不支持，可以提个issue记录下

10、Q：目前代码是以master分支为主，还是以bitsail-type-system分支为主？

#### A：master

11、是否提供鉴权认证或者加密插件？
#### A：目前是支持kerberos认证，加密插件目前还不支持，可以提几个issue
