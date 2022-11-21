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
