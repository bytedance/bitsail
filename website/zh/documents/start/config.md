---
order: 3
---

# 任务配置说明

[English](../../../en/documents/start/deployment.md) | 简体中文

-----

***BitSail*** 完整配置脚本是由一个 JSON 组成的，完整示意结构如下所示：

``` json
{
    "job":{
        "common":{
        ...
        },
        "reader":{
        ...
        },
        "writer":{
        ...
        }
    }
}
```

| 模块名称           | 参数含义                                                                                |
|----------------|-------------------------------------------------------------------------------------|
| common         | 主要负责设置一些通用的参数配置，如标注作业的元数据信息，标注作业所需要插件的实现方案等。                                        |
| reader/readers | 主要负责设置源数据侧相关参数信息等。以MySQL数据源进行举例，需要在reader的子域下设置JDBC的连接信息，操作的库表信息等。                  |
| writer/writers | 主要负责设置目标数据源的相关参数等。以Hive目标数据源为例，需要在writer的子域下设置Hive的metastore连接信息，设置Hive库表、分区的相关信息等。 |

## Common 模块

示例：

``` json
{
    "job":{
        "common":{
            "user_name":"bytedance_dts",
            "instance_id":-1L,
            "job_id":-1L,
            "job_name":"",
            "min_parallelism":1,
            "max_parallelism":5,
            "parallelism_chain":false,
            "max_dirty_records_stored_num":50,
            "dirty_records_count_threshold":-1,
            "dirty_records_percentage_threshold":-1
        }
    }
}
```

说明：

元数据配置：任务的基本信息配置

| 参数名称        | 是否必须 | 默认值 | 参数含义                          | 示例           |
|-------------|------|-----|-------------------------------|--------------|
| user_name   | TRUE | -   | 提交作业的用户                       | bitsail      |
| job_id      | TRUE | -   | 提交作业的id                       | 12345        |
| instance_id | TRUE | -   | 作业的实例id                       | 12345        |
| job_name    | TRUE | -   | 作业的名称，用于指定作业在外部资源provider的名称； | bitsail_conf |

并行度配置：配置任务的读写并发信息

| 参数名称              | 是否必须  | 默认值   | 参数含义                                                 | 示例  |
|-------------------|-------|-------|------------------------------------------------------|-----|
| min_parallelism   | FALSE | 1     | 作业最小的并行度，自动计算的并行度会大于等于最小并行度                          | 2   |
| max_parallelism   | FALSE | 512   | 作业最大的并行度，自动计算的并行度会小于等于最大并行度                          | 2   |
| parallelism_chain | FALSE | FALSE | 算子之间是否需要统一并行度。如果开启该设计，会选择reader和writer之间最小的并行度来进行设置。 | 2   |

脏数据配置：任务的脏数据配置

| 参数名称                              | 是否必须  | 默认值 | 参数含义                                       | 示例  |
|-----------------------------------|-------|-----|--------------------------------------------|-----|
| max_dirty_records_stored_num      | FALSE | 50  | 每个task级别脏数据的收集数量，默认为50条                    | 50  |
| dirty_records_count_threshold     | FALSE | -1  | 整体脏数据的阈值设置，如果在传输结束后发现脏数据多于该设置，作业失败。        | -1  |
| dirty_record_percentage_threshold | FALSE | -1  | 整体脏数据占整体传输数据的比例，如果传输结束后发现脏数据的比例大于该阈值，作业失败。 | -1  |

流控配置： 任务的流控配置

| 参数名称                               | 是否必须 | 默认值 | 参数含义                  | 示例 |
|---------------------------------------|---------|------|--------------------------|-----|
| reader_transport_channel_speed_byte   | FALSE   | -1   | 控制单并发读的流量, X字节/S  | 10  |
| reader_transport_channel_speed_record | FALSE   | -1   | 控制单并发读的速度, X条/S    | 10  |
| writer_transport_channel_speed_byte   | FALSE   | -1   | 控制单并发写的流量, Y字节/S  | 10  |
| writer_transport_channel_speed_record | FALSE   | -1   | 控制单并发写的速度, Y条/S    | 10  |

## Reader 模块

字节跳动数据集成目前支持多数据源写入的同时读取，在支持多数据源读取的场景下，要求上游数据源的输入数据schema需要保持一致，下面介绍Reader模块的具体信息。

示例：

``` json
{
    "job":{
        "reader":
   
            {
                "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat",
                "columns":[
                    {
                        "name":"id",
                        "type":"bigint"
                    },
                    {
                        "name":"name",
                        "type":"varchar"
                    }
                ],
                "table_name":"your table name",
                "db_name":"your database name",
                "password":"your database connection password",
                "user_name":"your database connection username",
                "split_pk":"your table primary key",
                "connections":[
                    {
                        "slaves":[
                            {
                                "port":"your connection's port",
                                "db_url":"your connection's url",
                                "host":"your connection's host"
                            }
                        ],
                        "shard_num":0,
                        "master":{
                            "port":"your connection's port",
                            "db_url":"your connection's url",
                            "host":"your connection's host"
                        }
                    }
                ]
            }
        
    }
}
```

通用参数：

| 参数名称                   | 是否必须  | 默认值 | 参数含义                                      | 示例                                                                 |
|------------------------|-------|-----|-------------------------------------------|--------------------------------------------------------------------|
| class                  | TRUE  | -   | 标识使用的connector的 class 名称                  | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat |
| reader_parallelism_num | FALSE | -   | 指定该Reader的并行度，默认情况下数据引擎会按照其实现逻辑计算得到一个并行度。 | 2                                                                  |

其他参数详情：参考具体 connector 实现参数

## Writer 模块

字节跳动数据集成目前支持同时写出到多个目标数据源，多个写出的目标数据源的schema需要保持一致。下面介绍Writer模块的具体组成信息。

``` json
{
    "writer":
        {
            "class":"com.bytedance.bitsail.connector.legacy.hive.sink.HiveParquetOutputFormat",
            "db_name":"your hive database' name.",
            "table_name":"your hive database' table name.",
            "partition":"your partition which want to add.",
            "metastore_properties":"{\"hive.metastore.uris\":\"thrift://localhost:9083\"}",
            "columns":[
                {
                    "name":"id",
                    "type":"bigint"
                }
            ],
            "write_mode":"overwrite",
            "writer_parallelism_num":1
        }
}

```

通用参数：

| 参数名称                   | 是否必须  | 默认值 | 参数含义                                      | 示例                                                                       |
|------------------------|-------|-----|-------------------------------------------|--------------------------------------------------------------------------|
| class                  | TRUE  | -   | 标识使用的connector的 class 名称                  | com.bytedance.bitsail.connector.legacy.hive.sink.HiveParquetOutputFormat |
| writer_parallelism_num | FALSE | -   | 指定该Writer的并行度，默认情况下数据引擎会按照其实现逻辑计算得到一个并行度。 | 2                                                                        |

其他参数详情：参考具体 [connector](../connectors/README.md) 实现参数
