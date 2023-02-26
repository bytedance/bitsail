---
order: 3
---

# Job Configuration Guide

English | [简体中文](../../../zh/documents/start/config.md)

-----

***BitSail*** script configuration is managed by JSON structure, follow scripts show the complete structure:

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

| Module Name    | Description                                                                                                                                                                                                                                                                                         |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| common         | It is mainly responsible for general setting, such job metadata, plugins setting.                                                                                                                                                                                                                   |
| reader/readers | It is mainly responsible for setting relevant parameter information on the source data side. Taking the MySQL data source as an example, you need to set the JDBC connection information and the database table information of the operation under the subdomain of the reader.                     |
| writer/writers | Mainly responsible for setting the relevant parameters of the target data source, etc. Taking the Hive target data source as an example, you need to set the Hive metastore connection information under the writer's subdomain, and set the Hive database table and partition related information. |

## Common Module

Example：

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
Description：

Metadata Parameters:

| Parameter name | Required | Default | Description                                            | Example      |
|----------------|----------|---------|--------------------------------------------------------|--------------|
| user_name      | TRUE     | -       | job's submitter                                        | bitsail      |
| job_id         | TRUE     | -       | job' unique id                                         | 12345        |
| instance_id    | TRUE     | -       | job's instance id, maybe use in some scheduler system. | 12345        |
| job_name       | TRUE     | -       | job's name                                             | bitsail_conf |

Parameter parallelism：

| Parameter name    | Required | Default | Description                                                                                                                                            | Example |
|-------------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| min_parallelism   | FALSE    | 1       | The minimum parallelism of the job, the parallelism from automatic calculation will be greater than or equal to the minimum parallelism.               | 2       |
| max_parallelism   | FALSE    | 512     | The maximum parallelism of the job, the parallelism from automatic calculation will be less than or equal to the maximum parallelism.                  | 2       |
| parallelism_chain | FALSE    | FALSE   | Whether chain the operator between operators. If this option is enabled, will select min parallelism between readers and writers as final parallelism. | 2       |

Dirty record setting:(Only in batch mode)

| Parameter name                    | Required | Default | Description                                                                                                                       | Example |
|-----------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------|---------|
| max_dirty_records_stored_num      | FALSE    | 50      | Every task collect size for dirty record.                                                                                         | 50      |
| dirty_records_count_threshold     | FALSE    | -1      | The threshold of the total dirty records, if dirty records bigger than the threshold, job will fail in final                      | -1      |
| dirty_record_percentage_threshold | FALSE    | -1      | The percent threshold of the total dirty records, if dirty records percent bigger than the threshold,     job will fail in final. | -1      |

Flow control setting:

| Parameter name                         | Required | Default | Description                                                                        | Example |
|----------------------------------------|----------|---------|------------------------------------------------------------------------------------|---------|
| reader_transport_channel_speed_byte    | FALSE    | -1      | This param controls the traffic of a single concurrent reading, X bytes per second | 10      |
| reader_transport_channel_speed_record  | FALSE    | -1      | This param controls the speed of a single concurrent reading, X rows per second    | 10      |
| writer_transport_channel_speed_byte    | FALSE    | -1      | This param controls the traffic of a single concurrent writing, Y bytes per second | 10      |
| writer_transport_channel_speed_record  | FALSE    | -1      | This param controls the speed of a single concurrent writing, Y rows per second    | 10      |

## Reader Module

Examples：

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

Common Parameter：

| Parameter name         | Required | Default | Description                                      | Example                                                            |
|------------------------|----------|---------|--------------------------------------------------|--------------------------------------------------------------------|
| class                  | TRUE     | -       | Connector's class name                           | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat |
| reader_parallelism_num | FALSE    | -       | Specify the parallelism for the reader operator. | 2                                                                  |

Other parameters please check the [connector](./connectors)

## Writer Module

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

Common Parameters：

| Parameter name         | Required | Default | Description                                                                                 | Example                                                                  |
|------------------------|----------|---------|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| class                  | TRUE     | -       | Connector's class name                                                                      | com.bytedance.bitsail.connector.legacy.hive.sink.HiveParquetOutputFormat |
| writer_parallelism_num | FALSE    | -       | Specify Writer's parallelism, default bitsail will calculate write parallelism for the job. | 2                                                                        |

Other parameters please check the [connector](../connectors/README.md)
