# StreamingFile connector

Parent document: [Connectors](../README.md)

*StreamingFile* Connector mainly used in streaming, and it supports write both hdfs and use hive `Exactly-Once` semantics.
Provide reliable guarantee for real-time data warehouses.

## Feature

- Support `Exactly Once`。
- Support multi committer, compatible difference situations like data integrity or high timeliness.
- Data Trigger, effectively solve problems like delayed data or out of order .
- Hive DDL automatic detection, reduce manually align schema with hive. 

## Support data types

- HDFS
    - No need to care about the data types; write byte array directly.
- HIVE
    - Basic data types.
        - TINYINT
        - SMALLINT
        - INT
        - BIGINT
        - BOOLEAN
        - FLOAT
        - DOUBLE
        - STRING
        - BINARY
        - TIMESTAMP
        - DECIMAL
        - CHAR
        - VARCHAR
        - DATE
    - Complex data types.
        - Array
        - Map

## Parameters

### Common Parameters

| Name             | Required | Default Value | Enumeration Value  | Comments                                                                                   |
|:-----------------|:---------|:--------------|:-------------------|:-------------------------------------------------------------------------------------------|
| class            | Yes      | -             |                    | com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder |
| dump.format.type | Yes      | -             | hdfs<br/>hive<br/> | Write `hdfs` or `hive`                                                                     |

### Advanced Parameters

| Name                        | Required | Default Value                | Enumeration Value                                              | Comments                                                                                                                                                                                                                                                                                                                                                           |
|:----------------------------|:---------|:-----------------------------|:---------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enable_event_time           | No       | False                        |                                                                | Enable event time or not.                                                                                                                                                                                                                                                                                                                                          |
| event_time_fields           | No       | -                            |                                                                | If enable event time, use this parameter to specify the field name in the record.                                                                                                                                                                                                                                                                                  |    
| event_time_pattern          | No       | -                            |                                                                | If enable event time，if this parameter is null then use unix timestamp to parse the `event_time_fields`. If this field is not empty, use this field's value to parse the field value, examples: "yyyy-MM-dd HH:mm:ss"                                                                                                                                              | 
| event_time.tag_duration     | No       | 900000                       |                                                                | Unit:millisecond. Maximum wait time for the event time trigger. The formula: event_time - pending_commit_time > event_time.tag_duration, then will trigger the event time.Example: current event time=9：45, tag_duration=40min, pending trigger_commit_time=8:00, then 9:45 - (8:00 + 60min) = 45min > 40min the result is true, then event time could be trigger. |   
| dump.directory_frequency    | No       | dump.directory_frequency.day | dump.directory_frequency.day<br/>dump.directory_frequency.hour | Use for write hdfs.<br/> dump.directory_frequency.day:/202201/xx_data<br/> dump.directory_frequency.hour: /202201/01/data                                                                                                                                                                                                                                          | 
| rolling.inactivity_interval | No       | -                            |                                                                | The interval of the file rolling.                                                                                                                                                                                                                                                                                                                                  |
| rolling.max_part_size       | No       | -                            |                                                                | The file size of the file rolling.                                                                                                                                                                                                                                                                                                                                 |
| partition_strategy          | No       | partition_last               | partition_first<br/>partition_last                             | Committer strategy. partition_last: Waiting for all data ready then add hive partition to metastore.partition_first：add partition first。                                                                                                                                                                                                                           |

### HDFS Parameters

| Name                   | Required | Default Value | Enumeration Value                                                                                                                                                                            | Comments                                                                                                                                                              |
|:-----------------------|:---------|:--------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dump.output_dir        | Yes      | -             |                                                                                                                                                                                              | The location of hdfs output.                                                                                                                                          |
| hdfs.dump_type         | Yes      | -             | hdfs.dump_type.text<br/>hdfs.dump_type.json<br/>hdfs.dump_type.msgpack<br/>hdfs.dump_type.binary: `protobuf record, need use with follow parameters, proto.descriptor and proto.class_name`. | How the parse the record for the event_time                                                                                                                           |
| partition_infos        | Yes      | -             |                                                                                                                                                                                              | The partition for the hdfs directory, hdfs only can be the follow value [{"name":"date","value":"yyyyMMdd","type":"TIME"},{"name":"hour","value":"HH","type":"TIME"}] |
| hdfs.replication       | No       | 3             |                                                                                                                                                                                              | hdfs replication num.                                                                                                                                                 |
| hdfs.compression_codec | No       | None          |                                                                                                                                                                                              | hdfs file compression strategy.                                                                                                                                       |

### Hive Parameters

| Name                 | Required | Default Value | Enumeration Value                                                                                                                                                                         | Comments                                                                                                                                               |
|:---------------------|:---------|:--------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------|
| db_name              | Yes      | -             |                                                                                                                                                                                           | Database name for hive.                                                                                                                                | 
| table_name           | Yes      | -             |                                                                                                                                                                                           | Table name for hive.                                                                                                                                   | 
| metastore_properties | Yes      | -             |                                                                                                                                                                                           | Hive metastore configuration. eg: {\"metastore_uris\":\"thrift:localhost:9083\"}                                                                       |
| source_schema        | Yes      | -             |                                                                                                                                                                                           | Source schema, eg: [{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"create_time\",\"type\":\"bigint\"}] | 
| sink_schema          | Yes      | -             |                                                                                                                                                                                           | Sink schema, eg: [{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"create_time\",\"type\":\"bigint\"}]   | 
| partition_infos      | Yes      | -             |                                                                                                                                                                                           | Hive partition definition, eg: [{\"name\":\"date\",\"type\":\"TIME\"},{\"name\":\"hour\",\"type\":\"TIME\"}]                                           |
| hdfs.dump_type       | Yes      | -             | hdfs.dump_type.text<br/>hdfs.dump_type.json<br/>hdfs.dump_type.msgpack<br/>hdfs.dump_type.binary: protobuf record, need use with follow parameter, proto.descriptor and proto.class_name。 |                                                                                                                                                        |

## Reference docs

Configuration examples: [StreamingFile connector example](./streamingfile_example.md)
