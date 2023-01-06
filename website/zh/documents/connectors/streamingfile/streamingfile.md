# StreamingFile(流式HDFS)连接器

上级文档：[连接器](../README.md)

**StreamingFile** 连接器主要使用于流式场景中，提供以 `Exactly-Once` 语义的写入 HDFS、Hive 的能力，为实时数仓提供可靠的保障。

## 主要功能

- 写入支持`exactly once`。
- 提供多种提交策略，能够兼容数据完整性优先和数据时效性优先。
- 数据触发，有效解决延迟数据带来的数据漂移问题。
- Hive表结构自动发现，解决表结构变更，任务未及时重启的导致的数据不一致；

## 支持的数据类型

- HDFS
    - 无需关心数据数据结构；直接写入读取到的字节数组。
- HIVE
    - 基础类型
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
    - 复杂类型
        - Array
        - Map

## 主要参数

### 通用参数

| 参数名称             | 参数是否必须 | 参数默认值 | 参数枚举值              | 参数含义                                                                                       |
|:-----------------|:-------|:------|:-------------------|:-------------------------------------------------------------------------------------------|
| class            | 是      | -     |                    | com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder |
| dump.format.type | 是      | -     | hdfs<br/>hive<br/> | 写入哪种存储类型: HDFS 或者 Hive                                                                     |

### 通用优化参数

| 参数名称                        | 参数是否必须 | 参数默认值                        | 参数枚举值                                                          | 参数含义                                                                                                                                                                                                                                |   
|:----------------------------|:-------|:-----------------------------|:---------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enable_event_time           | 否      | False                        |                                                                | 是否开启归档                                                                                                                                                                                                                              |
| event_time_fields           | 否      | -                            |                                                                | 如果开启，指明归档字段的名称，这个名称是指字段在原始结构中的名称。                                                                                                                                                                                                   |    
| event_time_pattern          | 否      | -                            |                                                                | 如果该字段为空，则按照unix时间戳进行解析；如果该字段不为空，则按照该字段指定的格式进行解析，例如"yyyy-MM-dd HH:mm:ss"                                                                                                                                                             | 
| event_time.tag_duration     | 否      | 900000                       |                                                                | 单位:milliseconds，用于描述归档最大等待时间，计算公式为:当前event_time - 归档标签的时间 > event_time.tag_duration 就会生成这个小时的标签。例如，业务时间为:9:45，tag_duration=40min, 待生成的小时标签为8:00 9:45 - (8:00 + 60min) = 45min > 40min，则可以生成8点标签60min为默认需要等待一个小时才能打标签40min为需要额外等待的时间 |   
| dump.directory_frequency    | 否      | dump.directory_frequency.day | dump.directory_frequency.day<br/>dump.directory_frequency.hour | 输出格式为hdfs时，指定目录切分方式，dump.directory_frequency.day：按照日期进行目录划分dump.directory_frequency.hour：按照小时进行目录切分                                                                                                                                 | 
| rolling.inactivity_interval | 否      | -                            |                                                                | 单文件距离上次写入间隔指定切分文件                                                                                                                                                                                                                   |
| rolling.max_part_size       | 否      | -                            |                                                                | 单文件达到指定写入大小切分文件                                                                                                                                                                                                                     |
| partition_strategy          | 否      | partition_last               | partition_first,partition_last                                 | Hive 添加分区策略，支持 partition_last 和 partition_first 两种策略partition_last： 等分区所有数据都写入到hive后才添加hive 分区，添加分区延迟为天级任务 1 天，小时级任务 1 小时。partition_first：先加分区，适用于准实时场景，添加分区延迟为 1 个 Checkpoint 的间隔。                                                 |

### HDFS 参数

| 参数名称                   | 参数是否必须 | 参数默认值 | 参数枚举值                                                                                                                                                                             | 参数含义                                                                                                                     |
|:-----------------------|:-------|:------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------|
| dump.output_dir        | 是      | -     |                                                                                                                                                                                   | 输出格式为hdfs时，指定hdfs的输出路径。                                                                                                  |
| hdfs.dump_type         | 是      | -     | hdfs.dump_type.text:文本格式<br/>hdfs.dump_type.json: json格式<br/>hdfs.dump_type.msgpack: msgpack格式<br/>hdfs.dump_type.binary: 通过protobuf进行解析,需要配合proto.descriptor和proto.class_name参数。 | 解析的数据格式，按照需求进行填写                                                                                                         |
| partition_infos        | 是      | -     |                                                                                                                                                                                   | 写入hdfs的分区结构信息，对于hdfs来说，只可以是[{"name":"date","value":"yyyyMMdd","type":"TIME"},{"name":"hour","value":"HH","type":"TIME"}] |
| hdfs.replication       | 否      | 3     |                                                                                                                                                                                   | hdfs输出副本数量                                                                                                               |
| hdfs.compression_codec | 否      | None  |                                                                                                                                                                                   | hdfs压缩格式，请先确定当前使用的hadoop中支持哪些压缩方式后进行设置。None表示不压缩。                                                                        |
| hdfs.overwrite         | 否      | False |                                                                                                                                                                                   | 是否覆盖目标路径下原有文件。                                                                                                           |

### HIVE参数

| 参数名称                 | 参数是否必须 | 参数默认值 | 参数枚举值                                                                                                                                                                             | 参数含义                                                                                                                                                       |
|:---------------------|:-------|:------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| db_name              | 是      | -     |                                                                                                                                                                                   | 写入Hive库名                                                                                                                                                   | 
| table_name           | 是      | -     |                                                                                                                                                                                   | 写入Hive表名                                                                                                                                                   | 
| metastore_properties | 是      | -     |                                                                                                                                                                                   | Hive metastore的配置，包括url连接，以及一些其它可选配置。                                                                                                                      |
| source_schema        | 是      | -     |                                                                                                                                                                                   | 原始Schema信息，为string类型；例如[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"create_time\",\"type\":\"bigint\"}] | 
| sink_schema          | 是      | -     |                                                                                                                                                                                   | 目标Schema信息，为string类型；例如[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"create_time\",\"type\":\"bigint\"}] | 
| partition_infos      | 是      | -     |                                                                                                                                                                                   | 写入hive的分区结构信息，对于hive来说，可以按照分区来进行填写，如果是存在小时级别分区，示例为[{\"name\":\"date\",\"type\":\"TIME\"},{\"name\":\"hour\",\"type\":\"TIME\"}]                            |
| hdfs.dump_type       | 是      | -     | hdfs.dump_type.text:文本格式<br/>hdfs.dump_type.json: json格式<br/>hdfs.dump_type.msgpack: msgpack格式<br/>hdfs.dump_type.binary: 通过protobuf进行解析,需要配合proto.descriptor和proto.class_name参数。 | 解析的数据格式，按照需求进行填写                                                                                                                                           |

## 相关文档

配置示例文档：[StreamingFile(流式HDFS)连接器](./streamingfile_example.md)
