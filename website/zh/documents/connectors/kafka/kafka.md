# Kafka 连接器

上级文档：[连接器](../README.md)

Kafka 连接器支持如下功能点:

 - 批式场景下支持 `At Least Once` 的写入
 - 流式场景下支持 `Exactly Once` 的读取

## 依赖引入

Kafka连接器内部使用 1.0.1 版本的`org.apache.kafka:kafka-clients`进行数据写入，因此在使用kafka写连接器时时需要注意目标kafka集群能使用此版本`kafka-clients`连接。

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-kafka</artifactId>
   <version>${revision}</version>
</dependency>
```


## Kafka读连接器


### 参数配置

| 参数名                    | 参数是否必需 | 参数默认值 | 参数说明                                                                                               |
|------------------------|--------|-------|----------------------------------------------------------------------------------------------------|
| class                  | 是      |       | Kafka读连接器类名，只能为`com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceFunctionDAGBuilder` |
| child_connector_type   | 是      |       | 指定连接的消息队列种类，只能为`kafka`                                                                             |
| reader_parallelism_num | 否      |       | 读并发数                                                                                               |


#### KafkaConsumer属性设置

Kafka连接器底层使用FlinkKafkaConsumer进行读取。初始化FlinkKafkaConsumer的属性或者kafka信息均通过选项`job.reader.connector`传入，具体形式如下:

```json
{
  "job": {
    "reader": {
      "connector": {
        "prop_key": "prop_value"   // "prop_key"和"prop_value"代指属性key和属性value
      }
    }
  }
}
```

`job.reader.connector`支持 <string,string> 形式的KV配置，其中:
 - `prop_key`: KafkaConsumer属性key
 - `prop_value`: KafkaConsumer属性value

下面列举了一些常见的属性key:

<b>1. kafka集群属性</b>

| 属性key                       | 属性是否必需 | 属性默认值 | 属性可选值 | 属性说明         |
|-----------------------------|--------|-------|-------|--------------|
| connector.bootstrap.servers | 是      |       |       | 读取的kafka集群地址 |
| connector.topic             | 是      |       |       | 读取的topic     |
| connector.group.id          | 是      |       |       | kafka消费组     |

<b>2. kafka起始消费属性</b>

| 属性key                        | 属性是否必需 | 属性默认值         | 属性可选值                                                                                                                                                                                                                                                                                                          | 属性说明                                                                                                       |
|------------------------------|--------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| connector.startup-mode       | 否      | group-offsets | 1. `ealiest-offset`: 从partition的最早offset开始消费<br>2. `latest-offset`: 从partition的最新offset开始消费<br>3. `group-offsets`: 设置从当前consumer group的offset开始消费<br>4. `specific-offsets`: 指定各个partition的起始消费offset，配合connector.specific-offsets使用<br>5. `specific-timestamp`: 指定消费某个时间点后的数据，配合connector.specific-timestamp使用 | 决定kafka从何处开始消费                                                                                             |
| connector.specific-offsets   | 否      |               |                                                                                                                                                                                                                                                                                                                | 配合specific-offsets使用，格式为标准json字符串，例如:<br>```[{"partition":1,"offset":100},{"partition":2,"offset":200}]``` |
| connector.specific-timestamp | 否      |               |                                                                                                                                                                                                                                                                                                                | 配合specific-timestamp使用，指定KafkaConsumer的消费起始时间戳，单位ms                                                        |

<b>3. 其他FlinkKafkaConsumer参数</b>

FlinkKafkaConsumer支持许多参数，详情可参考<a href="https://kafka.apache.org/21/javadoc/?org/apache/kafka/clients/consumer/ConsumerConfig.html">ConsumerConfig(2.1.0) API]</a>。

若用户需要设置这些参数，可通过此 connector.XXX 来进行配置。
例如，若要设置MAX_PARTITION_FETCH_BYTES_CONFIG为1024，则添加参数：

```json
{
  "job": {
    "reader": {
      "connector": {
        "connector.max.partition.fetch.bytes": "1024"
      }
    }
  }
}
```

#### debug参数

Kafka读连接器用于流式场景，正常情况下会一直消费。若用户想通过只消费有限数量的数据进行debug，则可通过以下参数进行配置。注意这些参数需要添加`job.reader`前缀。 


| 参数名                           | 参数是否必需 | 参数默认值 | 参数说明                                                                   |
|-------------------------------|--------|-------|------------------------------------------------------------------------|
| enable_count_mode             | 否      | false | 是否在发送一段数据后结束当前任务，一般用于测试                                                |
| count_mode_record_threshold   | 否      | 10000 | 若enable_count_mode=true，则在kafka消费count_mode_record_threshold条数据后结束当前任务 |
| count_mode_run_time_threshold | 否      | 600   | 若enable_count_mode=true，则在任务运行count_mode_record_threshold秒后结束当前任务      |


### 支持的消息解析模式

从KafkaConsumer可以拉取到消息`ConsumerRecord`。 **BitSail** 支持两种对`ConsumerRecord`的处理方式。
用户可通过参数`job.reader.format_type`决定使用哪种方式:
 - `job.reader.format_type="json"`: 按照json格式解析
    - 在此模式下，**BitSail** 按照用户设置的参数 `job.reader.columns` ，对ConsumerRecord中value代表的json格式字符串进行解析。
    - 因此此模式下必需`job.reader.columns`参数
 - `job.reader.format_type="streaming_file"`: 不解析
    - 在此模式下，会直接将ConsumerRecord中的信息传出，具体结构如下:
   
```json
    [
   {"index":0, "name":"key", "type":"binary"},     // 消息key
   {"index":1, "name":"value", "type":"binary"},   // 消息value
   {"index":2, "name":"partition", "type":"string"}, // 消息来源partition
   {"index":3, "name":"offset", "type":"long"}     // 消息在partition中的offset
    ]
```


## kafka写连接器

### 参数配置

注意这些参数需要添加`job.writer`前缀。

#### 必需参数

| 参数名           | 参数默认值 | 参数说明                                                                                 |
|---------------|-------|--------------------------------------------------------------------------------------|
| class         |       | kafka写连接器类名，只能为`com.bytedance.bitsail.connector.legacy.kafka.sink.KafkaOutputFormat` |
| kafka_servers |       | kafka的bootstrap server地址，多个bootstrap server地址由逗号 `','` 分隔                            |
| topic_name    |       | 要写入的topic                                                                            |
| columns       |       | 数据字段名称及字段类型                                                                          |

#### 可选参数

| 参数名                    | 参数默认值 | 参数说明                                                                                                                                                   |
|------------------------|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| writer_parallelism_num | -     | 写并发数                                                                                                                                                   |
| partition_field        | -     | partition_field包含“job.writer.columns”中的一个或几个field，以逗号分隔（例如："id,timestamp"）。<br>若partition_field不为空，在发送数据到kafka topic时，会根据record中的这几个field的值决定写入哪个topic |
| log_failures_only      | false | 在KafkaProducer执行异步send操作失败时:<br>1. 若log_failures_only=true，则只通过log.error发送失败信息<br>2. 若log_failures_only=false，则抛出异常                                    |
| retries                | 10    | KafkaProducer的失败重试次数                                                                                                                                   |
| retry_backoff_ms       | 1000  | KafkaProducer的失败重试间隔，单位ms                                                                                                                              |
| linger_ms              | 5000  | KafkaProducer创建单个batch的最大等待时间，单位ms                                                                                                                     |

#### 其他参数

在初始化KafkaProducer时，用户可通过参数 `job.common.optional` 传入指定的初始化参数，示例如下:

```json
{
   "job": {
      "common": {
         "optional": {
            "batch.size": 16384,
            "buffer.memory": 33554432
         }
      }
   }
}
```

## 相关文档

配置示例文档：[Kafka 连接器示例](./kafka-example.md)
