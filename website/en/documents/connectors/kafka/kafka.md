# Kafka connector

Parent document: [Connectors](../README.md)

The Kafka connector supports the following functional points:

 - `At Least Once` write in batch scenarios
 - `Exactly Once` read in streaming scenarios

## Maven dependency

The Kafka connector internally uses `org.apache.kafka:kafka-clients` (version 1.0.1) for data writing. So when using kafka to write the connector, you need to pay attention that the target kafka cluster should be able to use this version of kafka-clients.

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-kafka</artifactId>
   <version>${revision}</version>
</dependency>
```


## Kafka reader


### Parameters

| Param name             | Required | Default value | Description                                                                                                               |
|------------------------|----------|---------------|---------------------------------------------------------------------------------------------------------------------------|
| class                  | Yes      |               | Reader class name for kafka connector,`com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceFunctionDAGBuilder` |
| child_connector_type   | Yes      |               | Only could be `kafka`                                                                                                     |
| reader_parallelism_num | No       |               | Reader parallelism num                                                                                                    |


#### Parameters for KafkaConsumer

The underlying Kafka connector uses `FlinkKafkaConsumer` for reading. The properties or kafka information of the initialized FlinkKafkaConsumer are passed in through options `job.reader.connector`. You can specify them as follows:


```json
{
  "job": {
    "reader": {
      "connector": {
        "prop_key": "prop_value"   // "prop_key" means property key, while "prop_val" means property value
      }
    }
  }
}
```

`job.reader.connector` supports KV configuration in the form of <string,string>, where:
- `prop_key`: FlinkKafkaConsumer property key
- `prop_value`: FlinkKafkaConsumer property key

Some common property used are listed below:

<b>1. Kafka cluster properties</b>

| Property key                | Required | Default value | Optional value | Description           |
|-----------------------------|----------|---------------|----------------|-----------------------|
| connector.bootstrap.servers | Yes      |               |                | kafka cluster address |
| connector.topic             | Yes      |               |                | topic to read         |
| connector.group.id          | Yes      |               |                | kafka consumer group  |

<b>2. Where to start consuming</b>

| Property key                 | Is necessary | Default value | Optional value                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Description                                                                                                                                            |
|------------------------------|--------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector.startup-mode       | No           | group-offsets | 1. `ealiest-offset`: Consume from the earliest offset of the partition<br>2. `latest-offset`: Consume from the latest offset of the partition<br>3. `group-offsets`: Comsume from the offset of the current consumer group<br>4. `specific-offsets`: Specify the offset for each partition, cooperate with `connector.specific-offsets`<br>5. `specific-timestamp`: Consume messages after a certain point in time, cooperate with `connector.specific-timestamp` | Decide from which offsets to consume                                                                                                                   |
| connector.specific-offsets   | No           |               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Used with specific-offsets, the format is a standard json string.<br>For example:<br>```[{"partition":1,"offset":100},{"partition":2,"offset":200}]``` |
| connector.specific-timestamp | No           |               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Used with specific-timestamp (ms) to specify the offset to consume                                                                                     |

<b>3. Other FlinkKafkaConsumer parameters</b>


FlinkKafkaConsumer supports many parameters, please refer to <a href="https://kafka.apache.org/21/javadoc/?org/apache/kafka/clients/consumer/ConsumerConfig.html">ConsumerConfig(2.1.0) API]</a> for details .
If the user needs to set these parameters, it can be configured through `connector.XXX`. 

For example, to set MAX_PARTITION_FETCH_BYTES_CONFIG to 1024, add the parameter:


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

#### Parameters for debugging

The Kafka read connector is used in streaming scenarios and will be consumed all the time under normal circumstances. 
If the user wants to debug by consuming only a limited amount of data, the following parameters can be configured. Note that these parameters need to be added to `job.reader` block.


| Property key | Is necessary | Default value  | Description |
| ------ | ---------- | ------- | ---- |
| enable_count_mode | No | false | Whether to end the current task after sending a piece of data, generally used for testing |
| count_mode_record_threshold| No | 10000 | If `enable_count_mode=true`, the current task ends after consuming `count_mode_record_threshold` pieces of messages |
|count_mode_run_time_threshold| No | 600 | If `enable_count_mode=true`, end the current task after running `count_mode_record_threshold` seconds |


### Supported message parsing modes

Messages can be pulled from KafkaConsumer in format of ConsumerRecord. **BitSail** supports two ways to handle ConsumerRecordof. The user can use `job.reader.format` to decide which method to use.

- `job.reader.format_type="json"`: Parse according to json format
    - In this mode, **BitSail** parses the json format string represented by value in ConsumerRecord according to the parameters `job.reader.columns`  set by the user.
    - Therefore, the parameters `job.reader.columns` is required in this mode
- `job.reader.format_type="streaming_file"`: Use raw byte value
    - In this mode, **BitSail** directly deliver the raw bytes value in ConsumerRecord. The specific structure is as follows:

```json
   [
    {"index":0, "name":"key", "type":"binary"},     // message key
    {"index":1, "name":"value", "type":"binary"},   // message value
    {"index":2, "name":"partition", "type":"string"}, // partition of the message
    {"index":3, "name":"offset", "type":"long"}     // offset of the meesage in partition
   ]
   ```


## Kafka Writer

### Parameters

Note that these parameters should be added to `job.writer` block.

#### Necessary parameters

| Param names   | Default value | Description                                                                                                 |
|---------------|---------------|-------------------------------------------------------------------------------------------------------------|
| class         |               | Writer class name of kafka connector, `com.bytedance.bitsail.connector.legacy.kafka.sink.KafkaOutputFormat` |
| kafka_servers |               | Kafka's bootstrap server address, multiple bootstrap server addresses are separated by `','`                |
| topic_name    |               | kafka topic                                                                                                 |
| columns       |               | Describing fields' names and data types                                                                     |

#### Optional parameters

| Param names            | Default value | Description                                                                                                                                                                                                                                                                                   |
|------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| writer_parallelism_num |               | writer parallelism num                                                                                                                                                                                                                                                                        |
| partition_field        |               | `partition_field` contains one or several fields from `job.writer.columns`, separated by commas (<i>e.g.</i> "id,timestamp"). If partition_field is not empty, when sending data to kafka topic, it will decide which topic to write based on the hash values ​​of these fields in the record |
| log_failures_only      | false         | When KafkaProducer fails to perform an asynchronous send operation:<br> 1. If `log_failures_only=true`, only log failure information<br> 2. If `log_failures_only=false`, throw an exception                                                                                                  |
| retries                | 10            | Number of failed retries for KafkaProducer                                                                                                                                                                                                                                                    |
| retry_backoff_ms       | 1000          | KafkaProducer's failure retry interval (ms)                                                                                                                                                                                                                                                   |
| linger_ms              | 5000          | The maximum waiting time (ms) for KafkaProducer to create a single batch                                                                                                                                                                                                                      |

#### Other parameters

When initializing the KafkaProducer, the user can use `job.common.optional` to pass initialization parameters, for example:

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

## Related documents

Configuration examples: [Kafka connector example](./kafka-example.md)
