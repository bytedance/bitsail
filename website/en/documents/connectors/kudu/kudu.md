# Kudu connector

Parent document: [Connectors](../README.md)

**BitSail** Kudu connector supports reading and writing kudu tables.

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-kudu</artifactId>
   <version>${revision}</version>
</dependency>
```

-----

## Kudu Reader

Kudu reader us scanner to read table, supporting common Kudu data types:

- Integer: `int8, int16, int32, int64`'
- Float number: `float, double, decimal`
- Bool: `boolean`
- Date & Time: `date, timestamp`
- String: `string, varchar`
- Binary: `binary, string_utf8`

### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.kudu.source.KuduSource",
      "kudu_table_name": "kudu_test_table",
      "kudu_master_address_list": ["localhost:1234", "localhost:4321"]
    }
  }
}
```

#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | yes  |       | Kudu reader's class name, `com.bytedance.bitsail.connector.kudu.source.KuduSource` |
| kudu_table_name | yes | | Kudu table to read |
| kudu_master_address_list | yes | | Kudu master addresses in list format |
| columns | yes | | The name and type of columns to read |
| reader_parallelism_num | no | | reader parallelism |


#### KuduClient related parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| kudu_admin_operation_timeout_ms | no | | Kudu client admin operation's  timeout. Unit is ms, default 30000ms |
| kudu_operation_timeout_ms | no | | Kudu client operation's timeout. Unit is ms, default 30000ms |
| kudu_connection_negotiation_timeout_ms | no  | |  Unit is ms，default 10000ms |
| kudu_disable_client_statistics | no  | | If to enable statistics in kudu client |
| kudu_worker_count | no | | client worker number. | 
| sasl_protocol_name | no | | Default "kudu" |
| require_authentication | no  | | If to enable authentication. |
| encryption_policy | no | OPTIONAL<br/>REQUIRED_REMOTE<br/>REQUIRED | encryption polocy. | 


#### KuduScanner related parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| read_mode | no | READ_LATEST<br/>READ_AT_SNAPSHOT | read mode |
| snapshot_timestamp_us | yes if read_mode=READ_AT_SNAPSHOT |  | specify which snapshot to read |
| enable_fault_tolerant | no | | If to enable fault tolerant |
| scan_batch_size_bytes | no | | Max bytes number in single batch |
| scan_max_count | no | | Max number of rows to scan |
| enable_cache_blocks| no |  | If to enable cache blocks, default true |
| scan_timeout_ms | no | | scan timeout. Unit is ms, default 30000ms |
| scan_keep_alive_period_ms | no | | |

#### Split related parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| split_strategy | no | SIMPLE_DIVIDE | Split strategy. Only support SIMPLE_DIVIDE now. |
| split_config | yes | | Split configuration for each strategy. |

##### SIMPLE_DIVIDE split strategy 
SIMPLE_DIVIDE strategy uses the following format of split_config:
```text
"{\"name\": \"key\", \"lower_bound\": 0, \"upper_bound\": \"10000\", \"split_num\": 3}"
```
- `name`: the name of split column, only support int8, int16, int32, int64 type.
- `lower_bound`: The min value of the split column (Scan table to get the min value if it is not set).
- `upper_bound`: The max value of the split column (Scan table to get the max value if it is not set).
- `split_num`: Number of split (Use the reader parallelism if it is not set).

SIMPLE_DIVIDE strategy will evenly divide the range `[lower_bound, upper_bound]` into split_num sub ranges, and each range is a split.


-----

## Kudu Writer

### Supported data type

Support common Kudu data types:


- Integer: `int8, int16, int32, int64`'
- Float number: `float, double, decimal`
- Bool: `boolean`
- Date & Time: `date, timestamp`
- String: `string, varchar`
- Binary: `binary, string_utf8`

### Supported operation type

Support the following operations:

- INSERT, INSERT_IGNORE
- UPSERT
- UPDATE, UPDATE_IGNORE


### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.kudu.sink.KuduSink",
      "kudu_table_name": "kudu_test_table",
      "kudu_master_address_list": ["localhost:1234", "localhost:4321"],
      "kudu_worker_count": 2
    }
  }
}
```


#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| class             | yes  |       | Kudu writer's class name, `com.bytedance.bitsail.connector.kudu.sink.KuduSink` |
| kudu_table_name | yes | | Kudu table to write |
| kudu_master_address_list | yes | | Kudu master addresses in list format |
| columns | yes | | The name and type of columns to write |
| writer_parallelism_num | no | | writer parallelism |

#### KuduClient related parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| kudu_admin_operation_timeout_ms | no | | Kudu client admin operation's  timeout. Unit is ms, default 30000ms |
| kudu_operation_timeout_ms | no | | Kudu client operation's timeout. Unit is ms, default 30000ms |
| kudu_connection_negotiation_timeout_ms | no  | |  Unit is ms，default 10000ms |
| kudu_disable_client_statistics | no  | | If to enable statistics in kudu client |
| kudu_worker_count | no | | client worker number. | 
| sasl_protocol_name | no | | Default "kudu" |
| require_authentication | no  | | If to enable authentication. |
| encryption_policy | no | OPTIONAL<br/>REQUIRED_REMOTE<br/>REQUIRED | encryption polocy. |

#### KuduSession related parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:-----------------------------|:---------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| kudu_session_flush_mode | no | AUTO_FLUSH_SYNC<br/>AUTO_FLUSH_BACKGROUND | Session's flush mode. Default AUTO_FLUSH_BACKGROUND |
| kudu_mutation_buffer_size | no | | The number of operations that can be buffered |
| kudu_session_flush_interval | no | | session flush interval，unit is ms | 
| kudu_session_timeout_ms | no | | Timeout for operations. The default timeout is 0, which disables the timeout functionality. |
| kudu_session_external_consistency_mode | no | CLIENT_PROPAGATED<br/>COMMIT_WAIT | External consistency mode for kudu session, default CLIENT_PROPAGATED |
| kudu_ignore_duplicate_rows | no | | Whether ignore all the row errors if they are all of the AlreadyPresent type. Throw exceptions if false. Default false. |


-----

## Related documents

Configuration example: [Kudu connector example](./kudu-example.md)
