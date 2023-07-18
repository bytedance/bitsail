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

| Param name                   | Required | Optional value | Description                               |
|:-----------------------------|:---------|:---------------|:------------------------------------------|
| read_mode | no | READ_LATEST<br/>READ_AT_SNAPSHOT | read mode                                 |
| snapshot_timestamp_us | yes if read_mode=READ_AT_SNAPSHOT |  | specify which snapshot to read            |
| enable_fault_tolerant | no | | If to enable fault tolerant               |
| scan_batch_size_bytes | no | | Max bytes number in single batch          |
| scan_max_count | no | | Max number of rows to scan                |
| enable_cache_blocks| no |  | If to enable cache blocks, default false  |
| scan_timeout_ms | no | | scan timeout. Unit is ms, default 30000ms |
| scan_keep_alive_period_ms | no | |                                           |
 | predicates | no | | predicate json string|

#### predicates

Query predicates on columns. Unlike traditional SQL syntax,
the simple query predicates are represented in a simple JSON 
syntax. Three types of predicates are supported, including 'Comparison',
'InList' and 'IsNull'.
* The 'Comparison' type support <=, <, =, > and >=,
     which can be represented as '[operator, column_name, value]',

     `e.g. '[">=", "col1", "value"]'`
* The 'InList' type can be represented as '["IN", column_name, [value1, value2, ...]]'

     `e.g. '["IN", "col2", ["value1", "value2"]]'`
* The 'IsNull' type determine whether the value is NULL or not, which can be represented as '[operator, column_name]'
     
     `e.g. '["NULL", "col1"]', or '["NOTNULL", "col2"]'`

Predicates can be combined together with predicate operators using the syntax 
`[operator, predicate, predicate, ..., predicate].`
For example,
`["AND", [">=", "col1", "value"], ["NOTNULL", "col2"]]` 
The only supported predicate operator is `AND`.) type: string default: ""


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
