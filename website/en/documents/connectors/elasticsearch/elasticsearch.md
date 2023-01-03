# Elasticsearch connector

Parent document: [Connectors](../README.md)

## Main function

The Elasticsearch connector can be used in stream and batch scenarios, providing the ability to write elasticsearch in `'At Least Once'` mode, and providing flexible write request construction.

## Supported version
- Support Elasticsearch 7.X

## Maven depedency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-elasticsearch</artifactId>
   <version>${revision}</version>
</dependency>
```

## Supported data types

Basic data types supported by Elasticsearch connectors:

- String type:
    - string
    - text
    - keyword
- Integer type:
    - long
    - integer
    - short
    - byte
- Float type:
    - double
    - float
    - half_float
    - scaled_float
- Bool type:
    - boolean
- Binary type:
    - binary
- Date type:
    - date

## Parameters

Users can add parameters to `job.writer` block in task configuration files.


### Necessary parameters

| Param name | Default value | Optional value | Description                                                                                                             |
|:-----------|:--------------|:---------------|:------------------------------------------------------------------------------------------------------------------------|
| class      | -             |                | Class name of Elasticsearch connector，`com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchSink` |
| es_hosts   | -             |                | Address list for Elasticsearch handling REST requests                                                                   |
| es_index   | -             |                | Elasticsearch index                                                                                                     |
| columns    | -             |                | Describing fields' names and types                                                                                      |


### Optional parameters

#### General optional parameters
| Param name             | Default value | Optional value | Description        |
|:-----------------------|:--------------|:---------------|:-------------------|
| writer_parallelism_num |               |                | writer parallelism |

#### Parameters for construct REST request
| Param name                    | Default value | Optional value | Description                                                               |
|:------------------------------|:--------------|:---------------|:--------------------------------------------------------------------------|
| request_path_prefix           | -             |                | The path prefix used by the http client when making a request             |
| connection_request_timeout_ms | 10000         |                | Timeout (ms) used by http connection manager when requesting a connection |
| connection_timeout_ms         | 10000         |                | Http connection establishment timeout (ms)                                |
| socket_timeout_ms             | 60000         |                | Socket timeout for http connection (ms)                                   |

#### Parameters for bulk request

| Param name                   | Default value | Optional value                  | Description                                                                                                                                       |
|:-----------------------------|:--------------|:--------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| bulk_flush_max_actions       | 300           |                                 | When the number of requests reaches, execute a bulk operation                                                                                     |
| bulk_flush_max_size_mb       | 10            |                                 | When the request data size (in MB) reaches, execute a bulk operation                                                                              |
| bulk_flush_interval_ms       | 10000         |                                 | How often to execute bulk operation (unit: ms)                                                                                                    |
| bulk_backoff_policy          | EXPONENTIAL   | CONSTANT<br>EXPONENTIAL<br>NONE | Backoff policy when bulk operation fails:<br>1. `CONSTANT`: fixed delay backoff<br>2. `EXPONENTAIL`: exponential backoff<br>3. `NONE`: no backoff |
| bulk_backoff_delay_ms        | 100           |                                 | Failure retry delay (ms) of bulk operation                                                                                                        |
| bulk_backoff_max_retry_count | 5             |                                 | The maximum number of failed retries for bulk operations                                                                                          |

#### Parameters for building ActionRequests

| Param name               | Default value | Optional value                                          | Description                                                                                                                           |
|:-------------------------|:--------------|:--------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------|
| es_operation_type        | "index"       | "index"<br>"create"<br>"update"<br>"upsert"<br>"delete" | Type of ActionRequest                                                                                                                 |
| es_dynamic_index_field   | -             |                                                         | Get the index name of this data to insert from this field                                                                             |
| es_operation_type_field  | -             |                                                         | Get the ActionRequest type of this data from this field                                                                               |
| es_version_field         | -             |                                                         | Get the version information of this data from this field                                                                              |
| es_id_fields             | ""            |                                                         | Get the document ID from this field.<br>The format is `','` separated string, <i>e.g.</i> `"1,2"`                                     |
| doc_exclude_fields       | ""            |                                                         | When creating a document, ignore these fields. The format is `','` separated string, for example: `"1,2"`                             |
| ignore_blank_value       | false         |                                                         | Whether to ignore fields with null values when creating documents                                                                     |
| flatten_map              | false         |                                                         | Whether to expand the `Map` type data into the document when creating the document                                                    |
| id_delimiter             | `#`           |                                                         | The separator used when merging multiple fields into one document id                                                                  |
| json_serializer_features | -             |                                                         | Json features used when building json strings. The format is `','` separated string, for example: `"QuoteFieldNames,UseSingleQuotes"` |


## Related documents

Configuration examples: [Elasticsearch connector example](./elasticsearch-example.md)
