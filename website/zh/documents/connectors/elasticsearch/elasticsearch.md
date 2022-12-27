# Elasticsearch 连接器

上级文档：[连接器](../README.md)

## 主要功能

Elasticsearch连接器可用于流、批场景，提供`At-Least-Once`语义地写入elasticsearch 地能力，并提供灵活地写入请求构建。

## 支持的版本信息
 - 支持Elasticsearch 7.X

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-elasticsearch</artifactId>
   <version>${revision}</version>
</dependency>
```

## 支持的数据类型

Elasticsearch连接器支持基本的字段类型:

 - 字符串类型:
    - string
    - text
    - keyword
 - 整数类型:
    - long
    - integer
    - short
    - byte
 - 浮点类型:
    - double
    - float
    - half_float
    - scaled_float
 - 布尔类型
    - boolean
 - 二进制类型
    - binary
 - 日期类型
    - date

## 主要参数

用户可通过在任务配置文件的 `job.writer` 块中添加如下参数。

### 必需参数

| 参数名称     | 参数默认值 | 参数枚举值 | 参数含义                                                                                                    |
|:---------|:------|:------|:--------------------------------------------------------------------------------------------------------|
| class    | -     |       | Elasticsearch连接器类名，只能为`com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchSink` |
| es_hosts | -     |       | Elasticsearch集群接受restful请求的地址列表                                                                         |
| es_index | -     |       | 要写入的elasticsearch索引                                                                                     |
| columns  | -     |       | 数据字段名称及类型                                                                                               |


### 可选参数

#### 通用可选参数
| 参数名称                   | 参数默认值 | 参数枚举值 | 参数含义 |
|:-----------------------|:------|:------|:-----|
| writer_parallelism_num |       |       | 写并发数 |

#### Restful请求参数
| 参数名称                          | 参数默认值 | 参数枚举值 | 参数含义                      |
|:------------------------------|:------|:------|:--------------------------|
| request_path_prefix           | -     |       | http客户端发起请求时使用的路径前缀       |
| connection_request_timeout_ms | 10000 |       | http连接管理器请求连接时使用的超时时间（毫秒） |
| connection_timeout_ms         | 10000 |       | http连接建立超时时间（毫秒）          |
| socket_timeout_ms             | 60000 |       | http连接的套接字超时时间（毫秒）        |

#### Bulk操作参数

| 参数名称                         | 参数默认值       | 参数枚举值                           | 参数含义                                                                                   |
|:-----------------------------|:------------|:--------------------------------|:---------------------------------------------------------------------------------------|
| bulk_flush_max_actions       | 300         |                                 | request数量到达多少时，执行一次bulk操作                                                              |
| bulk_flush_max_size_mb       | 10          |                                 | 请求数据大小（单位MB）到达多少时，执行一次bulk操作                                                           |
| bulk_flush_interval_ms       | 10000       |                                 | 每隔多久执行一次bulk操作（单位ms）                                                                   |
| bulk_backoff_policy          | EXPONENTIAL | CONSTANT<br>EXPONENTIAL<br>NONE | bulk操作失败时的重试策略:<br>1. `CONSTANT`: 固定延迟重试<br>2. `EXPONENTAIL`: 指数回退重试<br>3. `NONE`: 不重试 |
| bulk_backoff_delay_ms        | 100         |                                 | bulk操作的失败重试延迟，单位ms                                                                     |
| bulk_backoff_max_retry_count | 5           |                                 | bulk操作的失败最大重试次数                                                                        |

#### ActionRequest构建参数

| 参数名称                     | 参数默认值   | 参数枚举值                                                   | 参数含义                                                                          |
|:-------------------------|:--------|:--------------------------------------------------------|:------------------------------------------------------------------------------|
| es_operation_type        | "index" | "index"<br>"create"<br>"update"<br>"upsert"<br>"delete" | 决定创建的ActionRequest类型                                                          |
| es_dynamic_index_field   | -       |                                                         | 从源数据的该字段获取这条数据插入的索引名                                                          |
| es_operation_type_field  | -       |                                                         | 从源数据的该字段获取这条数据的ActionRequest类型                                                |
| es_version_field         | -       |                                                         | 从源数据的该字段获取这条数据的版本信息                                                           |
| es_id_fields             | ""      |                                                         | 从源数据的该字段获取文档id。格式为 `','` 分隔的下标字符串，例如: `"1,2"`                                 |
| doc_exclude_fields       | ""      |                                                         | 在创建文档时，忽略这些下标所在的字段。格式为 `','` 分隔的下标字符串，例如: `"1,2"`                             |
| ignore_blank_value       | false   |                                                         | 在创建文档时，是否忽略源数据中的值为空的字段                                                        |
| flatten_map              | false   |                                                         | 在创建文档时，是否将数据源中的Map类型数据展开放入文档                                                  |
| id_delimiter             | `#`     |                                                         | 在将多个字段合并成一个文档id时使用的分隔符                                                        |
| json_serializer_features | -       |                                                         | 在构建json字符串时使用的Json特性。格式为 `','` 分隔的字符串，例如: `"QuoteFieldNames,UseSingleQuotes"` |


## 相关文档

配置示例文档：[Elasticsearch 连接器示例](./elasticsearch-example.md)
